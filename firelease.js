'use strict';

if (typeof Promise === 'undefined') require('es6-promise').polyfill();
require('promise.prototype.finally');
var _ = require('underscore');
var NodeFire = require('nodefire');
var ms = require('ms');
var co = require('co');

/**
 * Return this from a worker to retry after the current lease expires, and to reset the lease
 * backoff to zero.
 */
exports.RETRY = {};

/**
 * Set this to the maximum number of concurrent tasks being executed at any moment across all
 * queues.
 * @type {number}
 */
exports.globalMaxConcurrent = Number.MAX_VALUE;

/**
 * Default option values for all subsequent attachWorker calls.  See that function for details.
 * @type {Object}
 */
exports.defaults = {
  maxConcurrent: Number.MAX_VALUE, bufferSize: 5, minLease: '30s', maxLease: '1h', leaseDelay: 0,
  healthyPingLatency: '1.5s'
};

/**
 * A function used to capture errors.  Defaults to logging the stack to the console, but you may
 * want to change it to something else in production.  The function should take a single exception
 * argument.
 */
exports.captureError = function(error) {console.log(error.stack);};

var PING_INTERVAL = ms('1m');
var PING_KEY = 'ping';

var queues = [];
var tasks = {};
var globalNumConcurrent = 0;
var shutdownCallbacks = [];

/**
 * Attaches a worker function to consume tasks from a queue.  You should normally attach no more
 * than one worker per path in any given process, but it's OK to run multiple processes on the same
 * paths concurrently.
 * @param {NodeFire} ref A NodeFire ref to the queue root in Firebase.  Individual tasks will be
 *        children of this root and must be objects.  The '_lease' key is reserved for use by
 *        Firelease in each task.
 * @param {Object} options Optional options, supporting the following values:
 *        maxConcurrent: {number} max number of tasks to handle concurrently for this worker.
 *        bufferSize: {number} upper bound on how many tasks to keep buffered and potentially go
 *          through leasing transactions in parallel; not worth setting higher than maxConcurrent,
 *          or higher than about 10.
 *        minLease: {number | string} minimum duration of each lease, which should equal the maximum
 *          expected time a worker will take to handle a task; specified as either a number of
 *          milliseconds, or a human-readable duration string.
 *        maxLease: {number | string} maximum duration of each lease, same format as minLease; the
 *          lease duration is doubled each time a task fails until it reaches maxLease.
 *        leaseDelay: {number | string} duration by which to delay leasing an item after it becomes
 *          available (same format as minLease); useful for setting up "backup" servers that only
 *          grab tasks that aren't taken up fast enough by the primary.
 *        preprocess: {function(Object):Object} a function to use to preprocess each item during the
 *          leasing transaction.  This function must be fast, synchronous, idempotent, and
 *          should return the modified item (passed as the sole argument, OK to mutate).  One use
 *          for preprocessing is to clean up items written to a queue by a process outside your
 *          control (e.g., webhooks).
 *        healthyPingLatency: {number | string} the maximum response latency to pings that is
 *          considered "healthy" for this queue; specified as either a number of milliseconds, or a
 *          human-readable duration string.
 * @param {function(Object):RETRY | number | string | undefined} worker The worker function that
 *        handles enqueued tasks.  It will be given a task object as argument, with a special $ref
 *        attribute set to the Nodefire ref of that task.  The worker can perform arbitrary
 *        computation whose duration should not exceed the queue's minLease value.  It can
 *        manipulate the task itself in Firebase as well, e.g. to delete it (to get at-most-once
 *        queue semantics) or otherwise modify it.  The worker can return any of the following:
 *        * undefined or null to cause the task to be retired from the queue.
 *        * firelease.RETRY to cause the task to be retried after the current lease expires (and
 *          reset the lease backoff counter).
 *        * A duration after which the task should be retried relative to when it was started (as
 *          either a number of milliseconds or a human-readable duration string).
 *        * An epoch in milliseconds greater than 1000000000000 at which the task should be tried.
 *        * A function that takes the task as argument and returns one of the values above.  This
 *          function will be executed in a transaction to ensure atomicity.
 *        All of these values can also be wrapped in a promise or a generator, which will be dealt
 *        with appropriately.
 */
exports.attachWorker = function(ref, options, worker) {
  queues.push(new Queue(ref, options, worker));
};

function duration(value) {
  if (_.isNumber(value)) return value;
  return ms(value);
}

var scanAll = _.debounce(function() {
  _.each(tasks, function(task) {
    task.queue.process(task);
  });
}, 100);


function Task(queue, snap) {
  this.queue = queue;
  this.ref = snap.ref();
  this.key = Task.makeKey(snap);
  this.updateFrom(snap);
}

Task.makeKey = function(snap) {
  return new NodeFire(snap.ref()).toString();
};

Task.prototype.updateFrom = function(snap) {
  this.expiry = snap.getPriority() || this.queue.now();
  // console.log('update', this.key, 'expiry', this.expiry);
  delete this.removed;
};

Task.prototype.prepare = function() {
  if (this.removed || this.working && !this.expiry) return false;
  var now = this.queue.now();
  var busy = this.expiry + this.queue.options.leaseDelay > now;
  // console.log('prepare', this.ref.key(), 'expiry', this.expiry, 'now', now);
  if (!busy) {
    // Locally reserve for min lease duration to prevent concurrent transaction attempts.  Expiry
    // will be overwritten when transaction completes or task gets removed.
    this.expiry = now + this.queue.constrainLeaseDuration(0);
  }
  if (this.timeout) clearTimeout(this.timeout);
  // Pad the timeout a bit, since it can fire early and we don't want to have to reschedule.
  this.timeout = setTimeout(
    this.queue.process.bind(this.queue, this),
    this.expiry + this.queue.options.leaseDelay - now + 100);
  return !busy;
};

Task.prototype.process = function() {
  var startTimestamp;
  this.working = true;
  return this.ref.transaction((function(item) {
    if (!item) return;
    if (this.ref.key() === PING_KEY) return null;
    item._lease = item._lease || {};
    startTimestamp = this.queue.now();
    // console.log('txn  ', this.ref.key(), 'lease', item._lease, 'now', startTimestamp);
    // Check if another process beat us to it.
    if (item._lease.expiry && item._lease.expiry + this.queue.options.leaseDelay > startTimestamp) {
      return;
    }
    item._lease.time = this.queue.constrainLeaseDuration(item._lease.time * 2 || 0);
    item._lease.expiry = startTimestamp + item._lease.time;
    item._lease.attempts = (item._lease.attempts || 0) + 1;
    item['.priority'] = item._lease.expiry;
    return this.queue.callPreprocess(item);
  }).bind(this)).then((function(item) {
    if (_.isUndefined(item) || item === null || this.ref.key() === PING_KEY) return;
    if (!_.isObject(item)) throw new Error('item not an object: ' + item);
    return this.run(item, startTimestamp);
  }).bind(this)).catch((function(error) {
    console.log('Queue item', this.key, 'lease transaction error:', error.message);
    error.firelease = {itemKey: this.key, phase: 'leasing'};
    exports.captureError(error);
    // Hardcoded retry in 1 second -- hard to do anything smarter, since we failed to update the
    // task in Firebase.
    setTimeout(this.queue.process.bind(this.queue, this), 1000);
  }).bind(this)).then((function() {
    this.working = false;
  }).bind(this));
};

Task.prototype.run = function(item, startTimestamp) {
  Object.defineProperty(item, '$ref', {value: this.ref});
  return this.queue.callWorker(item).finally((function() {
    var now = this.queue.now();
    if (now > item._lease.expiry) {
      // If it looks like we exceeded the lease time, double-check against the current item before
      // crying wolf, in case the worker extended the lease.
      return this.ref.get().then((function(item) {
        // If no item, we can't tell if it's because the worker chose to delete it early, or because
        // it overran its lease and another worker picked it up and completed it, so say nothing.
        if (!item) return;
        if (!item._lease) {
          console.log(
            'Queue item', this.key, 'likely exceeded its lease time by taking',
            ms(now - startTimestamp),
            'because the item has already been deleted and replaced with a new one.');
        } else if (now > item._lease.expiry) {
          console.log(
            'Queue item', this.key, 'exceeded lease time of',
            ms(item._lease.expiry - startTimestamp), 'by taking', ms(now - startTimestamp));
        }
      }).bind(this));
    }
  }).bind(this)).then((function(result) {
    if (_.isUndefined(result) || result === null) return this.ref.remove();  // common shortcut
    return this.ref.transaction(function(item2) {
      if (!item2) return;
      var value = _.isFunction(result) ? result(item2) : result;
      if (_.isUndefined(value) || value === null) return null;
      if (value === exports.RETRY) {
        if (item2._lease) delete item2._lease.time;
      } else if (_.isNumber(value) || _.isString(value)) {
        value = duration(value);
        item2._lease = item2._lease || {};
        item2._lease.expiry = value > 1000000000000 ? value : startTimestamp + value;
        item2._lease.time = null;
        item2['.priority'] = item2._lease.expiry;
      } else {
        throw new Error('Unexpected return value from worker: ' + value);
      }
      return item2;
    }).then(function(item2) {
      if (item2) item._lease = item2._lease;
    });
  }).bind(this), (function(error) {
    console.log('Queue item', this.key, 'processing error:', error.message);
    error.firelease = {itemKey: this.key, phase: 'processing'};
    exports.captureError(error);
  }).bind(this)).catch((function(error) {
    console.log('Queue item', this.key, 'post-processing error:', error.message);
    error.firelease = {itemKey: this.key, phase: 'post-processing'};
    exports.captureError(error);
  }).bind(this));
};


function Queue(ref, options, worker) {
  if (_.isFunction(options)) {
    worker = options;
    options = {};
  }
  this.options = _.defaults({}, options, exports.defaults);
  this.options.minLease = duration(this.options.minLease);
  this.options.maxLease = duration(this.options.maxLease);
  this.options.leaseDelay = duration(this.options.leaseDelay);
  this.options.healthyPingLatency = duration(this.options.healthyPingLatency);
  this.numConcurrent = 0;
  this.worker = worker;
  this.ref = ref;

  // Need each queue's scan function to be debounced separately.
  this.scan = _.debounce(function() {
    _.each(tasks, function(task) {
      if (task.queue === this) task.queue.process(task);
    }, this);
  }, 100);

  var top = ref.orderByPriority().limitToFirst(this.options.bufferSize);
  top.on('child_added', this.addTask.bind(this), this.crash.bind(this));
  top.on('child_removed', this.removeTask.bind(this), this.crash.bind(this));
  top.on('child_moved', this.addTask.bind(this), this.crash.bind(this));
}

Queue.prototype.crash = function(error) {
  console.log('Queue worker', this.ref.toString(), 'interrupted:', error);
  error.firelease = {queue: this.ref.toString(), phase: 'crashing'};
  exports.captureError(error);
  process.exit(1);
};

Queue.prototype.now = function() {
  return this.ref.now();
};

Queue.prototype.addTask = function(snap) {
  var taskKey = Task.makeKey(snap);
  // Ignore content updates to existing tasks that show up as priority-less adds.
  if (!snap.getPriority() && taskKey in tasks) return;
  var task = tasks[taskKey];
  if (task) {
    task.updateFrom(snap);
  } else {
    task = tasks[taskKey] = new Task(this, snap);
  }
  this.process(task);
};

Queue.prototype.removeTask = function(snap) {
  var taskKey = Task.makeKey(snap);
  var task = tasks[taskKey];
  if (!task) return;
  task.removed = true;
  if (task.timeout) {
    clearTimeout(task.timeout);
    delete task.timeout;
  }
  if (!task.working) delete tasks[taskKey];
};

Queue.prototype.hasQuota = function() {
  return this.numConcurrent < this.options.maxConcurrent &&
    globalNumConcurrent < exports.globalMaxConcurrent;
};

Queue.prototype.constrainLeaseDuration = function(time) {
  return Math.min(this.options.maxLease, Math.max(time, this.options.minLease));
};

Queue.prototype.process = function(task) {
  if (this.hasQuota() && task.prepare()) {
    globalNumConcurrent++;
    this.numConcurrent++;
    task.process().then((function() {
      if (task.removed) delete tasks[task.key];
      if (globalNumConcurrent === exports.globalMaxConcurrent) {
        scanAll();
      } else if (this.numConcurrent === this.options.maxConcurrent) {
        this.scan();
      }
      globalNumConcurrent--;
      this.numConcurrent--;
      invokeShutdownCallbacks();
    }).bind(this));
  }
};

Queue.prototype.callPreprocess = function(item) {
  if (this.options.preprocess) item = this.options.preprocess(item);
  return item;
};

Queue.prototype.callWorker = function(item) {
  try {
    var result = this.worker(item);
    if (result && typeof result.next === 'function' && typeof result.throw === 'function') {
      // Got a generator, let's co-ify it nicely to capture errors.
      return co(result);
    } else {
      return Promise.resolve(result);
    }
  } catch(e) {
    return Promise.reject(e);
  }
};


var pinging = false;
var pingIntervalHandle, pingCallback;

/**
 * Sets up regular pinging of all queues.  Can be called either before or after workers are
 * attached, and will always ping all queues.  Can be called more than once to change the
 * parameters.
 * @param {Function(Object) | null} callback The callback to invoke with a report each time we ping
 *        all the queues.  The report looks like: {healthy: true, maxLatency: 1234}.  If not
 *        specified, reports are silently dropped.
 * @param {number | string} interval The interval at which to ping queues, to both check the
 *        current response latency and make sure no tasks are stuck; specified as either a number
 *        of milliseconds, or a human-readable duration string.  Defaults to 1 minute.
 */
exports.pingQueues = function(callback, interval) {
  interval = interval && duration(interval) || PING_INTERVAL;
  if (pingIntervalHandle) clearInterval(pingIntervalHandle);
  pingCallback = callback;
  pingIntervalHandle = setInterval(function() {
    checkPings().catch(function(error) {
      console.log('Error while pinging:', error);
      error.firelease = {phase: 'pinging'};
      exports.captureError(error);
      pinging = false;
    });
  }, interval);
};

function checkPings() {
  if (pinging) return Promise.resolve();
  pinging = true;
  return Promise.all(_.map(queues, function(queue) {
    var start = Date.now();
    var pingRef = queue.ref.child(PING_KEY);
    return pingRef.transaction(function(item) {
      if (item) return;
      return {timestamp: start, '.priority': 1};
    }).then(function(item) {
      if (_.isUndefined(item)) return null;  // another process is currently pinging
      return waitUntilDeleted(pingRef).then(function() {
        var latency = Date.now() - start;
        return {latency: latency, healthy: latency < queue.options.healthyPingLatency};
      }, function() {
        return null;
      });
    });
  })).then(function(results) {
    results = _.compact(results);
    if (results.length) {
      // Backup scan in case tasks are stuck on a queue due to bugs.
      scanAll();
      if (pingCallback) {
        pingCallback({
          healthy: _.every(results, 'healthy'),
          maxLatency: Math.max.apply(null, _.pluck(results, 'latency'))});
      }
    }
    pinging = false;
  });
}

function waitUntilDeleted(ref) {
  return new Promise(function(resolve, reject) {
    function onValue(snap) {
      if (snap.val()) return;
      ref.off('value', onValue);
      resolve();
    }
    ref.on('value', onValue, reject);
  });
}


/**
 * Extends the lease on a task to give the worker more time to finish.  Checks a bunch of validity
 * constraints along the way and throws an error if the worker needs to abort.
 * @param {Object} item The original task object provided to a worker function.
 * @param {number | string} timeNeeded The minimum time needed counting from the current time,
 *        specified as either a number of milliseconds or a human-readable duration.  The actual
 *        lease may be extended by up to twice this amount, to prevent excessive churn.
 * @return {Promise} A promise that will be resolved when the lease has been extended, and rejected
 *         if something went wrong and the worker should abort.
 */
exports.extendLease = function(item, timeNeeded) {
  if (!(item && item._lease && item._lease.expiry)) throw new Error('Invalid task');
  timeNeeded = duration(timeNeeded);
  return item.$ref.transaction(function(item2) {
    try {
      if (!item2 || !item2._lease) throw new Error('Task disappeared, unable to extend lease.');
      if (item._lease.expiry !== item2._lease.expiry) {
        throw new Error('Task leased by another worker, unable to extend lease.');
      }
      var now = item.$ref.now();
      if (item2._lease.expiry <= now) throw new Error('Lease expired, unable to extend.');
      if (item2._lease.expiry >= now + timeNeeded) return;
      item2._lease.expiry += timeNeeded;
      item2['.priority'] = item2._lease.expiry;
      return item2;
    } catch (e) {
      e.firelease = {itemKey: item.$ref.toString(), timeNeeded: timeNeeded};
      throw e;
    }
  }).then(function(item2) {
    if (item2) item._lease.expiry = item2._lease.expiry;
  });
};


/**
 * Shuts down firelease by refusing to take new tasks, and invokes the callback once all currently
 * running tasks have completed.
 * @param {function} callback The callback to invoke when all tasks have completed.
 */
exports.shutdown = function(callback) {
  exports.globalMaxConcurrent = 0;
  shutdownCallbacks.push(callback);
  invokeShutdownCallbacks();
};

function invokeShutdownCallbacks() {
  if (!globalNumConcurrent && shutdownCallbacks.length) {
    for (var i = 0; i < shutdownCallbacks.length; i++) {
      try {
        shutdownCallbacks[i]();
      } catch(e) {
        e.message = 'Firelease shutdown callback failed: ' + e.message;
        exports.captureError(e);
      }
    }
    shutdownCallbacks = [];
  }
}
