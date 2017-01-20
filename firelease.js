'use strict';

require('promise.prototype.finally').shim();
const _ = require('lodash');
const NodeFire = require('nodefire');
const ms = require('ms');

const PING_INTERVAL = ms('1m');
const PING_KEY = 'ping';

const queues = [];
const tasks = {};
let globalMaxConcurrent = Number.MAX_VALUE;
let globalNumConcurrent = 0;
let shutdownResolve, shutdownReject, shutdownPromise;

const scanAll = _.debounce(function() {
  _.each(tasks, task => {
    task.queue.process(task);
  });
}, 100);


module.exports = {};

/**
 * Return this from a worker to retry after the current lease expires, and to reset the lease
 * backoff to zero.
 */
module.exports.RETRY = {};

/**
 * Set this to the maximum number of concurrent tasks being executed at any moment across all
 * queues.
 * @type {number}
 */
Object.defineProperty(module.exports, 'globalMaxConcurrent', {
  get: function() {return globalMaxConcurrent;},
  set: function(value) {
    globalMaxConcurrent = value;
    if (value) {
      if (shutdownReject) shutdownReject(new Error('Queues restarted'));
      shutdownPromise = shutdownResolve = shutdownReject = null;
      scanAll();
    }
  }
});

/**
 * Default option values for all subsequent attachWorker calls.  See that function for details.
 * @type {Object}
 */
module.exports.defaults = {
  maxConcurrent: Number.MAX_VALUE, minLease: '30s', maxLease: '1h', leaseDelay: 0, maxLeaseDelay: 0,
  healthyPingLatency: '1.5s'
};

/**
 * A function used to capture errors.  Defaults to logging the stack to the console, but you may
 * want to change it to something else in production.  The function should take a single exception
 * argument.
 */
module.exports.captureError = function(error) {console.log(error.stack);};


class Task {
  constructor(queue, snap) {
    this.queue = queue;
    this.ref = snap.ref();
    this.key = Task.makeKey(snap);
    this.updateFrom(snap);
  }

  static makeKey(snap) {
    return new NodeFire(snap.ref()).toString();
  }

  updateFrom(snap) {
    const value = snap.val();
    this.expiry = value && value._lease && value._lease.expiry || this.queue.now();
    // console.log('update', this.key, 'expiry', this.expiry);
    delete this.removed;
  }

  prepare() {
    if (tasks[this.key] !== this || this.removed || this.working && !this.expiry) return false;
    const now = this.queue.now();
    const busy = this.expiry + this.queue.leaseDelay > now;
    // console.log('prepare', this.ref.key(), 'expiry', this.expiry, 'now', now);
    if (!busy) {
      // Locally reserve for min lease duration to prevent concurrent transaction attempts.  Expiry
      // will be overwritten when transaction completes or task gets removed.
      this.expiry = now + this.queue.constrainLeaseDuration(0);
    }
    if (this.timeout) clearTimeout(this.timeout);
    this.timeout = setTimeout(
      this.queue.process.bind(this.queue, this), this.expiry + this.queue.leaseDelay - now);
    return !busy;
  }

  process() {
    let startTimestamp;
    let acquired;
    this.working = true;
    const transactionPromise = this.ref.transaction(item => {
      acquired = false;
      if (tasks[this.key] !== this) {
        console.log(`Queue item ${this.key} task not current, bailing from transaction`);
        return;
      }
      if (!item || this.ref.key() === PING_KEY) {
        acquired = true;
        return null;
      }
      startTimestamp = this.queue.now();
      // console.log('txn  ', this.ref.key(), 'lease', item._lease, 'now', startTimestamp);
      // Check if another process beat us to it.
      if (item._lease && item._lease.expiry &&
          item._lease.expiry + this.queue.leaseDelay > startTimestamp) {
        return item;
      }
      acquired = true;
      item._lease = item._lease || {};
      item._lease.time = this.queue.constrainLeaseDuration(item._lease.time * 2 || 0);
      item._lease.expiry = startTimestamp + item._lease.time;
      item._lease.attempts = (item._lease.attempts || 0) + 1;
      if (!item._lease.initial) item._lease.initial = startTimestamp;
      return this.queue.callPreprocess(item);
    }, {detectStuck: 8, prefetchValue: false, timeout: ms('15s')});
    return transactionPromise.then(item => {
      if (!acquired) this.queue.countTaskAcquired(false);
      if (!acquired || item === null || this.ref.key() === PING_KEY) return;
      if (!_.isObject(item)) throw new Error(`item not an object: ${item}`);
      Object.defineProperty(item, '$leaseTransaction', {value: transactionPromise.transaction});
      this.queue.countTaskAcquired(true);
      return this.run(item, startTimestamp);
    }).catch(error => {
      // Sometimes a transaction appears to get stuck on an item that has already been deleted.
      // Try to probe for the item explicitly and just give up if it's already gone.
      return this.ref.get().catch(e => 'placeholder').then(item => {
        if (!item) {
          console.log(`Queue item ${this.key} gone, discarding lease transaction error`);
          return;
        }
        console.log(`Queue item ${this.key} lease transaction error: ${error.message}`);
        error.firelease = _.extend(
          error.firelease || {}, {itemKey: this.key, phase: 'leasing', lease: item._lease});
        module.exports.captureError(error);
        // Hardcoded retry -- hard to do anything smarter, since we failed to update the task in
        // Firebase.
        this.expiry = 0;
        setTimeout(this.queue.scan, ms('3s'));
      });
    }).then(() => {
      this.working = false;
    });
  }

  run(item, startTimestamp) {
    Object.defineProperty(item, '$ref', {value: this.ref});
    Object.defineProperty(item, '$leaseTimeRemaining', {get: () => {
      if (!(item._lease && item._lease.expiry)) return 0;
      return Math.max(0, item._lease.expiry - this.queue.now());
    }});
    return this.queue.callWorker(item).finally(() => {
      const now = this.queue.now();
      if (now > item._lease.expiry) {
        // If it looks like we exceeded the lease time, double-check against the current item before
        // crying wolf, in case the worker extended the lease.
        return this.ref.get().then(item => {
          // If no item, we can't tell if it's because the worker chose to delete it early, or
          // because it overran its lease and another worker picked it up and completed it, so say
          // nothing.
          if (!item) return;
          if (!item._lease) {
            console.log(
              `Queue item ${this.key} likely exceeded its lease time by taking`,
              ms(now - startTimestamp),
              'because the item has already been deleted and replaced with a new one.');
          } else if (now > item._lease.expiry) {
            console.log(
              `Queue item ${this.key} exceeded lease time of`,
              ms(item._lease.expiry - startTimestamp), 'by taking', ms(now - startTimestamp));
          }
        });
      }
    }).then(result => {
      if (_.isUndefined(result) || result === null) return this.ref.remove();  // common shortcut
      return this.ref.transaction(item2 => {
        if (!item2) return null;
        let value = _.isFunction(result) ? result(item2) : result;
        if (_.isUndefined(value) || value === null) return null;
        if (value === module.exports.RETRY) {
          if (item2._lease) delete item2._lease.time;
        } else if (_.isNumber(value) || _.isString(value)) {
          value = duration(value);
          item2._lease = item2._lease || {};
          item2._lease.expiry = value > 1000000000000 ? value : startTimestamp + value;
          item2._lease.time = null;
        } else {
          throw new Error(`Unexpected return value from worker: ${value}`);
        }
        return item2;
      }, {prefetchValue: false}).then(item2 => {
        if (item2) item._lease = item2._lease;
      });
    }, error => {
      console.log(`Queue item ${this.key} processing error: ${error.message}`);
      error.firelease = _.extend(error.firelease || {} , {itemKey: this.key, phase: 'processing'});
      if (!error.level) error.level = 'warning';
      module.exports.captureError(error);
    }).catch(error => {
      console.log(`Queue item ${this.key} post-processing error: ${error.message}`);
      error.firelease =
        _.extend(error.firelease || {} , {itemKey: this.key, phase: 'post-processing'});
      module.exports.captureError(error);
    });
  }
}


class Queue {
  constructor(ref, options, worker) {
  if (_.isFunction(options)) {
      worker = options;
      options = {};
    }
    this.options = _.defaults({}, options, module.exports.defaults);
    this.options.minLease = duration(this.options.minLease);
    this.options.maxLease = duration(this.options.maxLease);
    this.options.minLeaseDelay = this.leaseDelay = duration(this.options.leaseDelay);
    delete this.options.leaseDelay;
    this.options.healthyPingLatency = duration(this.options.healthyPingLatency);
    this.options.maxLeaseDelay = duration(this.options.maxLeaseDelay);
    this.numConcurrent = 0;
    this.tasksAcquired = 0;
    this.worker = worker;
    this.ref = ref;

    // Need each queue's scan function to be debounced separately.
    this.scan = _.debounce(this.scan.bind(this), 100);

    ref.on('child_added', this.addTask, this.crash, this);
    ref.on('child_removed', this.removeTask, this.crash, this);
    ref.on('child_moved', this.addTask, this.crash, this);
  }

  scan() {
    _.each(tasks, task => {
      if (task.queue === this) task.queue.process(task);
    });
  }

  crash(error) {
    console.log(`Queue worker ${this.ref.toString()} interrupted:`, error);
    error.firelease =
      _.extend(error.firelease || {}, {queue: this.ref.toString(), phase: 'crashing'});
    module.exports.captureError(error);
    process.exit(1);
  }

  now() {
    return this.ref.now();
  }

  addTask(snap) {
    const taskKey = Task.makeKey(snap);
    let task = tasks[taskKey];
    if (task) {
      task.updateFrom(snap);
    } else {
      task = tasks[taskKey] = new Task(this, snap);
    }
    this.process(task);
  }

  removeTask(snap) {
    const taskKey = Task.makeKey(snap);
    const task = tasks[taskKey];
    if (!task) return;
    task.removed = true;
    if (task.timeout) {
      clearTimeout(task.timeout);
      delete task.timeout;
    }
    if (!task.working) delete tasks[taskKey];
  }

  hasQuota() {
    return this.numConcurrent < this.options.maxConcurrent &&
      globalNumConcurrent < globalMaxConcurrent;
  }

  constrainLeaseDuration(time) {
    return Math.min(this.options.maxLease, Math.max(time, this.options.minLease));
  }

  countTaskAcquired(acquired) {
    if (this.options.maxLeaseDelay) {
      this.leaseDelay = Math.max(this.options.minLeaseDelay, Math.min(
        this.options.maxLeaseDelay, this.leaseDelay + (acquired ? 1 : -2)));
    }
    if (acquired) this.tasksAcquired++;
  }

  process(task) {
    if (this.hasQuota() && task.prepare()) {
      globalNumConcurrent++;
      this.numConcurrent++;
      task.process().then(() => {
        globalNumConcurrent--;
        this.numConcurrent--;
        if (task.removed) delete tasks[task.key];
        if (globalNumConcurrent === globalMaxConcurrent - 1) {
          scanAll();
        } else if (this.numConcurrent === this.options.maxConcurrent - 1) {
          this.scan();
        }
        if (shutdownResolve && !globalMaxConcurrent && !globalNumConcurrent) shutdownResolve();
        if (!globalMaxConcurrent) {
          console.log(`Queues draining, tasks in progress: ${globalNumConcurrent}`);
        }
      }).catch(error => {
        error.message = `Unexpected error in Queue.process: ${error.message}`;
        module.exports.captureError(error);
      });
    }
  }

  callPreprocess(item) {
    if (this.options.preprocess) item = this.options.preprocess(item);
    return item;
  }

  callWorker(item) {
    try {
      const result = this.worker(item);
      if (result && typeof result.next === 'function' && typeof result.throw === 'function' &&
          Promise.co) {
        // Got a generator, let's co-ify it nicely to capture errors.
        return Promise.co(result);
      } else {
        return Promise.resolve(result);
      }
    } catch(e) {
      return Promise.reject(e);
    }
  }
}


/**
 * Attaches a worker function to consume tasks from a queue.  You should normally attach no more
 * than one worker per path in any given process, but it's OK to run multiple processes on the same
 * paths concurrently.  If you do, you probably want to set `maxLeaseDelay` to something greater
 * than zero, to properly balance task distribution between the processes.
 *
 * All durations can be specified as either a human-readable string, or a number of milliseconds.
 *
 * @param {NodeFire} ref A NodeFire ref to the queue root in Firebase.  Individual tasks will be
 *        children of this root and must be objects.  The '_lease' key is reserved for use by
 *        Firelease in each task.
 * @param {Object} options Optional options, supporting the following values:
 *        maxConcurrent: {number} max number of tasks to handle concurrently for this worker.
 *        minLease: {number | string} minimum duration of each lease, which should equal the maximum
 *          expected time a worker will take to handle a task.
 *        maxLease: {number | string} maximum duration of each lease; the lease duration is doubled
 *          each time a task fails until it reaches maxLease.
 *        leaseDelay: {number | string} duration by which to delay leasing an item after it becomes
 *          available; useful for setting up "backup" servers that only grab tasks that aren't taken
 *          up fast enough by the primary.
 *        maxLeaseDelay: {number | string} if non-zero, enables automatic leaseDelay adjustment and
 *          sets the maximum duration to wait before attempting to acquire a ready task.  This is
 *          often necessary to compensate for differences in machine or network speed, or for
 *          Firebase's consistent order for sending event notifications to multiple clients.
 *        preprocess: {function(Object):Object} a function to use to preprocess each item during the
 *          leasing transaction.  This function must be fast, synchronous, idempotent, and
 *          should return the modified item (passed as the sole argument, OK to mutate).  One use
 *          for preprocessing is to clean up items written to a queue by a process outside your
 *          control (e.g., webhooks).
 *        healthyPingLatency: {number | string} the maximum response latency to pings that is
 *          considered "healthy" for this queue.
 * @param {function(Object):RETRY | number | string | undefined} worker The worker function that
 *        handles enqueued tasks.  It will be given a task object as argument, with a special $ref
 *        attribute set to the Nodefire ref of that task.  The worker can perform arbitrary
 *        computation whose duration should not exceed the queue's minLease value.  It can
 *        manipulate the task itself in Firebase as well, e.g. to delete it (to get at-most-once
 *        queue semantics) or otherwise modify it.  The worker can return any of the following:
 *        * undefined or null to cause the task to be retired from the queue.
 *        * firelease.RETRY to cause the task to be retried after the current lease expires (and
 *          reset the lease backoff counter).
 *        * A duration after which the task should be retried relative to when it was started.
 *        * An epoch in milliseconds greater than 1000000000000 at which the task should be tried.
 *        * A function that takes the task as argument and returns one of the values above.  This
 *          function will be executed in a transaction to ensure atomicity.
 *        All of these values can also be wrapped in a promise or a generator, which will be dealt
 *        with appropriately.
 */
module.exports.attachWorker = function(ref, options, worker) {
  queues.push(new Queue(ref, options, worker));
};

function duration(value) {
  if (_.isNumber(value)) return value;
  return ms(value);
}


let pinging = false;
let pingIntervalHandle, pingCallback;

/**
 * Sets up regular pinging of all queues.  Can be called either before or after workers are
 * attached, and will always ping all queues.  Can be called more than once to change the
 * parameters.
 *
 * All durations can be specified as either a human-readable string, or a number of milliseconds.
 *
 * @param {Function(Object) | null} callback The callback to invoke with a report each time we ping
 *        all the queues.  The report looks like: {healthy: true, maxLatency: 1234}.  If not
 *        specified, reports are silently dropped.
 * @param {number | string} interval The interval at which to ping queues, to both check the
 *        current response latency and make sure no tasks are stuck.  Defaults to 1 minute.
 */
module.exports.pingQueues = function(callback, interval) {
  interval = interval && duration(interval) || PING_INTERVAL;
  if (pingIntervalHandle) clearInterval(pingIntervalHandle);
  pingCallback = callback;
  pingIntervalHandle = setInterval(() => {
    checkPings().catch(function(error) {
      error.firelease = _.extend(error.firelease || {}, {phase: 'pinging'});
      error.level = 'warning';
      module.exports.captureError(error);
      pinging = false;
    });
  }, interval);
};

function checkPings() {
  if (pinging) return Promise.resolve();
  pinging = true;
  return Promise.all(_.map(queues, queue => {
    const start = Date.now();
    const pingRef = queue.ref.child(PING_KEY);
    let pingFree;
    return pingRef.transaction(function(item) {
      pingFree = !item;
      return item || {timestamp: start, _lease: {expiry: 1}};
    }, {prefetchValue: false}).then(item => {
      if (!pingFree) return null;  // another process is currently pinging
      return waitUntilDeleted(pingRef).then(() => {
        const latency = Date.now() - start;
        return {
          queue: queue, latency: latency, healthy: latency < queue.options.healthyPingLatency,
          leaseDelay: queue.leaseDelay, tasksAcquired: queue.tasksAcquired
        };
      }, () => null);
    });
  })).then(results => {
    results = _.compact(results);
    if (results.length) {
      // Backup scan in case tasks are stuck on a queue due to bugs.
      scanAll();
      if (pingCallback) {
        const sickQueueKeys =
          _(results).reject('healthy').map(item => item.queue.ref.key()).value();
        const delays = _(results).pluck('leaseDelay').sortBy().value();
        const delaysMedian = delays.length % 2 ?
          delays[Math.floor(delays.length / 2)] :
          ((delays[Math.floor(delays.length / 2)] +
            delays[Math.ceil(delays.length / 2)]) / 2);
        pingCallback({
          healthy: _.every(results, 'healthy'),
          sickQueues: sickQueueKeys,
          maxLatency: _.max(_.pluck(results, 'latency')),
          tasksAcquired:
            _.reduce(results, function(sum, result) {return sum + result.tasksAcquired;}, 0),
          leaseDelays: {min: _.min(delays), max: _.max(delays), median: delaysMedian}
        });
      }
    }
    pinging = false;
  });
}

function waitUntilDeleted(ref) {
  return new Promise((resolve, reject) => {
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
 *
 * All durations can be specified as either a human-readable string, or a number of milliseconds.
 *
 * @param {Object} item The original task object provided to a worker function.
 * @param {number | string} timeNeeded The minimum time needed counting from the current time.  The
          actual lease may be extended by up to twice this amount, to prevent excessive churn.
 * @return {Promise} A promise that will be resolved when the lease has been extended, and rejected
 *         if something went wrong and the worker should abort.
 */
module.exports.extendLease = function(item, timeNeeded) {
  if (!(item && item._lease && item._lease.expiry)) throw new Error('Invalid task');
  item._lease.timeNeeded = Math.max(item._lease.timeNeeded || 0, duration(timeNeeded));
  if (!item._lease.extendLeasePromise) {
    let error, timeNeededUsed;
    item._lease.extendLeasePromise = item.$ref.transaction(item2 => {
      error = null;
      timeNeededUsed = null;
      const now = item.$ref.now();
      if (!item2) {
        error = new Error('Task disappeared, unable to extend lease.');
        error.firelease = {code: 'gone'};
        item2 = null;  // make sure we attempt a write to force sha check
      } else if (!item2._lease) {
        error = new Error('Task recreated, unable to extend lease.');
        error.firelease = {code: 'recreated'};
      } else if (item._lease.expiry !== item2._lease.expiry) {
        error = new Error('Task leased by another worker, unable to extend lease.');
        error.firelease = {code: 'stolen'};
      } else if (item2._lease.expiry <= now) {
        error = new Error('Lease expired, unable to extend.');
        error.firelease = {code: 'lost'};
      } else {
        timeNeededUsed = item._lease.timeNeeded;
        // Expiry is monotonically increasing, so safe to do early abort if it's high enough.
        if (item2._lease.expiry >= now + timeNeededUsed) return;
        item2._lease.expiry += timeNeededUsed;
      }
      return item2;
    }, {prefetchValue: false}).then(item2 => {
      let moreTimeNeeded;
      if (item._lease) {
        if (item._lease.timeNeeded > timeNeededUsed) moreTimeNeeded = item._lease.timeNeeded;
        delete item._lease.extendLeasePromise;
        delete item._lease.timeNeeded;
      }
      if (error) {
        error.firelease = _.extend(
          error.firelease || {}, {itemKey: item.$ref.toString(), timeNeeded: timeNeeded});
        return Promise.reject(error);
      }
      if (item2 && item._lease) item._lease.expiry = item2._lease.expiry;
      if (moreTimeNeeded) {
        // If an extendLease raced with the transaction then retry it.
        return module.exports.extendLease(item, moreTimeNeeded);
      }
    });
  }
  return item._lease.extendLeasePromise;
};


/**
 * Shuts down firelease by refusing to take new tasks, and invokes the callback once all currently
 * running tasks have completed.
 * @param {function} callback The callback to invoke when all tasks have completed.
 */
module.exports.shutdown = function(callback) {
  globalMaxConcurrent = 0;
  if (!shutdownPromise) {
    shutdownPromise = new Promise((resolve, reject) => {
      shutdownResolve = resolve;
      shutdownReject = reject;
    });
  }
  if (!globalNumConcurrent) shutdownResolve();
  return shutdownPromise;
};


/**
 * Lists the URLs of all tasks that are currently being worked on.
 */
module.exports.listTasksInProgress = function() {
  return _(tasks).map(function(task, key) {return task.working ? key : null;}).compact().value();
};
