import _ from 'lodash';
import ms from 'ms';
import NodeFire from 'nodefire';
import * as timers from 'safe-timers';

const PING_INTERVAL = ms('1m');
const PING_KEY = 'ping';

declare const RETRY_DIRECTIVE: unique symbol;

// TypeScript requires declaration merging to expose named types with a CommonJS `export =` value.
/* eslint-disable @typescript-eslint/no-namespace */
/* eslint-disable @typescript-eslint/no-shadow */
declare namespace firelease {
  type Duration = number | string;

  interface Lease {
    expiry?: number;
    time?: number;
    attempts?: number;
    initial?: number;
    busy?: boolean;
    timeNeeded?: number;
    extendLeasePromise?: Promise<void>;
  }

  interface LeaseItem {
    _lease?: Lease;
    [key: string]: any;
  }

  interface RetryDirective {
    readonly [RETRY_DIRECTIVE]: true;
  }

  interface WorkerItem extends LeaseItem {
    _lease: Lease & {expiry: number};
    readonly $ref: NodeFire;
    readonly $leaseTimeRemaining: number;
  }

  type FireleaseErrorLevel = 'fatal' | 'error' | 'warning' | 'log' | 'info' | 'debug';

  interface FireleaseErrorDetails {
    code?: string;
    itemKey?: string;
    phase?: string;
    queue?: string;
    timeNeeded?: Duration;
  }

  interface FireleaseError extends Error {
    firelease?: FireleaseErrorDetails;
    level?: FireleaseErrorLevel;
  }

  interface QueueOptions {
    maxConcurrent?: number;
    bufferSize?: number;
    minLease?: Duration;
    maxLease?: Duration;
    healthyPingLatency?: Duration;
    preprocess?: (item: LeaseItem) => LeaseItem;
  }

  interface PingReport {
    healthy: boolean;
    sickQueues: (string | null)[];
    sickSources: string[];
    stuckTasks: number;
    maxLatency: number;
    tasksAcquired: number;
  }

  type QueueRef = NodeFire | NodeFire[];
  type WorkerResult = RetryDirective | Duration | Lease | null | void |
    ((item: LeaseItem) => RetryDirective | Duration | Lease | null | void);
  type Worker = (item: WorkerItem) => WorkerResult | PromiseLike<WorkerResult>;

  interface FireleaseApi {
    readonly RETRY: RetryDirective;
    globalMaxConcurrent: number;
    defaults: QueueOptions;
    captureError: (error: FireleaseError) => void;
    attachWorker: {
      (refOrRefs: QueueRef, worker: Worker): void;
      (refOrRefs: QueueRef, options: QueueOptions, worker: Worker): void;
    };
    pingQueues(callback?: ((report: PingReport) => void) | null, interval?: Duration): void;
    extendLease(item: WorkerItem, timeNeeded: Duration): Promise<void>;
    blacklist(taskKey: string): boolean;
    shutdown(): Promise<void>;
    listTasksInProgress(): string[];
  }
}
/* eslint-enable @typescript-eslint/no-namespace */
/* eslint-enable @typescript-eslint/no-shadow */

type Duration = firelease.Duration;
type Lease = firelease.Lease;
type LeaseItem = firelease.LeaseItem;
type RetryDirective = firelease.RetryDirective;
type WorkerItem = firelease.WorkerItem;
type FireleaseError = firelease.FireleaseError;
type QueueOptions = firelease.QueueOptions;
type PingReport = firelease.PingReport;
type QueueRef = firelease.QueueRef;
type WorkerResult = firelease.WorkerResult;
type Worker = firelease.Worker;
type FireleaseApi = firelease.FireleaseApi;

interface LeaseSnapshot {
  ref: NodeFire;
  val(): unknown;
}

interface NormalizedQueueOptions {
  maxConcurrent: number;
  bufferSize: number;
  minLease: number;
  maxLease: number;
  healthyPingLatency: number;
  preprocess?: (item: LeaseItem) => LeaseItem;
}

interface SourcePingResult {
  source: QueueSource;
  latency: number;
  healthy: boolean;
}

interface QueuePingResult {
  queue: Queue;
  latency: number;
  healthy: boolean;
  sickSources: QueueSource[];
  tasksAcquired: number;
}

const queues: Queue[] = [];
const tasks: Record<string, Task> = {};
const blacklistedTaskKeys = new Set<string>();
let globalMaxConcurrent = Number.MAX_VALUE;
let globalNumConcurrent = 0;
let shutdownResolve: (() => void) | undefined;
let shutdownReject: ((error: Error) => void) | undefined;
let shutdownPromise: Promise<void> | undefined;

/**
 * Return this from a worker to retry after the current lease expires, and to reset the lease
 * backoff to zero.
 */
const RETRY = {} as RetryDirective;

/** Default option values for all subsequent attachWorker calls. */
const defaults: QueueOptions = {
  maxConcurrent: Number.MAX_VALUE, bufferSize: 5, minLease: '30s', maxLease: '1h',
  healthyPingLatency: '1.5s'
};

const scanAll = _.debounce(() => {
  _.forEach(tasks, task => {
    void task.queue.process(task);
  });
}, 100);

const firelease = {
  RETRY,
  defaults,
  captureError: (error: FireleaseError) => {console.error(error.stack);},
  attachWorker,
  pingQueues,
  extendLease,
  blacklist,
  shutdown,
  listTasksInProgress
} as FireleaseApi;

Object.defineProperty(firelease, 'globalMaxConcurrent', {
  get: () => globalMaxConcurrent,
  set: (value: number) => {
    globalMaxConcurrent = value;
    if (value) {
      shutdownReject?.(new Error('Queues restarted'));
      shutdownPromise = shutdownResolve = shutdownReject = undefined;
      scanAll();
    }
  }
});


class Task {
  readonly queue: Queue;
  readonly ref: NodeFire;
  readonly key: string;
  phase: 'wait' | 'lease' | 'work' | 'exceed' | 'post' | 'done' | 'retry' = 'wait';
  expiry = 0;
  removed?: boolean;
  working = false;
  timeout?: timers.Timeout;

  constructor(readonly source: QueueSource, snap: LeaseSnapshot) {
    this.queue = source.queue;
    this.ref = snap.ref;
    this.key = Task.makeKey(snap);
    this.updateFrom(snap);
  }

  static makeKey(snap: LeaseSnapshot) {
    return snap.ref.toString();
  }

  updateFrom(snap: LeaseSnapshot) {
    const value = snap.val() as LeaseItem | null;
    this.expiry = value?._lease?.expiry ?? this.ref.now;
    // console.log('update', this.key, 'expiry', this.expiry);
    delete this.removed;
  }

  prepare() {
    if (tasks[this.key] !== this || this.removed || this.working) return false;
    const now = this.ref.now;
    const busy = this.expiry > now;
    // console.log('prepare', this.ref.key, 'expiry', this.expiry, 'now', now);
    if (!busy) {
      // Locally reserve for min lease duration to prevent concurrent transaction attempts.  Expiry
      // will be overwritten when transaction completes or task gets removed.
      this.expiry = now + this.queue.constrainLeaseDuration(0);
    }
    this.timeout?.clear();
    this.timeout = timers.setTimeout(this.queue.process.bind(this.queue, this), this.expiry - now);
    return !busy;
  }

  async process() {
    let startTimestamp = 0;
    let acquired = false;
    this.working = true;
    this.phase = 'lease';
    const transactionPromise = this.ref.transaction(itemValue => {
      const item = itemValue as LeaseItem | null;
      acquired = false;
      if (tasks[this.key] !== this || this.removed) return;
      if (!item || this.ref.key === PING_KEY) {
        acquired = true;
        return null;
      }
      startTimestamp = this.ref.now;
      // console.log('txn  ', this.ref.key, 'lease', item._lease, 'now', startTimestamp);
      // Check if another process beat us to it.
      if (item._lease?.expiry && item._lease.expiry > startTimestamp) {
        return item;
      }
      acquired = true;
      item._lease ??= {};
      item._lease.time = this.queue.constrainLeaseDuration((item._lease.time ?? 0) * 2);
      item._lease.expiry = startTimestamp + item._lease.time;
      item._lease.attempts = (item._lease.attempts ?? 0) + 1;
      item._lease.initial ??= startTimestamp;
      item._lease.busy = true;
      return this.queue.callPreprocess(item);
    }, {detectStuck: 5, prefetchValue: false, timeout: ms('15s')});
    try {
      const item = await transactionPromise;
      if (acquired && item !== null && this.ref.key !== PING_KEY) {
        if (!_.isObject(item)) throw new Error(`item not an object: ${item}`);
        this.queue.tasksAcquired++;
        await this.run(item as WorkerItem, startTimestamp);
      }
    } catch (error) {
      // Hardcoded retry -- hard to do anything smarter, since we failed to update the task in
      // Firebase.
      this.expiry = 0;
      if (!/timeout/i.test(error.message) || this.source.connected) {
        console.log(`Queue item ${this.key} lease transaction error: ${error.message}`);
        error.firelease = _.assign(error.firelease ?? {}, {itemKey: this.key, phase: 'leasing'});
        firelease.captureError(error);
        timers.setTimeout(this.queue.scan, ms('3s'));
      }
    }
    this.working = false;
    this.phase = this.removed ? 'done' : 'retry';
  }

  async run(item: WorkerItem, startTimestamp: number) {
    Object.defineProperty(item, '$ref', {value: this.ref});
    Object.defineProperty(item, '$leaseTimeRemaining', {get: () => {
      if (!item._lease?.expiry) return 0;
      return Math.max(0, item._lease.expiry - this.ref.now);
    }});
    this.phase = 'work';
    let result: WorkerResult;
    try {
      try {
        result = await this.queue.callWorker(item);
      } finally {
        const now = this.ref.now;
        if (now > item._lease.expiry) {
          this.phase = 'exceed';
          // If it looks like we exceeded the lease time, double-check against the current item
          // before crying wolf, in case the worker extended the lease.
          const currentItem = await this.ref.get({cache: false}) as LeaseItem | null;
          // If no item, we can't tell if it's because the worker chose to delete it early, or
          // because it overran its lease and another worker picked it up and completed it, so say
          // nothing.
          if (currentItem) {
            if (!currentItem._lease) {
              console.log(
                `Queue item ${this.key} likely exceeded its lease time by taking`,
                ms(now - startTimestamp),
                'because the item has already been deleted and replaced with a new one.');
            } else if (currentItem._lease.expiry && now > currentItem._lease.expiry) {
              console.log(
                `Queue item ${this.key} exceeded lease time of`,
                ms(currentItem._lease.expiry - startTimestamp),
                'by taking', ms(now - startTimestamp));
            }
          }
        }
      }
    } catch (processingError) {
      try {
        if (/timeout/i.test(processingError.message) && !this.source.connected) return;
        console.log(`Queue item ${this.key} processing error: ${processingError.message}`);
        processingError.firelease = _.assign(
          processingError.firelease ?? {}, {itemKey: this.key, phase: 'processing'});
        processingError.level ??= 'warning';
        firelease.captureError(processingError);
        // Reset busy flag, unless we exceeded our original lease in which case we can't be sure
        // whether another handler has already picked up the task so leave it be.
        if (this.phase !== 'exceed') await this.ref.child('_lease/busy').set(null);
      } catch (postProcessingError) {
        this.capturePostProcessingError(postProcessingError);
      }
      return;
    }

    try {
      this.phase = 'post';
      if (_.isNil(result)) {
        await this.ref.remove();  // common shortcut
        return;
      }
      const item2 = await this.ref.transaction(itemValue => {
        const currentItem = itemValue as LeaseItem | null;
        if (!currentItem) return null;
        let value = _.isFunction(result) ? result(currentItem) : result;
        if (_.isNil(value)) return null;
        if (value === firelease.RETRY) {
          if (currentItem._lease) delete currentItem._lease.time;
        } else if (_.isNumber(value) || _.isString(value)) {
          value = duration(value);
          currentItem._lease ??= {};
          currentItem._lease.expiry =
            value > 1000000000000 ? value : startTimestamp + value;
          delete currentItem._lease.time;
        } else if (_.isObject(value)) {
          currentItem._lease = value as Lease;
        } else {
          throw new Error(`Unexpected return value from worker: ${value}`);
        }
        if (currentItem._lease) delete currentItem._lease.busy;
        return currentItem;
      }, {prefetchValue: false}) as LeaseItem | null | undefined;
      if (item2) item._lease = item2._lease as Lease & {expiry: number};
    } catch (postProcessingError) {
      this.capturePostProcessingError(postProcessingError);
    }
  }

  capturePostProcessingError(error: FireleaseError) {
    if (/timeout/i.test(error.message) && !this.source.connected) return;
    console.log(`Queue item ${this.key} post-processing error: ${error.message}`);
    error.firelease = _.assign(
      error.firelease ?? {}, {itemKey: this.key, phase: 'post-processing'});
    firelease.captureError(error);
  }
}


class QueueSource {
  topRef?: NodeFire;
  mode: 'initial' | 'normal' | 'failed' | 'failsafe' | 'recovery' = 'initial';
  epoch = 0;
  connected = false;

  constructor(readonly queue: Queue, readonly ref: NodeFire) {}

  start() {
    this.listen();

    this.ref.root.child('.info/connected').on('value', snap => {
      const connected = Boolean(snap.val());
      if (this.connected === connected) return;
      this.connected = connected;
      if (this.connected) {
        // On reconnection, rescan all tasks but give Firebase a few seconds to resync values from
        // the server.
        _.delay(() => {
          if (!this.connected) return;
          this.queue.scan();
        }, ms('5s'));
      } else {
        this.epoch += 1;
        const failed =
          (this.mode === 'initial' || this.mode === 'recovery') &&
          !_.some(queues, queue => _.some(
            queue.sources,
            source => source.mode === 'failed' && source.ref.root.isEqual(this.ref.root)));
        if (failed) {
          if (this.mode === 'initial') {
            console.log(`Queue worker ${this.ref} failed to load tasks, entering failsafe mode`);
            firelease.captureError(_.assign(
              new Error('Queue worker entering failsafe mode'),
              {extra: {queue: this.ref.toString()}}));
          }
          this.mode = 'failed';
          _.defer(() => {this.mode = 'failsafe';});
          this.listen(true);
          _.delay(() => {
            if (this.mode !== 'failsafe') return;
            this.mode = 'recovery';
            this.listen();
          }, _.random(ms('1m'), ms('2m')));
        } else {
          this.mode = 'initial';
          this.listen();
        }
      }
    });
  }

  listen(failsafe = false) {
    if (this.topRef) {
      this.topRef.off('child_added', this.addTask, this);
      this.topRef.off('child_removed', this.removeTask, this);
      this.topRef.off(
        this.topRef === this.ref ? 'child_changed' : 'child_moved', this.addTask, this);
      _.forEach(tasks, (task, taskKey) => {
        if (task.source === this) this.removeTask(taskKey);
      });
    }

    let bufferSize = this.queue.options.bufferSize;
    if (failsafe) bufferSize = Math.min(bufferSize, 5);
    const bufferAll = bufferSize === Infinity;
    this.topRef =
      bufferAll ? this.ref : this.ref.orderByChild('_lease/expiry').limitToFirst(bufferSize);
    this.topRef.on('child_added', this.addTask, this.crash, this);
    this.topRef.on('child_removed', this.removeTask, this.crash, this);
    this.topRef.on(bufferAll ? 'child_changed' : 'child_moved', this.addTask, this.crash, this);

    if (!failsafe) {
      void this.finishLoading(this.epoch);
    }
  }

  async finishLoading(epoch: number) {
    try {
      await this.ref.orderByChild('_lease/expiry').limitToFirst(1).get({timeout: ms('10s')});
      if (this.epoch !== epoch) return;
      const recovered = this.mode === 'recovery' ? ', exiting failsafe mode' : '';
      console.log(`Queue worker ${this.ref} loaded tasks${recovered}`);
      if (this.mode === 'initial' || this.mode === 'recovery') this.mode = 'normal';
    } catch (error) {
      if (error.code !== 'timeout') {
        this.crash(error);
        return;
      }
      if (this.epoch !== epoch) return;
      console.log(`Queue worker ${this.ref} loading timeout, forcing failsafe mode`);
      this.ref.database.goOffline();
      _.defer(() => this.ref.database.goOnline());
    }
  }

  crash(error: FireleaseError) {
    console.log(`Queue worker ${this.ref} interrupted:`, error.message);
    error.firelease =
      _.assign(error.firelease ?? {}, {queue: this.ref.toString(), phase: 'crashing'});
    firelease.captureError(error);
    // Give the error capture a chance to process before exiting.
    _.delay(() => {process.exit(1);}, ms('3s'));

  }

  addTask(snap: LeaseSnapshot) {
    const taskKey = Task.makeKey(snap);
    let task = tasks[taskKey];
    if (blacklistedTaskKeys.has(taskKey)) {
      if (task) this.removeTask(taskKey);
      return;
    }
    if (task) {
      task.updateFrom(snap);
    } else {
      task = tasks[taskKey] = new Task(this, snap);
    }
    void this.queue.process(task);
  }

  removeTask(snapOrKey: LeaseSnapshot | string) {
    const taskKey = _.isString(snapOrKey) ? snapOrKey : Task.makeKey(snapOrKey);
    const task = tasks[taskKey];
    if (task?.source !== this) return;
    task.removed = true;
    if (task.timeout) {
      task.timeout.clear();
      delete task.timeout;
    }
    if (!task.working) delete tasks[taskKey];
  }
}


class Queue {
  refs: NodeFire[];
  ref: NodeFire;
  options: NormalizedQueueOptions;
  numConcurrent = 0;
  tasksAcquired = 0;
  worker: Worker;
  sources: QueueSource[];

  constructor(refOrRefs: QueueRef, options: QueueOptions | Worker, worker?: Worker) {
    if (_.isFunction(options)) {
      worker = options;
      options = {};
    }
    const refs = _(refOrRefs).castArray().uniqBy(item => item.toString()).value() as NodeFire[];
    if (!refs.length) throw new Error('At least one queue ref is required');
    this.refs = refs;
    this.ref = this.refs[0];
    const filledOptions = _.defaults({}, options, firelease.defaults) as Required<QueueOptions>;
    filledOptions.minLease = duration(filledOptions.minLease);
    filledOptions.maxLease = duration(filledOptions.maxLease);
    filledOptions.healthyPingLatency = duration(filledOptions.healthyPingLatency);
    this.options = filledOptions as NormalizedQueueOptions;
    this.worker = worker as Worker;
    this.sources = _.map(this.refs, sourceRef => new QueueSource(this, sourceRef));

    // Need each queue's scan function to be debounced separately.
    this.scan = _.debounce(this.scan.bind(this), 100);
  }

  start() {
    _.forEach(this.sources, source => {source.start();});
  }

  scan() {
    _.forEach(tasks, task => {
      if (task.queue === this) void this.process(task);
    });
  }

  hasQuota() {
    return this.numConcurrent < this.options.maxConcurrent &&
      globalNumConcurrent < globalMaxConcurrent;
  }

  constrainLeaseDuration(time: number) {
    return Math.min(this.options.maxLease, Math.max(time, this.options.minLease));
  }

  async process(task: Task) {
    if (task.source.connected && this.hasQuota() && task.prepare()) {
      globalNumConcurrent++;
      this.numConcurrent++;
      try {
        await task.process();
        globalNumConcurrent--;
        this.numConcurrent--;
        if (task.removed) delete tasks[task.key];
        if (globalNumConcurrent === globalMaxConcurrent - 1) {
          scanAll();
        } else if (this.numConcurrent === this.options.maxConcurrent - 1) {
          this.scan();
        }
        if (!globalMaxConcurrent && !globalNumConcurrent) shutdownResolve?.();
        if (!globalMaxConcurrent) {
          console.log(`Queues draining, tasks in progress: ${globalNumConcurrent}`);
        }
      } catch (error) {
        error.message = `Unexpected error in Queue.process: ${error.message}`;
        firelease.captureError(error);
      }
    }
  }

  callPreprocess(item: LeaseItem) {
    if (this.options.preprocess) item = this.options.preprocess(item);
    return item;
  }

  async callWorker(item: WorkerItem) {
    return this.worker(item);
  }
}


/**
 * Attaches a worker function to consume tasks from a queue.  You should normally attach no more
 * than one worker per path in any given process, but it's OK to run multiple processes on the same
 * paths concurrently.
 *
 * All durations can be specified as either a human-readable string, or a number of milliseconds.
 *
 * @param {NodeFire | NodeFire[]} refOrRefs One or more NodeFire refs to queue roots in Firebase.
 *        Individual tasks will be children of these roots and must be objects.  All refs form one
 *        logical queue and share the same worker and concurrency limits.  The '_lease' key is
 *        reserved for use by Firelease in each task.
 * @param {Object} options Optional options, supporting the following values:
 *        maxConcurrent: {number} max number of tasks to handle concurrently for this worker.
 *        bufferSize: {number} upper bound on how many tasks to keep buffered from each source and
 *          potentially go through leasing transactions in parallel.  In principle, it's not worth
 *          setting higher than `maxConcurrent`, but you can set it to `Infinity` to keep the entire
 *          task queue buffered at all times if needed.
 *        minLease: {number | string} minimum duration of each lease, which should equal the maximum
 *          expected time a worker will take to handle a task.
 *        maxLease: {number | string} maximum duration of each lease; the lease duration is doubled
 *          each time a task fails until it reaches maxLease.
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
 *        * A complete _lease object, to be saved as-is.
 *        * A function that takes the task as argument and returns one of the values above.  This
 *          function will be executed in a transaction to ensure atomicity.
 *        All of these values can also be wrapped in a promise.
 */
function attachWorker(refOrRefs: QueueRef, worker: Worker): void;
function attachWorker(refOrRefs: QueueRef, options: QueueOptions, worker: Worker): void;
function attachWorker(refOrRefs: QueueRef, options: QueueOptions | Worker, worker?: Worker): void {
  const queue = new Queue(refOrRefs, options, worker);
  queues.push(queue);
  queue.start();
}

function duration(value: Duration) {
  if (_.isNumber(value)) return value;
  return ms(value as ms.StringValue);
}


let pinging = false;
let pingIntervalHandle: timers.Interval | undefined;
let pingCallback: ((report: PingReport) => void) | null | undefined;

/**
 * Sets up regular pinging of all queues.  Can be called either before or after workers are
 * attached, and will always ping all queues.  Can be called more than once to change the
 * parameters.
 *
 * All durations can be specified as either a human-readable string, or a number of milliseconds.
 *
 * @param {Function(Object) | null} callback The callback to invoke with a report each time we ping
 *        all the queues.  The report looks like:
 *        {healthy: true, maxLatency: 1234, sickQueues: [], sickSources: []}.  sickQueues contains
 *        logical queue keys, while sickSources contains the full URLs of unhealthy physical
 *        sources.  If not specified, reports are silently dropped.
 * @param {number | string} interval The interval at which to ping queues, to both check the
 *        current response latency and make sure no tasks are stuck.  Defaults to 1 minute.
 */
function pingQueues(
  callback?: ((report: PingReport) => void) | null,
  interval?: Duration
): void {
  const normalizedInterval = duration(interval ?? PING_INTERVAL);
  pingIntervalHandle?.clear();
  pingCallback = callback;
  pingIntervalHandle = timers.setInterval(() => {
    void runPingCheck();
  }, normalizedInterval);
}

async function runPingCheck() {
  try {
    await checkPings();
  } catch (error) {
    error.firelease = _.assign(error.firelease ?? {}, {phase: 'pinging'});
    error.level = 'warning';
    firelease.captureError(error);
    pinging = false;
  }
}

async function checkPings() {
  if (pinging) return;
  pinging = true;
  const results = await Promise.all(_.map(queues, checkQueuePings));
  const availableResults = _.compact(results) as QueuePingResult[];
  if (availableResults.length) {
    // Backup scan in case tasks are stuck on a queue due to bugs.
    scanAll();
    if (pingCallback) {
      const sickQueueKeys =
        _(availableResults).reject('healthy').map(item => item.queue.ref.key).value();
      const sickSourceUrls = _(availableResults)
        .flatMap(result => result.sickSources)
        .map(source => source.ref.toString())
        .uniq()
        .value();
      pingCallback({
        healthy: _.every(availableResults, 'healthy'),
        sickQueues: sickQueueKeys,
        sickSources: sickSourceUrls,
        stuckTasks: blacklistedTaskKeys.size,
        maxLatency: _.max(_.map(availableResults, 'latency'))!,
        tasksAcquired: _.reduce(availableResults, (sum, result) => sum + result.tasksAcquired, 0)
      });
    }
  }
  pinging = false;
}

async function checkQueuePings(queue: Queue) {
  const sourceResults = await Promise.all(
    _.map(queue.sources, source => checkSourcePing(queue, source)));
  const availableResults = _.compact(sourceResults) as SourcePingResult[];
  if (!availableResults.length) return null;
  return {
    queue,
    latency: _.max(_.map(availableResults, 'latency'))!,
    healthy: _.every(availableResults, 'healthy'),
    sickSources: _(availableResults).reject('healthy').map('source').value(),
    tasksAcquired: queue.tasksAcquired
  } as QueuePingResult;
}

async function checkSourcePing(queue: Queue, source: QueueSource) {
  const start = Date.now();
  const pingRef = source.ref.child(PING_KEY);
  let pingFree = false;
  await pingRef.transaction(item => {
    pingFree = !item;
    return item ?? {timestamp: start, _lease: {expiry: NodeFire.SERVER_TIMESTAMP}};
  }, {prefetchValue: false, timeout: ms('10s')});
  if (!pingFree) return null;  // another process is currently pinging
  try {
    await waitUntilDeleted(pingRef, queue.options.healthyPingLatency + ms('10s'));
    const latency = Date.now() - start;
    return {source, latency, healthy: latency < queue.options.healthyPingLatency};
  } catch {
    return {source, latency: Date.now() - start, healthy: false};
  }
}

function waitUntilDeleted(ref: NodeFire, timeout: number) {
  return new Promise<void>((resolve, reject) => {
    function onValue(snap: LeaseSnapshot) {
      if (snap.val()) return;
      ref.off('value', onValue);
      resolve();
    }
    ref.on('value', onValue, reject);
    if (timeout) timers.setTimeout(() => {reject(new Error('timeout'));}, timeout);
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
function extendLease(item: WorkerItem, timeNeeded: Duration): Promise<void> {
  if (!item?._lease?.expiry) throw new Error('Invalid task');
  item._lease.timeNeeded = Math.max(item._lease.timeNeeded ?? 0, duration(timeNeeded));
  if (!item._lease.extendLeasePromise) {
    if (!globalMaxConcurrent) return Promise.reject(new Error('shutdown in progress'));
    item._lease.extendLeasePromise = updateLease(item, timeNeeded);
  }
  return item._lease.extendLeasePromise as Promise<void>;
}

async function updateLease(item: WorkerItem, timeNeeded: Duration) {
  let error: FireleaseError | undefined;
  let timeNeededUsed: number | null = null;
  const itemValue = await item.$ref.transaction(currentValue => {
    let currentItem = currentValue as LeaseItem | null;
    error = undefined;
    timeNeededUsed = null;
    const now = item.$ref.now;
    if (!currentItem) {
      error = new Error('Task disappeared, unable to extend lease.');
      error.firelease = {code: 'gone'};
      currentItem = null;  // make sure we attempt a write to force sha check
    } else if (!currentItem._lease) {
      error = new Error('Task recreated, unable to extend lease.');
      error.firelease = {code: 'recreated'};
    } else if (item._lease.expiry !== currentItem._lease.expiry) {
      error = new Error('Task leased by another worker, unable to extend lease.');
      error.firelease = {code: 'stolen'};
    } else if (currentItem._lease.expiry <= now) {
      error = new Error('Lease expired, unable to extend.');
      error.firelease = {code: 'lost'};
    } else {
      const currentLease = currentItem._lease as Lease & {expiry: number};
      timeNeededUsed = item._lease.timeNeeded ?? 0;
      // Expiry is monotonically increasing, so safe to do early abort if it's high enough.
      if (currentLease.expiry >= now + timeNeededUsed) return;
      currentLease.expiry += timeNeededUsed;
    }
    return currentItem;
  }, {prefetchValue: false});
  const currentItem = itemValue as LeaseItem | null | undefined;
  const activeLease = item._lease as Lease | undefined;
  const moreTimeNeeded = (activeLease?.timeNeeded ?? 0) > (timeNeededUsed ?? 0) ?
    activeLease?.timeNeeded : undefined;
  if (activeLease) {
    delete activeLease.extendLeasePromise;
    delete activeLease.timeNeeded;
  }
  if (error) {
    error.firelease = _.assign(
      error.firelease ?? {}, {itemKey: item.$ref.toString(), timeNeeded});
    throw error;
  }
  if (currentItem && activeLease) {
    activeLease.expiry = (currentItem._lease as Lease & {expiry: number}).expiry;
  }
  if (moreTimeNeeded) {
    // If an extendLease raced with the transaction then retry it.
    await firelease.extendLease(item, moreTimeNeeded);
  }
}

/**
 * Blacklist the given task key from ever being processed again.
 * @param {string} taskKey The task key to blacklist.  This is the full Firebase URL of the task and
 *        can be obtained from an error using `error.firelease.itemKey`.
 * @return {boolean} True if the task key was added to the list, false if it was already present.
 */
function blacklist(taskKey: string): boolean {
  if (blacklistedTaskKeys.has(taskKey)) return false;
  blacklistedTaskKeys.add(taskKey);
  const task = tasks[taskKey];
  if (task) task.source.removeTask(taskKey);
  return true;
}


/**
 * Shuts down firelease by refusing to take new tasks.
 * @return {Promise<void>} A promise that resolves when the shutdown is complete.
 */
function shutdown(): Promise<void> {
  globalMaxConcurrent = 0;
  if (!shutdownPromise) {
    shutdownPromise = new Promise((resolve, reject) => {
      shutdownResolve = resolve;
      shutdownReject = reject;
    });
  }
  if (!globalNumConcurrent) shutdownResolve?.();
  return shutdownPromise;
}


/**
 * Lists the URLs of all tasks that are currently being worked on.
 */
function listTasksInProgress(): string[] {
  return _(tasks).pickBy('working').keys().value();
}

export = firelease;
