import _ from 'lodash';
import ms from 'ms';
import NodeFire, {type TransactionMetadata} from 'nodefire';
import * as timers from 'safe-timers';
import {
  FireleaseStats, QueueSourceStats, QueueStats, type QueueSourceMode
} from './stats';

export const TESTABLES = {resetBetweenTests, waitUntilDeleted};

const PING_INTERVAL = ms('1m');
const PING_KEY = 'ping';
const QUEUE_CHECK_TIMEOUT = ms('15s');
const QUEUE_SIZE_HYSTERESIS = 0.15;
const QUEUE_SIZE_MISMATCH_THRESHOLD = 100;
const DEMOTION_JITTER = ms('30s');
const LEASE_TRANSACTION_DURATION_ALPHA = 0.1;

declare const RETRY_DIRECTIVE: unique symbol;

export type Duration = number | string;

export interface Lease {
  expiry?: number;
  time?: number;
  attempts?: number;
  initial?: number;
  busy?: boolean;
  timeNeeded?: number;
  extendLeasePromise?: Promise<void>;
}

export type AcquiredLease = Lease & {
  expiry: number, time: number, attempts: number, initial: number, readonly firstAcquisition?: true
};

export interface LeaseItem {
  _lease?: Lease;
  [key: string]: any;
}

export interface RetryDirective {
  readonly [RETRY_DIRECTIVE]: true;
}

export interface WorkerItem extends LeaseItem {
  _lease: AcquiredLease;
  readonly $ref: NodeFire;
  readonly $leaseTimeRemaining: number;
}

export type FireleaseErrorLevel = 'fatal' | 'error' | 'warning' | 'log' | 'info' | 'debug';

export interface FireleaseErrorDetails {
  cause?: string;
  code?: string;
  count?: number;
  delta?: number;
  description?: string;
  itemKey?: string;
  listenerLimit?: number;
  liveCount?: number;
  mode?: QueueSourceMode;
  phase?: string;
  queue?: string;
  reason?: string;
  source?: string;
  timeNeeded?: Duration;
  timeout?: Duration;
}

export interface FireleaseError extends Error {
  firelease?: FireleaseErrorDetails;
  level?: FireleaseErrorLevel;
}

export type LeaseTransactionOutcome = 'acquired' | 'contended' | 'failed';
export type CaptureLeaseTransactionMetrics = (
  outcome: LeaseTransactionOutcome, tries: number, duration: number
) => undefined;

export interface QueueOptions {
  maxConcurrent?: number;
  bufferSize?: number;
  minLease?: Duration;
  maxLease?: Duration;
  healthyPingLatency?: Duration;
  preprocess?: (item: LeaseItem) => LeaseItem;
  captureLeaseTransactionMetrics?: CaptureLeaseTransactionMetrics;
}

export type PingReport = FireleaseStats;

export type QueueRef = NodeFire | NodeFire[];
export type WorkerResult = RetryDirective | Duration | Lease | null | void |
  ((item: LeaseItem) => RetryDirective | Duration | Lease | null | void);
export type Worker = (item: WorkerItem) => WorkerResult | PromiseLike<WorkerResult>;

export interface FireleaseSettings {
  globalMaxConcurrent: number;
  safeQueueSize: number;
  queueCheckInterval: Duration;
  queueLoadTimeout: Duration;
  captureError: (error: FireleaseError) => void;
}

export interface FireleaseApi {
  readonly RETRY: RetryDirective;
  readonly settings: FireleaseSettings;
  readonly defaults: QueueOptions;
  readonly stats: FireleaseStats;
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
  captureLeaseTransactionMetrics?: CaptureLeaseTransactionMetrics;
}

const queues: Queue[] = [];
const tasks: Record<string, Task> = {};
const blacklistedTaskKeys = new Set<string>();
let globalMaxConcurrent = Number.MAX_VALUE;
let globalNumConcurrent = 0;
let safeQueueSize = 6000;
let queueCheckInterval = ms('5m');
let queueLoadTimeout = ms('1m');
let shutdownResolve: (() => void) | undefined;
let shutdownReject: ((error: Error) => void) | undefined;
let shutdownPromise: Promise<void> | undefined;

const defaultCaptureError = (error: FireleaseError) => {console.error(error.stack);};

/**
 * Return this from a worker to retry after the current lease expires, and to reset the lease
 * backoff to zero.
 */
export const RETRY = {} as RetryDirective;

/** Default option values for all subsequent attachWorker calls. */
export const defaults: QueueOptions = {
  maxConcurrent: Number.MAX_VALUE, bufferSize: Infinity, minLease: '30s', maxLease: '1h',
  healthyPingLatency: '1.5s'
};

export const stats = new FireleaseStats(() => blacklistedTaskKeys.size);

const scanAll = _.debounce(() => {
  _.forEach(tasks, task => {
    void task.queue.process(task);
  });
}, 100);

export const settings: FireleaseSettings = {
  get globalMaxConcurrent() {
    return globalMaxConcurrent;
  },
  set globalMaxConcurrent(value: number) {
    globalMaxConcurrent = value;
    if (value) {
      shutdownReject?.(new Error('Queues restarted'));
      shutdownPromise = shutdownResolve = shutdownReject = undefined;
      scanAll();
    }
  },
  get safeQueueSize() {
    return safeQueueSize;
  },
  set safeQueueSize(value: number) {
    if (!Number.isFinite(value) || value < 1) {
      throw new Error('safeQueueSize must be a positive finite number');
    }
    safeQueueSize = Math.floor(value);
  },
  get queueCheckInterval() {
    return queueCheckInterval;
  },
  set queueCheckInterval(value: Duration) {
    const normalizedValue = duration(value);
    if (!Number.isFinite(normalizedValue) || normalizedValue <= 0) {
      throw new Error('queueCheckInterval must be a positive finite duration');
    }
    queueCheckInterval = normalizedValue;
  },
  get queueLoadTimeout() {
    return queueLoadTimeout;
  },
  set queueLoadTimeout(value: Duration) {
    const normalizedValue = duration(value);
    if (!Number.isFinite(normalizedValue) || normalizedValue <= 0) {
      throw new Error('queueLoadTimeout must be a positive finite duration');
    }
    queueLoadTimeout = normalizedValue;
  },
  captureError: defaultCaptureError
};

const firelease = Object.freeze({
  RETRY,
  settings,
  defaults,
  stats,
  attachWorker,
  pingQueues,
  extendLease,
  blacklist,
  shutdown,
  listTasksInProgress
}) as FireleaseApi;


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
    let contended = false;
    let reschedule = true;
    let firstAcquisition = false;
    this.working = true;
    this.phase = 'lease';
    const transactionPromise = this.ref.transaction(itemValue => {
      const item = itemValue as LeaseItem | null;
      acquired = false;
      contended = false;
      firstAcquisition = false;
      if (tasks[this.key] !== this || this.removed) return;
      if (!item || this.ref.key === PING_KEY) {
        acquired = true;
        return null;
      }
      startTimestamp = this.ref.now;
      // console.log('txn  ', this.ref.key, 'lease', item._lease, 'now', startTimestamp);
      // Check if another process beat us to it.
      if (item._lease?.expiry && item._lease.expiry > startTimestamp) {
        contended = true;
        return item;
      }
      acquired = true;
      firstAcquisition = _.isNil(item._lease?.initial);
      item._lease ??= {};
      item._lease.time = this.queue.constrainLeaseDuration((item._lease.time ?? 0) * 2);
      item._lease.expiry = startTimestamp + item._lease.time;
      item._lease.attempts = (item._lease.attempts ?? 0) + 1;
      item._lease.initial ??= startTimestamp;
      item._lease.busy = true;
      return this.queue.callPreprocess(item);
    }, {detectStuck: 5, prefetchValue: false, timeout: ms('15s')});
    let transactionCompleted = false;
    try {
      const item = await transactionPromise;
      transactionCompleted = true;
      if (acquired && item !== null && this.ref.key !== PING_KEY) {
        this.recordLeaseTransaction('acquired', transactionPromise.transaction);
        if (firstAcquisition) Object.defineProperty(item._lease, 'firstAcquisition', {value: true});
        this.queue.stats.tasksAcquired++;
        await this.run(item as WorkerItem, startTimestamp);
      } else if (contended) {
        this.recordLeaseTransaction('contended', transactionPromise.transaction);
      }
    } catch (error) {
      if (!transactionCompleted && this.ref.key !== PING_KEY) {
        this.recordLeaseTransaction('failed', transactionPromise.transaction);
      }
      reschedule = false;
      // Hardcoded retry -- hard to do anything smarter, since we failed to update the task in
      // Firebase.
      this.expiry = 0;
      if (!/timeout/i.test(error.message) || this.source.connected) {
        console.log(`Queue item ${this.key} lease transaction error: ${error.message}`);
        error.firelease = _.assign(error.firelease ?? {}, {itemKey: this.key, phase: 'leasing'});
        settings.captureError(error);
        timers.setTimeout(this.queue.scan, ms('3s'));
      }
    }
    this.working = false;
    this.phase = this.removed ? 'done' : 'retry';
    if (!this.removed && reschedule) {
      // Wait until Queue.process() releases this task's concurrency slot before re-arming its
      // lease-expiry timer.  Listener swaps can replay the task while it is still working.
      timers.setTimeout(() => {void this.queue.process(this);}, 0);
    }
  }

  recordLeaseTransaction(
    outcome: LeaseTransactionOutcome, transaction: TransactionMetadata | undefined
  ) {
    try {
      const leaseStats = this.source.stats.leaseTransactions;
      leaseStats[outcome] += 1;
      if (!transaction?.tries || transaction?.duration === undefined) return;
      const tries = transaction?.tries;
      const transactionDuration = (transaction?.prefetchDuration ?? 0) + transaction?.duration;
      leaseStats.tries += tries;
      leaseStats.duration = leaseStats.duration === 0 ?
        transactionDuration || 1 :
        leaseStats.duration * (1 - LEASE_TRANSACTION_DURATION_ALPHA) +
          transactionDuration * LEASE_TRANSACTION_DURATION_ALPHA;
      this.queue.options.captureLeaseTransactionMetrics?.(outcome, tries, transactionDuration);
    } catch (error) {
      try {
        const metricError: FireleaseError = _.isError(error) ? error : new Error(String(error));
        metricError.firelease = _.assign(
          metricError.firelease ?? {}, {itemKey: this.key, phase: 'lease-metric'});
        settings.captureError(metricError);
      } catch (captureError) {
        try {
          console.error('Error capturing lease transaction metric error:', captureError);
        } catch {
          // Metric recording must never interrupt task processing.
        }
      }
    }
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
        settings.captureError(processingError);
        // Reset busy flag, unless we exceeded our original lease in which case we can't be sure
        // whether another handler has already picked up the task so leave it be.
        if (this.phase !== 'exceed') await this.ref.child('_lease/busy').set(null);
      } catch (postProcessingError) {
        this.handlePostProcessingError(postProcessingError);
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
      if (item2) item._lease = item2._lease as AcquiredLease;
    } catch (postProcessingError) {
      this.handlePostProcessingError(postProcessingError);
    }
  }

  handlePostProcessingError(error: FireleaseError) {
    if (/timeout/i.test(error.message) && !this.source.connected) return;
    console.log(`Queue item ${this.key} post-processing error: ${error.message}`);
    error.firelease = _.assign(
      error.firelease ?? {}, {itemKey: this.key, phase: 'post-processing'});
    settings.captureError(error);
  }
}


interface QueueCheckJob {
  source: QueueSource;
  epoch: number;
  description: string;
  run: () => Promise<void>;
  resolve: () => void;
}

type QueueLoadResult = 'loaded' | 'stopped' | 'timed-out';

class QueueCheckQueue {
  jobs: QueueCheckJob[] = [];
  active?: QueueCheckJob;
  draining = false;
  previousDuration = 0;
  previousFinishedAt = 0;

  enqueue(source: QueueSource, description: string, epoch: number, run: () => Promise<void>) {
    const duplicate =
      (this.active?.source === source && this.active.epoch === epoch) ||
      // Match shorthand would deep-compare QueueSource, but identity is required here.
      // eslint-disable-next-line lodash/matches-shorthand
      _.some(this.jobs, job => job.source === source && job.epoch === epoch);
    if (duplicate) {
      source.reportError(
        'queue-check-coalesced', 'Firelease queue check coalesced', 'warning', {description});
      return Promise.resolve();
    }
    return new Promise<void>(resolve => {
      this.jobs.push({source, epoch, description, run, resolve});
      void this.drain();
    });
  }

  reset() {
    _.forEach(this.jobs.splice(0), job => {job.resolve();});
    this.previousDuration = 0;
    this.previousFinishedAt = 0;
  }

  async drain() {
    if (this.draining) return;
    this.draining = true;
    while (this.jobs.length) {
      const job = this.jobs.shift()!;
      try {
        if (!job.source.isCurrent(job.epoch)) {
          job.resolve();
          continue;
        }

        // Don't exceed a 50% duty cycle.
        const minimumStart = this.previousFinishedAt + this.previousDuration;
        const now = performance.now();
        if (minimumStart > now) {
          await new Promise<void>(resolve => {
            timers.setTimeout(resolve, minimumStart - now);
          });
        }

        if (!job.source.isCurrent(job.epoch)) {
          job.resolve();
          continue;
        }

        this.active = job;
        const start = performance.now();
        try {
          await job.run();
        } catch (error) {
          job.source.crash(
            'queue-check-failed', 'Firelease queue check failed', error,
            {description: job.description});
        } finally {
          const finishedAt = performance.now();
          this.previousDuration = finishedAt - start;
          this.previousFinishedAt = finishedAt;
          this.active = undefined;
          job.resolve();
        }

      } catch (error) {
        job.source.crash(
          'queue-check-queue-failed', 'Firelease queue check queue failed', error,
          {description: job.description});
        break;
      }
    }
    this.draining = false;
  }
}

const queueCheckQueue = new QueueCheckQueue();

class QueueListener {
  readonly query: NodeFire;
  readonly changeEvent: 'child_changed' | 'child_moved';
  readonly snapshots = new Map<string, LeaseSnapshot>();
  loaded = false;
  stopped = false;
  observers = new Set<(listener: QueueListener) => void>();

  constructor(readonly source: QueueSource, readonly mode: QueueSourceMode) {
    const limit = source.listenerLimit(mode);
    this.query =
      mode === 'full' ? source.ref : source.ref.orderByChild('_lease/expiry').limitToFirst(limit);
    this.changeEvent = mode === 'full' ? 'child_changed' : 'child_moved';
    if (source.adaptive && mode === 'full') this.observers.add(source.onListenerSize);
  }

  start() {
    this.query.on('child_added', this.onAdd, this.onError);
    this.query.on('child_removed', this.onRemove, this.onError);
    this.query.on(this.changeEvent, this.onAdd, this.onError);
    this.query.on('value', this.onValue, this.onError);
  }

  readonly onAdd = (snap: LeaseSnapshot) => {
    this.snapshots.set(Task.makeKey(snap), snap);
    this.source.addTask(snap);
    this.notify();
  };

  readonly onRemove = (snap: LeaseSnapshot) => {
    this.snapshots.delete(Task.makeKey(snap));
    this.source.removeTask(snap);
    this.notify();
  };

  readonly onValue = () => {
    if (this.stopped) return;
    this.loaded = true;
    this.query.off('value', this.onValue);
    this.notify();
  };

  readonly onError = (error: FireleaseError) => {
    this.source.crash(
      'queue-listener-failed', 'Firelease queue listener failed', error, {mode: this.mode});
    this.stop();
  };

  waitForLoad(timeout: number): Promise<QueueLoadResult> {
    if (this.loaded) return Promise.resolve('loaded');
    if (this.stopped) return Promise.resolve('stopped');
    return new Promise(resolve => {
      let timeoutHandle: timers.Timeout;  // eslint-disable-line prefer-const
      const onChange = () => {
        if (!this.loaded && !this.stopped) return;
        this.observers.delete(onChange);
        timeoutHandle?.clear();
        resolve(this.loaded ? 'loaded' : 'stopped');
      };
      this.observers.add(onChange);
      timeoutHandle = timers.setTimeout(() => {
        this.observers.delete(onChange);
        resolve('timed-out');
      }, timeout);
    });
  }

  stop() {
    if (this.stopped) return;
    this.stopped = true;
    this.query.off('child_added', this.onAdd);
    this.query.off('child_removed', this.onRemove);
    this.query.off(this.changeEvent, this.onAdd);
    this.query.off('value', this.onValue);
    this.observers.delete(this.source.onListenerSize);
    this.notify();
  }

  notify() {
    for (const observer of this.observers) observer(this);
  }
}

class QueueSource {
  mode: QueueSourceMode = 'safe';
  epoch = 0;
  connected = false;
  crashing = false;
  activeListener?: QueueListener;
  checkTimer?: timers.Timeout;
  demotionTimer?: timers.Timeout;
  exitTimer?: timers.Timeout;
  promotionNotBeforeTimestamp = 0;
  initialStartupComplete = false;
  readonly connectionRef: NodeFire;
  readonly stats: QueueSourceStats;

  constructor(readonly queue: Queue, readonly ref: NodeFire) {
    this.connectionRef = this.ref.root.child('.info/connected');
    this.stats = new QueueSourceStats(ref.toString());
  }

  get adaptive() {
    return this.queue.options.bufferSize === Infinity;
  }

  start() {
    this.connectionRef.on('value', this.onConnection);
  }

  readonly onConnection = (snap: LeaseSnapshot) => {
    const connected = Boolean(snap.val());
    if (this.connected === connected) return;
    this.connected = connected;
    this.stats.connected = connected;
    this.epoch++;
    const epoch = this.epoch;
    this.cancelPendingWork();
    if (connected) {
      this.stats.healthy = true;
      void this.enqueueStartup(epoch);
    } else {
      this.activeListener?.stop();
      this.activeListener = undefined;
      this.clearTasks();
      if (this.stats.size !== null && this.stats.sizeTimestamp === undefined) {
        this.stats.sizeTimestamp = Date.now();
      }
      this.stats.healthy = false;
    }
  };

  reset() {
    this.connectionRef.off('value', this.onConnection);
    this.connected = false;
    this.epoch++;
    this.cancelPendingWork();
    this.exitTimer?.clear();
    this.exitTimer = undefined;
    this.activeListener?.stop();
    this.activeListener = undefined;
    this.clearTasks();
  }

  async enqueueStartup(epoch: number) {
    await queueCheckQueue.enqueue(this, 'startup', epoch, () => this.initialize(epoch));
  }

  async initialize(epoch: number) {
    try {
      let targetMode: QueueSourceMode = 'safe';
      if (this.adaptive) {
        const count = await this.probeSize('startup');
        if (!this.isCurrent(epoch)) return;
        targetMode = count !== null && count < fullQueueSize() ? 'full' : 'safe';
      }
      const loadedMode = await this.loadListener(targetMode, epoch, 'startup');
      if (!loadedMode) return;
      const loadedTaskCount = this.activeListener?.snapshots.size ?? 0;
      console.log(`Queue worker ${this.ref} loaded ${loadedTaskCount} tasks in ${loadedMode} mode`);
      if (loadedMode === 'safe') this.scheduleSafeCheck();
      this.initialStartupComplete = true;
    } catch (e) {
      if (this.initialStartupComplete) throw e;
      this.crash(
        'queue-startup-failed', 'Firelease queue startup failed', e,
        {description: 'startup'});
    }
  }

  isCurrent(epoch: number) {
    return !this.crashing && this.connected && this.epoch === epoch;
  }

  listenerLimit(mode: QueueSourceMode) {
    if (mode === 'full') return Infinity;
    const limit = this.adaptive ? settings.safeQueueSize : this.queue.options.bufferSize;
    return Math.max(1, Math.floor(limit));
  }

  async probeSize(reason: string) {
    try {
      const keys = await this.ref.childrenKeys({timeout: QUEUE_CHECK_TIMEOUT});
      this.stats.size = keys.length;
      delete this.stats.sizeDelta;
      this.stats.sizeTimestamp = Date.now();
      return keys.length;
    } catch (error) {
      this.reportError(
        'queue-count-failed', 'Firelease queue count failed', 'warning',
        {cause: error.message, reason});
      return null;
    }
  }

  scheduleSafeCheck() {
    this.checkTimer?.clear();
    if (!(this.adaptive && this.connected && this.mode === 'safe')) return;
    const interval = queueCheckInterval;
    const jitteredInterval = Math.max(0, Math.round(interval * (0.95 + Math.random() * 0.1)));
    const epoch = this.epoch;
    this.checkTimer = timers.setTimeout(() => {
      this.checkTimer = undefined;
      this.scheduleSafeCheck();
      void queueCheckQueue.enqueue(
        this, 'scheduled shallow count', epoch, () => this.runScheduledCheck(epoch));
    }, jitteredInterval);
  }

  async runScheduledCheck(epoch: number) {
    if (!this.isCurrent(epoch) || this.mode !== 'safe') return;
    const count = await this.probeSize('scheduled');
    if (!this.isCurrent(epoch) || count === null || this.mode !== 'safe') return;
    const liveCount = this.activeListener?.snapshots.size ?? 0;
    const listenerLimit = this.listenerLimit('safe');
    const delta = count - liveCount;
    if (liveCount < listenerLimit) this.stats.sizeDelta = delta;
    else delete this.stats.sizeDelta;
    if (this.stats.sizeDelta !== undefined && delta >= QUEUE_SIZE_MISMATCH_THRESHOLD) {
      this.reportError(
        'safe-queue-size-mismatch', 'Firelease safe queue size mismatch', 'error',
        {count, delta, listenerLimit, liveCount});
    }
    if (count < fullQueueSize() && performance.now() >= this.promotionNotBeforeTimestamp) {
      await this.promote(epoch);
    }
  }

  async promote(epoch: number) {
    if (!this.isCurrent(epoch) || this.mode !== 'safe') return;
    const loadedMode = await this.loadListener('full', epoch, 'promotion');
    if (!loadedMode) return;
    if (loadedMode === 'safe') {
      this.scheduleSafeCheck();
      return;
    }
    console.log(
      `Queue worker ${this.ref} promoted to full mode with` +
        ` ${this.activeListener?.snapshots.size ?? 0} tasks`);
  }

  scheduleDemotion() {
    if (!(this.adaptive && this.connected && this.mode === 'full' && !this.demotionTimer)) return;
    const epoch = this.epoch;
    const delay = Math.round(Math.random() * DEMOTION_JITTER);
    console.log(`Queue worker ${this.ref} scheduling demotion with ${ms(delay)} jitter`);
    this.demotionTimer = timers.setTimeout(() => {
      this.demotionTimer = undefined;
      void queueCheckQueue.enqueue(this, 'live-count demotion', epoch, () => this.demote(epoch));
    }, delay);
  }

  async demote(epoch: number) {
    if (!this.isCurrent(epoch) || this.mode !== 'full' ||
        (this.activeListener?.snapshots.size ?? 0) <= settings.safeQueueSize) return;
    const lastFullSize = this.activeListener?.snapshots.size ?? 0;
    if (!await this.loadListener('safe', epoch, 'demotion')) return;
    this.stats.size = lastFullSize;
    delete this.stats.sizeDelta;
    this.stats.sizeTimestamp = Date.now();
    console.log(
      `Queue worker ${this.ref} demoted to safe mode with` +
        ` ${this.activeListener?.snapshots.size ?? 0} buffered tasks`);
    this.scheduleSafeCheck();
  }

  async loadListener(
    mode: QueueSourceMode, epoch: number, description: string
  ): Promise<QueueSourceMode | undefined> {
    let details = {description, mode, timeout: queueLoadTimeout};
    const result = await this.replaceListener(mode, epoch);
    if (result === 'loaded') {
      if (mode === 'full') this.promotionNotBeforeTimestamp = 0;
      return mode;
    }
    if (result === 'stopped') return;

    if (mode === 'full') {
      this.reportError(
        'queue-load-timeout', 'Firelease queue load timed out', 'warning', details);
      this.promotionNotBeforeTimestamp = performance.now() + queueCheckInterval * 3;
      const fallbackResult = await this.replaceListener('safe', epoch);
      if (fallbackResult === 'loaded') return 'safe';
      if (fallbackResult === 'stopped') return;
      details = {description: `${description} fallback`, mode: 'safe', timeout: queueLoadTimeout};
    }

    this.crash(
      'queue-load-timeout', 'Firelease queue load timed out', new Error('timeout'), details);
  }

  async replaceListener(mode: QueueSourceMode, epoch: number): Promise<QueueLoadResult> {
    if (!this.isCurrent(epoch)) return 'stopped';
    this.activeListener?.stop();
    this.activeListener = undefined;
    this.clearTasks();
    if (mode === 'full') {
      this.checkTimer?.clear();
      this.checkTimer = undefined;
    } else {
      this.demotionTimer?.clear();
      this.demotionTimer = undefined;
    }
    const listener = new QueueListener(this, mode);
    this.activeListener = listener;
    listener.start();
    const result = await listener.waitForLoad(queueLoadTimeout);
    if (result !== 'loaded' || !this.isCurrent(epoch) || this.activeListener !== listener) {
      listener.stop();
      if (this.activeListener === listener) {
        this.activeListener = undefined;
        this.clearTasks();
      }
      return result === 'timed-out' && this.isCurrent(epoch) ? 'timed-out' : 'stopped';
    }
    this.mode = mode;
    this.stats.mode = mode;
    listener.notify();
    return 'loaded';
  }

  readonly onListenerSize = (listener: QueueListener) => {
    if (listener !== this.activeListener || !listener.loaded) return;
    const size = listener.snapshots.size;
    this.stats.size = size;
    delete this.stats.sizeDelta;
    delete this.stats.sizeTimestamp;
    if (size > settings.safeQueueSize) {
      this.scheduleDemotion();
    } else if (this.demotionTimer) {
      this.demotionTimer.clear();
      this.demotionTimer = undefined;
    }
  };

  cancelPendingWork() {
    this.checkTimer?.clear();
    this.checkTimer = undefined;
    this.demotionTimer?.clear();
    this.demotionTimer = undefined;
    this.promotionNotBeforeTimestamp = 0;
  }

  async checkPing() {
    const startedAt = performance.now();
    const timestamp = Date.now();
    const pingRef = this.ref.child(PING_KEY);
    let pingFree = false;
    try {
      await pingRef.transaction(item => {
        pingFree = !item;
        return item ?? {timestamp, _lease: {expiry: NodeFire.SERVER_TIMESTAMP}};
      }, {prefetchValue: false, timeout: ms('10s')});
    } catch (error) {
      this.recordPingResult(startedAt, false);
      throw error;
    }
    if (!pingFree) return;  // another process is currently pinging
    try {
      await waitUntilDeleted(pingRef, this.queue.options.healthyPingLatency + ms('10s'));
    } catch {
      this.recordPingResult(startedAt, false);
      return;
    }
    this.recordPingResult(startedAt, true);
  }

  recordPingResult(startedAt: number, succeeded: boolean) {
    const latency = Math.round(performance.now() - startedAt);
    this.stats.latency = latency;
    this.stats.healthy = succeeded && latency < this.queue.options.healthyPingLatency;
    this.stats.pingTimestamp = Date.now();
  }

  reportError(
    code: string, message: string, level: FireleaseErrorLevel, details: FireleaseErrorDetails = {}
  ) {
    const error = new Error(message) as FireleaseError;
    error.level = level;
    error.firelease = {
      ...details, code, phase: 'queue-sizing', queue: this.queue.ref.toString(),
      source: this.ref.toString()
    };
    if (level === 'error') console.error(message, error.firelease);
    else console.warn(message, error.firelease);
    settings.captureError(error);
  }

  crash(
    code: string, message: string, cause: FireleaseError, details: FireleaseErrorDetails = {}
  ) {
    if (this.crashing) return;
    this.crashing = true;
    this.cancelPendingWork();
    const error = new Error(message) as FireleaseError;
    error.level = 'fatal';
    error.firelease = {
      ...details, cause: cause.message, code, phase: 'crashing',
      queue: this.queue.ref.toString(), source: this.ref.toString()
    };
    console.error(message, error.firelease);
    settings.captureError(error);
    // Give the error capture a chance to process before exiting.
    this.exitTimer = timers.setTimeout(() => {process.exit(1);}, ms('3s'));
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

  clearTasks() {
    _.forEach(tasks, (task, taskKey) => {
      if (task.source === this) this.removeTask(taskKey);
    });
  }
}


class Queue {
  refs: NodeFire[];
  ref: NodeFire;
  options: NormalizedQueueOptions;
  numConcurrent = 0;
  worker: Worker;
  sources: QueueSource[];
  readonly stats: QueueStats;

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
    this.stats = new QueueStats(this.ref.toString(), this.ref.key, _.map(this.sources, 'stats'));
    stats.queues.push(this.stats);

    // Need each queue's scan function to be debounced separately.
    this.scan = _.debounce(this.scan.bind(this), 100);
  }

  start() {
    _.forEach(this.sources, source => {source.start();});
  }

  reset() {
    (this.scan as _.DebouncedFunc<Queue['scan']>).cancel();
    _.forEach(this.sources, source => {source.reset();});
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
        settings.captureError(error);
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
 *          potentially go through leasing transactions in parallel.  It defaults to `Infinity`,
 *          which is preferred for efficiency and correctness unless the queue will usually remain
 *          above `settings.safeQueueSize`.  `Infinity` adapts between a full listener and a safe
 *          listener limited to `settings.safeQueueSize` tasks.  Use a finite value only to keep an
 *          ordinarily large queue permanently on a limited listener.  An explicit finite value is
 *          used as-is and may be greater than `settings.safeQueueSize`.
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
 *        captureLeaseTransactionMetrics: {function(string, number, number)} a callback invoked
 *          after each acquired, contended, or failed task lease transaction with its outcome,
 *          NodeFire transaction tries, and duration in milliseconds.  The callback must be
 *          synchronous.
 * @param {function(Object):RETRY | number | string | undefined} worker The worker function that
 *        handles enqueued tasks.  It will be given a task object as argument, with a special $ref
 *        attribute set to the Nodefire ref of that task.  On a task's first acquisition its _lease
 *        also has a non-enumerable firstAcquisition property set to true; it is not saved to
 *        Firebase and is absent on subsequent acquisitions.  The worker can perform arbitrary
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
export function attachWorker(refOrRefs: QueueRef, worker: Worker): void;
export function attachWorker(refOrRefs: QueueRef, options: QueueOptions, worker: Worker): void;
export function attachWorker(
  refOrRefs: QueueRef,
  options: QueueOptions | Worker,
  worker?: Worker
): void {
  const queue = new Queue(refOrRefs, options, worker);
  queues.push(queue);
  queue.start();
}

function duration(value: Duration) {
  if (_.isNumber(value)) return value;
  return ms(value as ms.StringValue);
}

function fullQueueSize() {
  return settings.safeQueueSize * (1 - QUEUE_SIZE_HYSTERESIS);
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
 * @param {Function(Object) | null} callback The callback to invoke with the live `stats` object
 *        each time we ping all the queues.  It retains the existing global fields and adds
 *        structured results for every logical queue and physical source.  If not specified,
 *        reports are silently dropped.
 * @param {number | string} interval The interval at which to ping queues, to both check the
 *        current response latency and make sure no tasks are stuck.  Defaults to 1 minute.
 */
export function pingQueues(
  callback?: ((report: PingReport) => void) | null,
  interval?: Duration
): void {
  const normalizedInterval = interval ? duration(interval) : PING_INTERVAL;
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
    settings.captureError(error);
    pinging = false;
  }
}

async function checkPings() {
  if (pinging) return;
  pinging = true;
  await Promise.all(_(queues)
    .flatMap(queue => queue.sources)
    .map(source => source.checkPing())
    .value());
  // Backup scan in case tasks are stuck on a queue due to bugs.
  scanAll();
  pingCallback?.(stats);
  pinging = false;
}

function waitUntilDeleted(ref: NodeFire, timeout: number) {
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    let timeoutHandle: timers.Timeout | undefined;  // eslint-disable-line prefer-const
    function finish(error?: Error) {
      if (settled) return;
      settled = true;
      timeoutHandle?.clear();
      ref.off('value', onValue);
      if (error) reject(error);
      else resolve();
    }
    function onValue(snap: LeaseSnapshot) {
      if (snap.val()) return;
      finish();
    }
    timeoutHandle = timeout ?
      timers.setTimeout(() => {finish(new Error('timeout'));}, timeout) : undefined;
    try {
      ref.on('value', onValue, finish);
    } catch (error) {
      finish(error);
    }
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
export function extendLease(item: WorkerItem, timeNeeded: Duration): Promise<void> {
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
export function blacklist(taskKey: string): boolean {
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
export function shutdown(): Promise<void> {
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
export function listTasksInProgress(): string[] {
  return _(tasks).pickBy('working').keys().value();
}

function resetBetweenTests() {
  scanAll.cancel();
  pingIntervalHandle?.clear();
  pingIntervalHandle = undefined;
  pingCallback = undefined;
  pinging = false;

  _.forEach(queues, queue => {queue.reset();});
  queueCheckQueue.reset();
  _.forEach(tasks, (task, taskKey) => {
    task.timeout?.clear();
    delete tasks[taskKey];
  });
  queues.length = 0;
  stats.queues.length = 0;
  blacklistedTaskKeys.clear();
  globalMaxConcurrent = Number.MAX_VALUE;
  globalNumConcurrent = 0;
  safeQueueSize = 6000;
  queueCheckInterval = ms('5m');
  queueLoadTimeout = ms('1m');
  shutdownResolve = shutdownReject = shutdownPromise = undefined;
  delete defaults.preprocess;
  delete defaults.captureLeaseTransactionMetrics;
  _.assign(defaults, {
    maxConcurrent: Number.MAX_VALUE,
    bufferSize: Infinity,
    minLease: '30s',
    maxLease: '1h',
    healthyPingLatency: '1.5s'
  });
  settings.captureError = defaultCaptureError;
}

export default firelease;
