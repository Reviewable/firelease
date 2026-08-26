import _ from 'lodash';
import ms from 'ms';
import type NodeFire from 'nodefire';
import type {TransactionMetadata} from 'nodefire';
import * as timers from 'safe-timers';

import type {FireleaseRuntime} from './runtime';
import type {QueueSourceStats, QueueStats} from './stats';
import type {
  AcquiredLease, Duration, FireleaseError, Lease, LeaseItem, LeaseSnapshot,
  LeaseTransactionOutcome, NormalizedQueueOptions, Worker, WorkerItem, WorkerResult
} from './types';

export const PING_KEY = 'ping';
const LEASE_TRANSACTION_DURATION_ALPHA = 0.1;

export interface TaskQueue {
  readonly ref: NodeFire;
  readonly options: NormalizedQueueOptions;
  readonly stats: QueueStats;
  constrainLeaseDuration(time: number): number;
  callPreprocess(item: LeaseItem): LeaseItem;
  callWorker(item: WorkerItem): ReturnType<Worker>;
}

export interface TaskSource {
  readonly queue: TaskQueue;
  readonly stats: QueueSourceStats;
  readonly connected: boolean;
  removeTask(snapOrKey: LeaseSnapshot | string): void;
}

export class TaskRegistry {
  readonly items: Record<string, Task> = {};
  readonly blacklistedKeys = new Set<string>();

  constructor(readonly runtime: FireleaseRuntime) {}

  get blacklistedSize() {
    return this.blacklistedKeys.size;
  }

  get(key: string) {
    return this.items[key];
  }

  create(source: TaskSource, snap: LeaseSnapshot) {
    // Task owns a reference back to this registry, so the declarations are intentionally mutual.
    // eslint-disable-next-line @typescript-eslint/no-use-before-define
    const task = new Task(this, this.runtime, source, snap);
    this.items[task.key] = task;
    return task;
  }

  delete(key: string) {
    delete this.items[key];
  }

  values() {
    return _.values(this.items);
  }

  each(iteratee: (task: Task, key: string) => void) {
    _.forEach(this.items, iteratee);
  }

  blacklist(taskKey: string) {
    if (this.blacklistedKeys.has(taskKey)) return false;
    this.blacklistedKeys.add(taskKey);
    this.items[taskKey]?.source.removeTask(taskKey);
    return true;
  }

  listInProgress() {
    return _(this.items).pickBy('working').keys().value();
  }

  reset() {
    this.each((task, taskKey) => {
      task.timeout?.clear();
      delete this.items[taskKey];
    });
    this.blacklistedKeys.clear();
  }
}

export class Task {
  readonly queue: TaskQueue;
  readonly ref: NodeFire;
  readonly key: string;
  phase: 'wait' | 'lease' | 'work' | 'exceed' | 'post' | 'done' | 'retry' = 'wait';
  expiry = 0;
  leaseRetryTime = 0;
  consecutiveLeaseTransactionFailures = 0;
  removed?: boolean;
  working = false;
  ready = false;
  readyVersion = 0;
  timeout?: timers.Timeout;

  constructor(
    readonly registry: TaskRegistry,
    readonly runtime: FireleaseRuntime,
    readonly source: TaskSource,
    snap: LeaseSnapshot
  ) {
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
    this.expiry = Math.max(value?._lease?.expiry ?? this.ref.now, this.leaseRetryTime);
    delete this.removed;
  }

  prepare() {
    if (this.registry.get(this.key) !== this || this.removed || this.working) return false;
    const now = this.ref.now;
    const busy = this.expiry > now;
    if (!busy) this.expiry = now + this.queue.constrainLeaseDuration(0);
    this.timeout?.clear();
    this.timeout = undefined;
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
      if (this.registry.get(this.key) !== this || this.removed) return;
      if (!item || this.ref.key === PING_KEY) {
        acquired = true;
        return null;
      }
      startTimestamp = this.ref.now;
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
      this.consecutiveLeaseTransactionFailures = 0;
      this.leaseRetryTime = 0;
      if (acquired && item !== null && this.ref.key !== PING_KEY) {
        this.recordLeaseTransaction('acquired', transactionPromise.transaction);
        if (firstAcquisition) Object.defineProperty(item._lease, 'firstAcquisition', {value: true});
        this.queue.stats.tasksAcquired++;
        await this.run(item as WorkerItem, startTimestamp);
      } else if (contended) {
        this.recordLeaseTransaction('contended', transactionPromise.transaction);
      } else if (acquired && item === null) {
        this.removed = true;
        reschedule = false;
      }
    } catch (error) {
      if (!transactionCompleted && this.ref.key !== PING_KEY) {
        this.recordLeaseTransaction('failed', transactionPromise.transaction);
      }
      const failures = ++this.consecutiveLeaseTransactionFailures;
      const retryDelay = ms('1s') * 2 ** (failures - 1);
      this.leaseRetryTime = this.ref.now + retryDelay;
      this.expiry = this.leaseRetryTime;
      if (failures >= 3 && (!/timeout/i.test(error.message) || this.source.connected)) {
        const report = new Error('Firelease lease transaction failed') as FireleaseError;
        report.level = 'error';
        report.firelease = {
          cause: error.message,
          code: 'lease-transaction-failed',
          count: failures,
          itemKey: this.key,
          phase: 'leasing',
          retryDelay
        };
        console.error(report.message, report.firelease);
        try {
          this.runtime.config.captureError(report);
        } catch (captureError) {
          console.error('Error capturing lease transaction failure:', captureError);
        }
      }
    }
    this.working = false;
    this.phase = this.removed ? 'done' : 'retry';
    if (!this.removed && reschedule) {
      timers.setTimeout(() => {this.runtime.scheduler.schedule(this);}, 0);
    }
  }

  recordLeaseTransaction(
    outcome: LeaseTransactionOutcome, transaction: TransactionMetadata | undefined
  ) {
    try {
      const leaseStats = this.source.stats.leaseTransactions;
      leaseStats[outcome] += 1;
      if (!transaction?.tries || transaction.duration === undefined) return;
      const tries = transaction.tries;
      const transactionDuration = (transaction.prefetchDuration ?? 0) + transaction.duration;
      leaseStats.tries += tries;
      leaseStats.duration = leaseStats.duration === 0 ? transactionDuration || 1 :
        leaseStats.duration * (1 - LEASE_TRANSACTION_DURATION_ALPHA) +
        transactionDuration * LEASE_TRANSACTION_DURATION_ALPHA;
      this.queue.options.captureLeaseTransactionMetrics?.(outcome, tries, transactionDuration);
    } catch (error) {
      try {
        const metricError: FireleaseError = _.isError(error) ? error : new Error(String(error));
        metricError.firelease = _.assign(
          metricError.firelease ?? {}, {itemKey: this.key, phase: 'lease-metric'});
        this.runtime.config.captureError(metricError);
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
          const currentItem = await this.ref.get({cache: false}) as LeaseItem | null;
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
        this.runtime.config.captureError(processingError);
        if (this.phase !== 'exceed') await this.ref.child('_lease/busy').set(null);
      } catch (postProcessingError) {
        this.handlePostProcessingError(postProcessingError);
      }
      return;
    }

    try {
      this.phase = 'post';
      if (_.isNil(result)) {
        await this.ref.remove();
        return;
      }
      const item2 = await this.ref.transaction(itemValue => {
        const currentItem = itemValue as LeaseItem | null;
        if (!currentItem) return null;
        let value = _.isFunction(result) ? result(currentItem) : result;
        if (_.isNil(value)) return null;
        if (value === this.runtime.retry) {
          if (currentItem._lease) delete currentItem._lease.time;
        } else if (_.isNumber(value) || _.isString(value)) {
          value = this.runtime.duration(value);
          currentItem._lease ??= {};
          currentItem._lease.expiry = value > 1000000000000 ? value : startTimestamp + value;
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
    this.runtime.config.captureError(error);
  }
}

export function extendTaskLease(
  runtime: FireleaseRuntime, item: WorkerItem, timeNeeded: Duration
): Promise<void> {
  if (!item?._lease?.expiry) throw new Error('Invalid task');
  item._lease.timeNeeded = Math.max(item._lease.timeNeeded ?? 0, runtime.duration(timeNeeded));
  if (!item._lease.extendLeasePromise) {
    item._lease.extendLeasePromise = updateLease(runtime, item, timeNeeded);
  }
  return item._lease.extendLeasePromise;
}

async function updateLease(
  runtime: FireleaseRuntime, item: WorkerItem, timeNeeded: Duration
) {
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
      currentItem = null;
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
  if (moreTimeNeeded) await extendTaskLease(runtime, item, moreTimeNeeded);
}
