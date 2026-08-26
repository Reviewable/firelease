import _ from 'lodash';
import ms from 'ms';
import NodeFire, {type OperationDescriptor} from 'nodefire';
import * as timers from 'safe-timers';

import {waitUntilDeleted} from './queues';
import {FireleaseRuntime} from './runtime';
import {extendTaskLease} from './tasks';
import type {
  Duration, FireleaseApi, FireleaseError, FireleaseSettings, PingReport, QueueOptions, QueueRef,
  ReleaseLimit, Worker, WorkerItem
} from './types';

const PING_INTERVAL = ms('1m');
const runtime = new FireleaseRuntime();
let pinging = false;
let pingIntervalHandle: timers.Interval | undefined;
let pingCallback: ((report: PingReport) => void) | null | undefined;

/** Return this from a worker to retry after the current lease expires. */
export const RETRY = runtime.retry;

/** Default option values for all subsequent attachWorker calls. */
export const defaults = runtime.defaults;
export const stats = runtime.stats;

export const settings: FireleaseSettings = {
  get safeQueueSize() {
    return runtime.config.safeQueueSize;
  },
  set safeQueueSize(value: number) {
    if (!Number.isFinite(value) || value < 1) {
      throw new Error('safeQueueSize must be a positive finite number');
    }
    runtime.config.safeQueueSize = Math.floor(value);
  },
  get queueCheckInterval() {
    return runtime.config.queueCheckInterval;
  },
  set queueCheckInterval(value: Duration) {
    const normalizedValue = runtime.duration(value);
    if (!Number.isFinite(normalizedValue) || normalizedValue <= 0) {
      throw new Error('queueCheckInterval must be a positive finite duration');
    }
    runtime.config.queueCheckInterval = normalizedValue;
  },
  get queueLoadTimeout() {
    return runtime.config.queueLoadTimeout;
  },
  set queueLoadTimeout(value: Duration) {
    const normalizedValue = runtime.duration(value);
    if (!Number.isFinite(normalizedValue) || normalizedValue <= 0) {
      throw new Error('queueLoadTimeout must be a positive finite duration');
    }
    runtime.config.queueLoadTimeout = normalizedValue;
  },
  get writeLatencyTarget() {
    return runtime.config.writeLatencyTarget;
  },
  set writeLatencyTarget(value: Duration) {
    const normalizedValue = runtime.duration(value);
    if (!Number.isFinite(normalizedValue) || normalizedValue <= 0) {
      throw new Error('writeLatencyTarget must be a positive finite duration');
    }
    runtime.config.writeLatencyTarget = normalizedValue;
    runtime.databases.restartObservations();
  },
  get initialTaskStartRate() {
    return runtime.config.initialTaskStartRate;
  },
  set initialTaskStartRate(value: number) {
    validateTaskStartRate(value, 'initialTaskStartRate');
    if (value > runtime.config.maxTaskStartRate) {
      throw new Error('initialTaskStartRate cannot exceed maxTaskStartRate');
    }
    runtime.config.initialTaskStartRate = value;
  },
  get maxTaskStartRate() {
    return runtime.config.maxTaskStartRate;
  },
  set maxTaskStartRate(value: number) {
    validateTaskStartRate(value, 'maxTaskStartRate');
    if (value < runtime.config.initialTaskStartRate) {
      throw new Error('maxTaskStartRate cannot be less than initialTaskStartRate');
    }
    runtime.config.maxTaskStartRate = value;
    runtime.databases.clampRatesToMaximum();
  },
  get taskStartRateAdjustmentInterval() {
    return runtime.config.taskStartRateAdjustmentInterval;
  },
  set taskStartRateAdjustmentInterval(value: Duration) {
    const normalizedValue = runtime.duration(value);
    if (!Number.isFinite(normalizedValue) || normalizedValue <= 0) {
      throw new Error('taskStartRateAdjustmentInterval must be a positive finite duration');
    }
    runtime.config.taskStartRateAdjustmentInterval = normalizedValue;
    runtime.databases.restartObservations();
  },
  get taskStartRateMinSamples() {
    return runtime.config.taskStartRateMinSamples;
  },
  set taskStartRateMinSamples(value: number) {
    if (!Number.isFinite(value) || value < 1 || Math.floor(value) !== value) {
      throw new Error('taskStartRateMinSamples must be a positive finite integer');
    }
    runtime.config.taskStartRateMinSamples = value;
  },
  get captureError() {
    return runtime.config.captureError;
  },
  set captureError(value: (error: FireleaseError) => void) {
    runtime.config.captureError = value;
  }
};

export const TESTABLES = {
  observeWriteOperation,
  resetBetweenTests,
  setSchedulerRandom: (random: () => number) => {runtime.scheduler.setRandom(random);},
  waitUntilDeleted
};

const firelease = Object.freeze({
  RETRY,
  settings,
  defaults,
  stats,
  get state() {return runtime.state;},
  attachWorker,
  start,
  limitTasksInFlight,
  waitForTasksInFlight,
  pingQueues,
  extendLease,
  blacklist,
  shutdown,
  listTasksInProgress
}) as FireleaseApi;

NodeFire.interceptOperations(observeWriteOperation, 'after');

/**
 * Attaches a worker function to consume tasks from a queue.  You should normally attach no more
 * than one worker per path in any given process, but it's OK to run multiple processes on the same
 * paths concurrently.
 *
 * All durations can be specified as either a human-readable string, or a number of milliseconds.
 *
 * @param {NodeFire | NodeFire[]} refOrRefs One or more NodeFire refs to queue roots in Firebase.
 *        Individual tasks will be children of these roots and must be objects.  All refs form one
 *        logical queue and share the same worker.  The '_lease' key is reserved for use by
 *        Firelease in each task.
 * @param {Object} options Optional options, supporting the following values:
 *        bufferSize: {number} upper bound on how many tasks to keep buffered from each source and
 *          potentially go through leasing transactions in parallel.  It defaults to `Infinity`,
 *          which is preferred for efficiency and correctness unless the queue will usually remain
 *          above `settings.safeQueueSize`.  `Infinity` adapts between a full listener and a safe
 *          listener limited to `settings.safeQueueSize` tasks.  Use a finite value only to keep an
 *          ordinarily large queue permanently on a limited listener.  An explicit finite value is
 *          used as-is and may be greater than `settings.safeQueueSize`.
 *        priority: {number} positive relative task-start priority.  Values at least 1 are
 *          foreground; values below 1 are background and run only when no foreground queue has an
 *          admissible task.  Defaults to 1.
 *        minLease: {number | string} minimum duration of each lease, which should equal the maximum
 *          expected time a worker will take to handle a task.
 *        maxLease: {number | string} maximum duration of each lease; the lease duration is doubled
 *          each time a task fails until it reaches maxLease.
 *        preprocess: {function(Object):Object} a function to use to preprocess each item during the
 *          leasing transaction.  This function must be fast, synchronous, idempotent, and should
 *          return the modified item (passed as the sole argument, OK to mutate).
 *        healthyPingLatency: {number | string} the maximum response latency to pings that is
 *          considered "healthy" for this queue.
 *        captureLeaseTransactionMetrics: {function(string, number, number)} a synchronous callback
 *          invoked after each task lease transaction with its outcome, tries, and duration.
 * @param {function(Object):RETRY | number | string | undefined} worker The worker function that
 *        handles enqueued tasks.  It receives a task with a special `$ref` attribute and can return
 *        a retry directive, retry time, complete lease object, transaction callback, or null to
 *        retire the task.  Any result may be wrapped in a promise.
 */
export function attachWorker(refOrRefs: QueueRef, worker: Worker): void;
export function attachWorker(refOrRefs: QueueRef, options: QueueOptions, worker: Worker): void;
export function attachWorker(
  refOrRefs: QueueRef,
  options: QueueOptions | Worker,
  worker?: Worker
): void {
  runtime.attach(refOrRefs, options, worker);
}

/** Starts all attached queues and resolves once every initial listener has loaded. */
export function start(): Promise<void> {
  return runtime.start();
}

/** Limits tasks in flight in the optional Firebase subtree.  A limit of zero pauses it. */
export function limitTasksInFlight(limit: number, ref?: NodeFire): ReleaseLimit {
  return runtime.scheduler.limitTasksInFlight(limit, ref);
}

/** Waits for actual tasks in flight in a scope to fall to its current effective limit. */
export function waitForTasksInFlight(timeout: Duration, ref?: NodeFire): Promise<void> {
  return runtime.scheduler.waitForTasksInFlight(timeout, ref);
}

/**
 * Sets up regular pinging of all queues.  Can be called before or after workers are attached, and
 * can be called more than once to change the callback or interval.
 *
 * @param {Function(Object) | null} callback Callback invoked with the live `stats` object after
 *        pinging all queues.  If omitted, reports are silently dropped.
 * @param {number | string} interval Interval at which queues are pinged.  Defaults to 1 minute.
 */
export function pingQueues(
  callback?: ((report: PingReport) => void) | null,
  interval?: Duration
): void {
  const normalizedInterval = interval ? runtime.duration(interval) : PING_INTERVAL;
  pingIntervalHandle?.clear();
  pingCallback = callback;
  pingIntervalHandle = timers.setInterval(() => {void runPingCheck();}, normalizedInterval);
}

async function runPingCheck() {
  try {
    await checkPings();
  } catch (error) {
    error.firelease = _.assign(error.firelease ?? {}, {phase: 'pinging'});
    error.level = 'warning';
    runtime.config.captureError(error);
    pinging = false;
  }
}

async function checkPings() {
  if (pinging) return;
  pinging = true;
  await Promise.all(_(runtime.queues.queues)
    .flatMap(queue => queue.sources)
    .filter(source => source.connected && source.loaded)
    .map(source => source.checkPing())
    .value());
  runtime.scheduler.rebuild();
  pingCallback?.(stats);
  pinging = false;
}

/**
 * Extends a task's lease to give its worker more time to finish.
 * @param {Object} item The original task object provided to a worker function.
 * @param {number | string} timeNeeded Minimum additional time needed from the current time.
 */
export function extendLease(item: WorkerItem, timeNeeded: Duration): Promise<void> {
  return extendTaskLease(runtime, item, timeNeeded);
}

/**
 * Blacklists a task key from being processed again.
 * @return {boolean} Whether the task key was newly added to the blacklist.
 */
export function blacklist(taskKey: string): boolean {
  return runtime.tasks.blacklist(taskKey);
}

/** Shuts down Firelease and resolves once all currently running tasks have completed. */
export function shutdown(): Promise<void> {
  return runtime.shutdown();
}

/** Lists the URLs of all tasks currently being worked on. */
export function listTasksInProgress(): string[] {
  return runtime.tasks.listInProgress();
}

function validateTaskStartRate(value: number, name: string) {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive finite number`);
  }
}

function observeWriteOperation(op: OperationDescriptor) {
  try {
    runtime.databases.observe(op);
  } catch (cause) {
    const error = new Error('Firelease write latency observation failed') as FireleaseError;
    error.firelease = {
      cause: (cause as Error).message,
      code: 'write-latency-observation-failed',
      phase: 'task-start-control',
      source: op.ref.toString()
    };
    console.error(error.message, error.firelease);
    try {
      runtime.config.captureError(error);
    } catch (captureCause) {
      console.error('Error capturing write latency observation error:', captureCause);
    }
  }
}

function resetBetweenTests() {
  pingIntervalHandle?.clear();
  pingIntervalHandle = undefined;
  pingCallback = undefined;
  pinging = false;
  runtime.reset();
}

export default firelease;
