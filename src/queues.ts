import _ from 'lodash';
import ms from 'ms';
import NodeFire from 'nodefire';
import * as timers from 'safe-timers';

import type {ConnectionGroup} from './database';
import type {FireleaseRuntime} from './runtime';
import {QueueSourceStats, QueueStats, type QueueSourceMode} from './stats';
import {PING_KEY, Task, type TaskQueue, type TaskSource} from './tasks';
import type {
  FireleaseError, FireleaseErrorDetails, FireleaseErrorLevel, LeaseItem, LeaseSnapshot,
  NormalizedQueueOptions, QueueOptions, QueueRef, Worker, WorkerItem
} from './types';

const QUEUE_CHECK_TIMEOUT = ms('15s');
const QUEUE_SIZE_HYSTERESIS = 0.15;
const QUEUE_SIZE_MISMATCH_THRESHOLD = 100;
const DEMOTION_JITTER = ms('30s');

type QueueLoadResult = 'loaded' | 'stopped' | 'timed-out';

interface QueueCheckJob {
  source: QueueSource;
  epoch: number;
  description: string;
  run: () => Promise<void>;
  resolve: () => void;
}

class QueueCheckQueue {
  jobs: QueueCheckJob[] = [];
  active?: QueueCheckJob;
  draining = false;

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
        this.active = job;
        try {
          await job.run();
        } catch (error) {
          job.source.crash(
            'queue-check-failed', 'Firelease queue check failed', error,
            {description: job.description});
        } finally {
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

export class QueueListener {
  readonly query: NodeFire;
  readonly changeEvent: 'child_changed' | 'child_moved';
  readonly snapshots = new Map<string, LeaseSnapshot>();
  loaded = false;
  activated = false;
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
    if (this.activated) this.source.addTask(snap);
    this.notify();
  };

  readonly onRemove = (snap: LeaseSnapshot) => {
    this.snapshots.delete(Task.makeKey(snap));
    if (this.activated) this.source.removeTask(snap);
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

  activate() {
    if (this.activated || this.stopped || !this.loaded) return;
    this.activated = true;
    for (const snap of this.snapshots.values()) this.source.addTask(snap, false);
    this.source.system.runtime.scheduler.wake();
  }

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

export class QueueSource implements TaskSource {
  mode: QueueSourceMode = 'safe';
  epoch = 0;
  connected = false;
  loaded = false;
  reconnecting = true;
  crashing = false;
  connectionStartTime = 0;
  pendingStartupEpoch?: number;
  enqueuedStartupEpoch?: number;
  activeListener?: QueueListener;
  checkTimer?: timers.Timeout;
  demotionTimer?: timers.Timeout;
  exitTimer?: timers.Timeout;
  promotionNotBeforeTimestamp = 0;
  initialStartupComplete = false;
  readonly readyTasks: {task: Task, version: number}[] = [];
  readyTaskIndex = 0;
  readonly stats: QueueSourceStats;

  constructor(
    readonly system: QueueSystem,
    readonly queue: Queue,
    readonly ref: NodeFire,
    readonly group: ConnectionGroup
  ) {
    this.stats = new QueueSourceStats(ref.toString());
    this.group.sources.push(this);
    system.sources.push(this);
  }

  get adaptive() {
    return this.queue.options.bufferSize === Infinity;
  }

  setConnected(connected: boolean, epoch: number, connectionStartTime: number) {
    this.connected = connected;
    this.stats.connected = connected;
    this.epoch = epoch;
    this.cancelPendingWork();
    if (connected) {
      this.reconnecting = true;
      this.stats.healthy = true;
      this.connectionStartTime = connectionStartTime;
      this.pendingStartupEpoch = epoch;
    } else {
      this.reconnecting = true;
      this.loaded = false;
      this.pendingStartupEpoch = undefined;
      this.activeListener?.stop();
      this.activeListener = undefined;
      this.clearTasks();
      if (this.stats.size !== null && this.stats.sizeTimestamp === undefined) {
        this.stats.sizeTimestamp = Date.now();
      }
      this.stats.healthy = false;
    }
    this.system.runtime.stateChanged();
  }

  enqueuePendingStartup() {
    const epoch = this.pendingStartupEpoch;
    if (epoch === undefined || this.enqueuedStartupEpoch === epoch ||
        !this.isCurrent(epoch)) return;
    this.enqueuedStartupEpoch = epoch;
    void this.enqueueStartup(epoch);
  }

  reset() {
    this.connected = false;
    this.loaded = false;
    this.reconnecting = true;
    this.epoch++;
    this.pendingStartupEpoch = undefined;
    this.enqueuedStartupEpoch = undefined;
    this.cancelPendingWork();
    this.exitTimer?.clear();
    this.exitTimer = undefined;
    this.activeListener?.stop();
    this.activeListener = undefined;
    this.clearTasks();
  }

  async enqueueStartup(epoch: number) {
    await this.system.checkQueue.enqueue(this, 'startup', epoch, () => this.initialize(epoch));
  }

  async initialize(epoch: number) {
    try {
      let targetMode: QueueSourceMode = 'safe';
      if (this.adaptive) {
        const count = await this.probeSize('startup');
        if (!this.isCurrent(epoch)) return;
        targetMode = count !== null && count < this.system.fullQueueSize() ? 'full' : 'safe';
      }
      const loadedMode = await this.loadListener(targetMode, epoch, 'startup');
      if (!loadedMode) return;
      const loadedTaskCount = this.activeListener?.snapshots.size ?? 0;
      const connectionDuration = Math.round(performance.now() - this.connectionStartTime);
      console.log(
        `Queue worker ${this.ref} loaded ${loadedTaskCount} tasks in ${loadedMode} mode` +
        ` (${ms(connectionDuration)})`);
      if (loadedMode === 'safe') this.scheduleSafeCheck();
      this.loaded = true;
      this.reconnecting = false;
      this.initialStartupComplete = true;
      this.pendingStartupEpoch = undefined;
      this.system.runtime.stateChanged();
    } catch (error) {
      this.crash(
        'queue-startup-failed', 'Firelease queue startup failed', error, {description: 'startup'});
    }
  }

  isCurrent(epoch: number) {
    return !this.crashing && this.connected && this.epoch === epoch;
  }

  listenerLimit(mode: QueueSourceMode) {
    if (mode === 'full') return Infinity;
    const limit = this.adaptive ?
      this.system.runtime.config.safeQueueSize : this.queue.options.bufferSize;
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
    const interval = this.system.runtime.config.queueCheckInterval;
    const jitteredInterval = Math.max(0, Math.round(interval * (0.95 + Math.random() * 0.1)));
    const epoch = this.epoch;
    this.checkTimer = timers.setTimeout(() => {
      this.checkTimer = undefined;
      void this.system.checkQueue.enqueue(
        this, 'scheduled shallow count', epoch, () => this.runScheduledCheck(epoch)
      ).finally(() => {
        if (this.isCurrent(epoch) && this.mode === 'safe') this.scheduleSafeCheck();
      });
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
    if (count < this.system.fullQueueSize() &&
        performance.now() >= this.promotionNotBeforeTimestamp) {
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
      void this.system.checkQueue.enqueue(this, 'live-count demotion', epoch, () =>
        this.demote(epoch));
    }, delay);
  }

  async demote(epoch: number) {
    if (!this.isCurrent(epoch) || this.mode !== 'full' ||
        (this.activeListener?.snapshots.size ?? 0) <=
        this.system.runtime.config.safeQueueSize) return;
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
    const timeout = this.system.runtime.config.queueLoadTimeout;
    let details = {description, mode, timeout};
    const result = await this.replaceListener(mode, epoch);
    if (result === 'loaded') {
      if (mode === 'full') this.promotionNotBeforeTimestamp = 0;
      return mode;
    }
    if (result === 'stopped') return;

    if (mode === 'full') {
      this.reportError(
        'queue-load-timeout', 'Firelease queue load timed out', 'warning', details);
      this.promotionNotBeforeTimestamp =
        performance.now() + this.system.runtime.config.queueCheckInterval * 3;
      const fallbackResult = await this.replaceListener('safe', epoch);
      if (fallbackResult === 'loaded') return 'safe';
      if (fallbackResult === 'stopped') return;
      details = {description: `${description} fallback`, mode: 'safe', timeout};
    }

    const error = new Error('timeout') as FireleaseError;
    this.crash('queue-load-timeout', 'Firelease queue load timed out', error, details);
    throw error;
  }

  async replaceListener(mode: QueueSourceMode, epoch: number): Promise<QueueLoadResult> {
    if (!this.isCurrent(epoch)) return 'stopped';
    this.loaded = false;
    this.system.runtime.stateChanged();
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
    const result = await listener.waitForLoad(this.system.runtime.config.queueLoadTimeout);
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
    this.loaded = true;
    listener.activate();
    listener.notify();
    return 'loaded';
  }

  readonly onListenerSize = (listener: QueueListener) => {
    if (listener !== this.activeListener || !listener.loaded) return;
    const size = listener.snapshots.size;
    this.stats.size = size;
    delete this.stats.sizeDelta;
    delete this.stats.sizeTimestamp;
    if (size > this.system.runtime.config.safeQueueSize) {
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
    const startTime = performance.now();
    const timestamp = Date.now();
    const pingRef = this.ref.child(PING_KEY);
    let pingFree = false;
    try {
      await pingRef.transaction(item => {
        pingFree = !item;
        return item ?? {timestamp, _lease: {expiry: NodeFire.SERVER_TIMESTAMP}};
      }, {prefetchValue: false, timeout: ms('10s')});
    } catch (error) {
      this.recordPingResult(startTime, false);
      throw error;
    }
    if (!pingFree) return;
    try {
      await waitUntilDeleted(pingRef, this.queue.options.healthyPingLatency + ms('10s'));
    } catch {
      this.recordPingResult(startTime, false);
      return;
    }
    this.recordPingResult(startTime, true);
  }

  recordPingResult(startTime: number, succeeded: boolean) {
    const latency = Math.round(performance.now() - startTime);
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
    this.system.runtime.config.captureError(error);
  }

  crash(
    code: string, message: string, cause: FireleaseError, details: FireleaseErrorDetails = {}
  ) {
    if (this.crashing) return;
    this.crashing = true;
    this.loaded = false;
    this.reconnecting = true;
    this.cancelPendingWork();
    const error = new Error(message) as FireleaseError;
    error.level = 'fatal';
    error.firelease = {
      ...details, cause: cause.message, code, phase: 'crashing',
      queue: this.queue.ref.toString(), source: this.ref.toString()
    };
    console.error(message, error.firelease);
    this.system.runtime.config.captureError(error);
    this.activeListener?.stop();
    this.activeListener = undefined;
    this.clearTasks();
    if (!this.initialStartupComplete) this.system.runtime.failStartup(cause);
    this.exitTimer = timers.setTimeout(() => {process.exit(1);}, ms('3s'));
  }

  addTask(snap: LeaseSnapshot, wake = true, schedule = true) {
    const taskKey = Task.makeKey(snap);
    let task = this.system.runtime.tasks.get(taskKey);
    if (this.system.runtime.tasks.blacklistedKeys.has(taskKey)) {
      if (task) this.removeTask(taskKey);
      return;
    }
    if (task) task.updateFrom(snap);
    else task = this.system.runtime.tasks.create(this, snap);
    if (schedule) this.system.runtime.scheduler.schedule(task, wake);
  }

  removeTask(snapOrKey: LeaseSnapshot | string) {
    const taskKey = _.isString(snapOrKey) ? snapOrKey : Task.makeKey(snapOrKey);
    const task = this.system.runtime.tasks.get(taskKey);
    if (task?.source !== this) return;
    task.removed = true;
    task.ready = false;
    task.readyVersion++;
    if (task.timeout) {
      task.timeout.clear();
      delete task.timeout;
    }
    if (!task.working) this.system.runtime.tasks.delete(taskKey);
    this.system.runtime.scheduler.wake();
  }

  clearTasks() {
    this.system.runtime.tasks.each((task, taskKey) => {
      if (task.source === this) this.removeTask(taskKey);
    });
  }

  enqueueReady(task: Task) {
    task.ready = true;
    task.readyVersion++;
    this.readyTasks.push({task, version: task.readyVersion});
  }

  takeReadyTask() {
    while (this.readyTaskIndex < this.readyTasks.length) {
      const entry = this.readyTasks[this.readyTaskIndex++];
      if (entry.version !== entry.task.readyVersion || !entry.task.ready ||
          entry.task.source !== this || entry.task.removed || entry.task.working ||
          this.system.runtime.tasks.get(entry.task.key) !== entry.task) continue;
      entry.task.ready = false;
      if (this.readyTaskIndex > 1000 && this.readyTaskIndex * 2 > this.readyTasks.length) {
        this.readyTasks.splice(0, this.readyTaskIndex);
        this.readyTaskIndex = 0;
      }
      return entry.task;
    }
    if (this.readyTaskIndex) {
      this.readyTasks.length = 0;
      this.readyTaskIndex = 0;
    }
  }

  hasReadyTask() {
    while (this.readyTaskIndex < this.readyTasks.length) {
      const entry = this.readyTasks[this.readyTaskIndex];
      if (entry.version === entry.task.readyVersion && entry.task.ready &&
          entry.task.source === this && !entry.task.removed && !entry.task.working &&
          this.system.runtime.tasks.get(entry.task.key) === entry.task) return true;
      this.readyTaskIndex++;
    }
    if (this.readyTaskIndex) {
      this.readyTasks.length = 0;
      this.readyTaskIndex = 0;
    }
    return false;
  }
}

export class Queue implements TaskQueue {
  refs: NodeFire[];
  ref: NodeFire;
  options: NormalizedQueueOptions;
  worker: Worker;
  sources: QueueSource[];
  nextSourceIndex = 0;
  readonly stats: QueueStats;

  constructor(
    readonly system: QueueSystem,
    refOrRefs: QueueRef,
    options: QueueOptions | Worker,
    worker?: Worker
  ) {
    if (_.isFunction(options)) {
      worker = options;
      options = {};
    }
    const refs = _(refOrRefs).castArray().uniqBy(item => item.toString()).value() as NodeFire[];
    if (!refs.length) throw new Error('At least one queue ref is required');
    this.refs = refs;
    this.ref = refs[0];
    const filledOptions =
      _.defaults({}, options, system.runtime.defaults) as Required<QueueOptions>;
    if (!Number.isFinite(filledOptions.priority) || filledOptions.priority <= 0) {
      throw new Error('Queue priority must be a positive finite number');
    }
    filledOptions.minLease = system.runtime.duration(filledOptions.minLease);
    filledOptions.maxLease = system.runtime.duration(filledOptions.maxLease);
    filledOptions.healthyPingLatency = system.runtime.duration(filledOptions.healthyPingLatency);
    this.options = filledOptions as NormalizedQueueOptions;
    this.worker = worker as Worker;
    this.sources = _.map(this.refs, sourceRef =>
      new QueueSource(system, this, sourceRef, system.runtime.databases.getGroup(sourceRef)));
    this.stats = new QueueStats(
      this.ref.toString(), this.ref.key, filledOptions.priority, _.map(this.sources, 'stats'));
    system.runtime.stats.queues.push(this.stats);
  }

  reset() {
    _.forEach(this.sources, source => {source.reset();});
  }

  hasReadyTask() {
    return _.some(
      this.sources, source => source.connected && source.loaded && source.hasReadyTask());
  }

  constrainLeaseDuration(time: number) {
    return Math.min(this.options.maxLease, Math.max(time, this.options.minLease));
  }

  takeReadyTask(canUseSource: (source: QueueSource) => boolean) {
    for (let offset = 0; offset < this.sources.length; offset++) {
      const index = (this.nextSourceIndex + offset) % this.sources.length;
      const source = this.sources[index];
      if (!source.connected || !source.loaded || !canUseSource(source)) continue;
      const task = source.takeReadyTask();
      if (!task) continue;
      this.nextSourceIndex = (index + 1) % this.sources.length;
      return task;
    }
  }

  callPreprocess(item: LeaseItem) {
    return this.options.preprocess ? this.options.preprocess(item) : item;
  }

  async callWorker(item: WorkerItem) {
    return this.worker(item);
  }
}

export class QueueSystem {
  readonly queues: Queue[] = [];
  readonly sources: QueueSource[] = [];
  readonly checkQueue = new QueueCheckQueue();

  constructor(readonly runtime: FireleaseRuntime) {}

  attach(refOrRefs: QueueRef, options: QueueOptions | Worker, worker?: Worker) {
    const queue = new Queue(this, refOrRefs, options, worker);
    this.queues.push(queue);
  }

  enqueuePendingStartups() {
    for (const source of this.sources) source.enqueuePendingStartup();
  }

  fullQueueSize() {
    return this.runtime.config.safeQueueSize * (1 - QUEUE_SIZE_HYSTERESIS);
  }

  reset() {
    for (const queue of this.queues) queue.reset();
    this.checkQueue.reset();
    this.queues.length = 0;
    this.sources.length = 0;
    this.runtime.stats.queues.length = 0;
  }
}

export function waitUntilDeleted(ref: NodeFire, timeout: number) {
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
