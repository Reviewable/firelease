import _ from 'lodash';
import ms from 'ms';

import {DatabaseSystem} from './database';
import {QueueSystem} from './queues';
import {TaskScheduler} from './scheduler';
import {FireleaseStats} from './stats';
import {TaskRegistry} from './tasks';
import type {
  Duration, FireleaseError, FireleaseState, QueueOptions, QueueRef, RetryDirective, RuntimeConfig,
  Worker
} from './types';

const defaultCaptureError = (error: FireleaseError) => {console.error(error.stack);};

export class FireleaseRuntime {
  readonly config: RuntimeConfig = {
    safeQueueSize: 6000,
    queueCheckInterval: ms('5m'),
    queueLoadTimeout: ms('1m'),
    writeLatencyTarget: ms('2s'),
    initialTaskStartRate: 5,
    maxTaskStartRate: 50,
    taskStartRateAdjustmentInterval: ms('5s'),
    taskStartRateMinSamples: 20,
    captureError: defaultCaptureError
  };

  readonly retry = {} as RetryDirective;
  readonly defaults: QueueOptions = {
    bufferSize: Infinity, priority: 1, minLease: '30s', maxLease: '1h',
    healthyPingLatency: '1.5s'
  };

  readonly stats: FireleaseStats;
  readonly tasks: TaskRegistry;
  readonly databases: DatabaseSystem;
  readonly queues: QueueSystem;
  readonly scheduler: TaskScheduler;
  startRequested = false;
  initialStartupComplete = false;
  startPromise?: Promise<void>;
  startResolve?: () => void;
  startReject?: (error: Error) => void;
  startupFailed = false;
  shuttingDown = false;
  suspendingStartupEnqueue = false;
  shutdownResolve?: () => void;
  shutdownPromise?: Promise<void>;

  constructor() {
    this.stats = new FireleaseStats(this);
    this.tasks = new TaskRegistry(this);
    this.databases = new DatabaseSystem(this);
    this.queues = new QueueSystem(this);
    this.scheduler = new TaskScheduler(this);
  }

  get state(): FireleaseState {
    if (this.shuttingDown) return 'shutting-down';
    if (!this.initialStartupComplete ||
        _.some(this.queues.sources, source => source.reconnecting)) {
      return 'initializing';
    }
    if (this.scheduler.allSourcesPaused()) return 'paused';
    return 'running';
  }

  attach(refOrRefs: QueueRef, options: QueueOptions | Worker, worker?: Worker) {
    if (this.startRequested) throw new Error('Cannot attach a worker after Firelease has started');
    this.queues.attach(refOrRefs, options, worker);
  }

  start(): Promise<void> {
    if (this.startPromise) return this.startPromise;
    this.startRequested = true;
    this.startPromise = new Promise<void>((resolve, reject) => {
      this.startResolve = resolve;
      this.startReject = reject;
    });
    this.scheduler.start();
    this.suspendingStartupEnqueue = true;
    try {
      this.databases.start();
    } catch (cause) {
      this.startupFailed = true;
      const error = cause as FireleaseError;
      for (const source of this.queues.sources) {
        if (!source.initialStartupComplete) {
          source.crash(
            'queue-startup-failed', 'Firelease queue startup failed', error,
            {description: 'connection listener'});
        }
      }
      this.startReject?.(error);
    } finally {
      this.suspendingStartupEnqueue = false;
    }
    this.queues.enqueuePendingStartups();
    this.stateChanged();
    return this.startPromise;
  }

  connectionChanged() {
    if (!this.suspendingStartupEnqueue) this.queues.enqueuePendingStartups();
    this.scheduler.wake();
  }

  stateChanged() {
    if (!this.startRequested) return;
    if (!this.initialStartupComplete && !this.startupFailed &&
        _.every(this.queues.sources, source => source.initialStartupComplete)) {
      this.initialStartupComplete = true;
      for (const group of this.databases.groups) {
        group.controlledEpoch = group.epoch;
        group.controller.reset('startup');
      }
      this.startResolve?.();
      this.scheduler.wake();
      return;
    }
    if (!this.initialStartupComplete) return;
    for (const group of this.databases.groups) {
      if (group.controlledEpoch === group.epoch || !group.connected ||
          !_.every(group.sources, source => source.loaded)) continue;
      group.controlledEpoch = group.epoch;
      group.controller.reset('reconnect');
      this.scheduler.wake();
    }
  }

  failStartup(error: Error) {
    this.startupFailed = true;
    this.startReject?.(error);
  }

  canDispatch() {
    return this.initialStartupComplete && !this.shuttingDown;
  }

  taskFinished(inFlightCount: number) {
    if (this.shuttingDown && !inFlightCount) this.shutdownResolve?.();
    if (this.shuttingDown) console.log(`Queues draining, tasks in progress: ${inFlightCount}`);
  }

  shutdown(): Promise<void> {
    if (!this.shuttingDown) {
      this.shuttingDown = true;
      this.scheduler.stopStartingTasks();
    }
    if (!this.shutdownPromise) {
      this.shutdownPromise = new Promise(resolve => {this.shutdownResolve = resolve;});
    }
    if (!this.scheduler.inFlightCount) this.shutdownResolve?.();
    return this.shutdownPromise;
  }

  duration(value: Duration) {
    if (_.isNumber(value)) return value;
    return ms(value as ms.StringValue);
  }

  reset() {
    this.queues.reset();
    this.databases.reset();
    this.scheduler.reset();
    this.tasks.reset();

    this.config.safeQueueSize = 6000;
    this.config.queueCheckInterval = ms('5m');
    this.config.queueLoadTimeout = ms('1m');
    this.config.writeLatencyTarget = ms('2s');
    this.config.initialTaskStartRate = 5;
    this.config.maxTaskStartRate = 50;
    this.config.taskStartRateAdjustmentInterval = ms('5s');
    this.config.taskStartRateMinSamples = 20;
    this.config.captureError = defaultCaptureError;
    this.startRequested = false;
    this.initialStartupComplete = false;
    this.startPromise = undefined;
    this.startResolve = undefined;
    this.startReject = undefined;
    this.startupFailed = false;
    this.shuttingDown = false;
    this.suspendingStartupEnqueue = false;
    this.shutdownResolve = undefined;
    this.shutdownPromise = undefined;
    delete this.defaults.preprocess;
    delete this.defaults.captureLeaseTransactionMetrics;
    _.assign(this.defaults, {
      bufferSize: Infinity,
      priority: 1,
      minLease: '30s',
      maxLease: '1h',
      healthyPingLatency: '1.5s'
    });
  }
}
