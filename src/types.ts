import type NodeFire from 'nodefire';

import type {FireleaseStats, QueueSourceMode} from './stats';

declare const RETRY_DIRECTIVE: unique symbol;

export type Duration = number | string;
export type FireleaseState = 'initializing' | 'running' | 'paused' | 'shutting-down';
export type ReleaseLimit = () => void;

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
  limit?: number;
  liveCount?: number;
  mode?: QueueSourceMode;
  phase?: string;
  queue?: string;
  reason?: string;
  retryDelay?: Duration;
  source?: string;
  scope?: string;
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
  bufferSize?: number;
  priority?: number;
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
  safeQueueSize: number;
  queueCheckInterval: Duration;
  queueLoadTimeout: Duration;
  writeLatencyTarget: Duration;
  initialTaskStartRate: number;
  maxTaskStartRate: number;
  taskStartRateAdjustmentInterval: Duration;
  taskStartRateMinSamples: number;
  captureError: (error: FireleaseError) => void;
}

export interface FireleaseApi {
  readonly RETRY: RetryDirective;
  readonly settings: FireleaseSettings;
  readonly defaults: QueueOptions;
  readonly stats: FireleaseStats;
  readonly state: FireleaseState;
  attachWorker: {
    (refOrRefs: QueueRef, worker: Worker): void;
    (refOrRefs: QueueRef, options: QueueOptions, worker: Worker): void;
  };
  start(): Promise<void>;
  limitTasksInFlight(limit: number, ref?: NodeFire): ReleaseLimit;
  waitForTasksInFlight(timeout: Duration, ref?: NodeFire): Promise<void>;
  pingQueues(callback?: ((report: PingReport) => void) | null, interval?: Duration): void;
  extendLease(item: WorkerItem, timeNeeded: Duration): Promise<void>;
  blacklist(taskKey: string): boolean;
  shutdown(): Promise<void>;
  listTasksInProgress(): string[];
}

export interface LeaseSnapshot {
  ref: NodeFire;
  val(): unknown;
}

export interface NormalizedQueueOptions {
  bufferSize: number;
  priority: number;
  minLease: number;
  maxLease: number;
  healthyPingLatency: number;
  preprocess?: (item: LeaseItem) => LeaseItem;
  captureLeaseTransactionMetrics?: CaptureLeaseTransactionMetrics;
}

export interface RuntimeConfig {
  safeQueueSize: number;
  queueCheckInterval: number;
  queueLoadTimeout: number;
  writeLatencyTarget: number;
  initialTaskStartRate: number;
  maxTaskStartRate: number;
  taskStartRateAdjustmentInterval: number;
  taskStartRateMinSamples: number;
  captureError: (error: FireleaseError) => void;
}
