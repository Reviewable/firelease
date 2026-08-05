import NodeFire from 'nodefire';
import firelease, {
  RETRY, TESTABLES, attachWorker, blacklist, defaults, extendLease, listTasksInProgress, pingQueues,
  settings, shutdown, type Duration, type FireleaseApi, type FireleaseError,
  type FireleaseErrorDetails, type FireleaseErrorLevel, type FireleaseSettings,
  type FireleaseStats, type Lease, type LeaseItem, type PingReport, type QueueOptions,
  type QueueMode, type QueueRef, type QueueSourceMode, type QueueSourceStats, type QueueStats,
  type RetryDirective, type Worker, type WorkerItem, type WorkerResult
} from '../src';

// Verify the package's default and named TypeScript exports.

declare const queueRef: NodeFire;
declare const generatorWorker: () => Generator<unknown, void, unknown>;
declare const lease: Lease;
declare const leaseItem: LeaseItem;
declare const workerItem: WorkerItem;
declare const fireleaseError: FireleaseError;
declare const pingReport: PingReport;
declare const queueOptions: QueueOptions;
declare const duration: Duration;
declare const errorDetails: FireleaseErrorDetails;
declare const errorLevel: FireleaseErrorLevel;
declare const fireleaseSettings: FireleaseSettings;
declare const fireleaseStats: FireleaseStats;
declare const queueStats: QueueStats;
declare const queueSourceStats: QueueSourceStats;
declare const queueMode: QueueMode;
declare const queueSourceMode: QueueSourceMode;
declare const queue: QueueRef;
declare const retry: RetryDirective;
declare const worker: Worker;
declare const workerResult: WorkerResult;
declare const api: FireleaseApi;
void [
  lease, leaseItem, workerItem, fireleaseError, pingReport, queueOptions, duration, errorDetails,
  errorLevel, fireleaseSettings, fireleaseStats, queueStats, queueSourceStats, queueMode,
  queueSourceMode,
  queue, retry, worker, workerResult, api
];
const namedDefaults: QueueOptions = defaults;
const namedRetry: RetryDirective = RETRY;
const namedSettings: FireleaseSettings = settings;
void [
  TESTABLES, attachWorker, blacklist, extendLease, listTasksInProgress, pingQueues, shutdown,
  namedDefaults, namedRetry, namedSettings
];
TESTABLES.resetBetweenTests();

// @ts-expect-error Generator workers are no longer supported.
firelease.attachWorker(queueRef, generatorWorker);

firelease.attachWorker(queueRef, item => {
  const payload = item.payload;
  const leaseTimeRemaining: number = item.$leaseTimeRemaining;
  void payload;
  void leaseTimeRemaining;
  return firelease.RETRY;
});

firelease.attachWorker(
  [queueRef],
  {bufferSize: Infinity, minLease: '30s', preprocess: item => item},
  async item => {await firelease.extendLease(item, '1m');}
);

// @ts-expect-error Lease delays are no longer supported.
firelease.attachWorker(queueRef, {leaseDelay: '1s'}, () => undefined);

// @ts-expect-error Adaptive lease delays are no longer supported.
firelease.attachWorker(queueRef, {maxLeaseDelay: '1s'}, () => undefined);

firelease.pingQueues(report => {
  const tasksAcquired: number = report.tasksAcquired;
  const sickQueues: (string | null)[] = report.sickQueues;
  const sickSources: string[] = report.sickSources;
  const sources: QueueSourceStats[] = report.queues.flatMap(queueResult => queueResult.sources);
  // @ts-expect-error Lease-delay telemetry was removed with the delay mechanism.
  void report.leaseDelays;
  void [tasksAcquired, sickQueues, sickSources, sources];
});

firelease.settings.globalMaxConcurrent = 10;
firelease.settings.safeQueueSize = 6000;
firelease.settings.queueCheckInterval = '5m';
firelease.settings.queueLoadTimeout = '1m';
firelease.settings.captureError = error => {void error.firelease;};
const currentStats: FireleaseStats = firelease.stats;
const sizeDelta: number | undefined = queueSourceStats.sizeDelta;
const aggregateMode: QueueMode = queueStats.mode;
const aggregateSize: number | null = queueStats.size;
const aggregateSizeDelta: number | undefined = queueStats.sizeDelta;
const aggregateSizeTimestamp: number | undefined = queueStats.sizeTimestamp;
// @ts-expect-error Mutable settings are nested under `settings`.
firelease.globalMaxConcurrent = 10;
// @ts-expect-error Mutable settings are nested under `settings`.
firelease.captureError = () => undefined;
const shutdownPromise: Promise<void> = firelease.shutdown();
const taskUrls: string[] = firelease.listTasksInProgress();
void shutdownPromise;
void [
  taskUrls, currentStats, sizeDelta, aggregateMode, aggregateSize, aggregateSizeDelta,
  aggregateSizeTimestamp
];
