import NodeFire from 'nodefire';
import firelease, {
  RETRY, TESTABLES, attachWorker, blacklist, defaults, extendLease, limitTasksInFlight,
  listTasksInProgress, pingQueues, settings, shutdown, start,
  waitForTasksInFlight, type CaptureLeaseTransactionMetrics, type Duration, type FireleaseApi,
  type DatabaseStats,
  type FireleaseError, type FireleaseErrorDetails, type FireleaseErrorLevel, type FireleaseSettings,
  type FireleaseState, type FireleaseStats, type Lease, type LeaseItem,
  type LeaseTransactionStats, type PingReport, type ReleaseLimit,
  type LeaseTransactionOutcome, type QueueOptions,
  type QueueMode, type QueueRef, type QueueSourceMode, type QueueSourceStats, type QueueStats,
  type RetryDirective, type Worker, type WorkerItem, type WorkerResult
} from '../src';

// Verify the package's default and named TypeScript exports.

declare const queueRef: NodeFire;
declare const generatorWorker: () => Generator<unknown, void, unknown>;
declare const lease: Lease;
declare const leaseItem: LeaseItem;
declare const leaseTransactionStats: LeaseTransactionStats;
declare const leaseTransactionOutcome: LeaseTransactionOutcome;
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
declare const releaseLimit: ReleaseLimit;
declare const fireleaseState: FireleaseState;
void [
  lease, leaseItem, leaseTransactionStats, leaseTransactionOutcome, workerItem, fireleaseError,
  pingReport, queueOptions, duration, errorDetails, errorLevel, fireleaseSettings, fireleaseStats,
  queueStats, queueSourceStats, queueMode,
  queueSourceMode,
  queue, retry, worker, workerResult, api, releaseLimit, fireleaseState
];
const namedDefaults: QueueOptions = defaults;
const namedRetry: RetryDirective = RETRY;
const namedSettings: FireleaseSettings = settings;
const captureLeaseTransactionMetrics: CaptureLeaseTransactionMetrics = (
  outcome, tries, transactionDuration
) => {
  void [outcome, tries, transactionDuration];
};
// @ts-expect-error Lease transaction metric callbacks must be synchronous.
const asyncLeaseTransactionMetrics: CaptureLeaseTransactionMetrics = async () => undefined;
void [
  TESTABLES, attachWorker, blacklist, extendLease, limitTasksInFlight,
  listTasksInProgress, pingQueues, shutdown, start, waitForTasksInFlight,
  namedDefaults, namedRetry, namedSettings, captureLeaseTransactionMetrics,
  asyncLeaseTransactionMetrics
];
TESTABLES.resetBetweenTests();

// @ts-expect-error Generator workers are no longer supported.
firelease.attachWorker(queueRef, generatorWorker);

firelease.attachWorker(queueRef, item => {
  const payload = item.payload;
  const leaseTimeRemaining: number = item.$leaseTimeRemaining;
  const firstAcquisition: true | undefined = item._lease.firstAcquisition;
  void payload;
  void leaseTimeRemaining;
  void firstAcquisition;
  return firelease.RETRY;
});

firelease.attachWorker(
  [queueRef],
  {
    bufferSize: Infinity,
    priority: 0.5,
    minLease: '30s',
    preprocess: item => item,
    captureLeaseTransactionMetrics
  },
  async item => {await firelease.extendLease(item, '1m');}
);

// @ts-expect-error Lease delays are no longer supported.
firelease.attachWorker(queueRef, {leaseDelay: '1s'}, () => undefined);

// @ts-expect-error Adaptive lease delays are no longer supported.
firelease.attachWorker(queueRef, {maxLeaseDelay: '1s'}, () => undefined);

firelease.pingQueues(report => {
  const tasksAcquired: number = report.tasksAcquired;
  const acquired: number = report.leaseTransactions.acquired;
  const contended: number = report.leaseTransactions.contended;
  const failed: number = report.leaseTransactions.failed;
  const tries: number = report.leaseTransactions.tries;
  const transactionDuration: number = report.leaseTransactions.duration;
  const sickQueues: (string | null)[] = report.sickQueues;
  const sickSources: string[] = report.sickSources;
  const databases: DatabaseStats[] = report.databases;
  const sources: QueueSourceStats[] = report.queues.flatMap(queueResult => queueResult.sources);
  // @ts-expect-error Lease-delay telemetry was removed with the delay mechanism.
  void report.leaseDelays;
  void [
    tasksAcquired, acquired, contended, failed, tries, transactionDuration, sickQueues, sickSources,
    databases, sources
  ];
});

firelease.settings.safeQueueSize = 6000;
firelease.settings.queueCheckInterval = '5m';
firelease.settings.queueLoadTimeout = '1m';
// @ts-expect-error Queue aging was replaced by weighted priority selection.
firelease.settings.queueStarvationTimeout = '3s';
firelease.settings.writeLatencyTarget = '2s';
firelease.settings.initialTaskStartRate = 5;
firelease.settings.maxTaskStartRate = 50;
firelease.settings.taskStartRateAdjustmentInterval = '5s';
firelease.settings.taskStartRateMinSamples = 20;
firelease.settings.captureError = error => {void error.firelease;};
const state: FireleaseState = firelease.state;
const startupPromise: Promise<void> = firelease.start();
const releaseFlightLimit: ReleaseLimit = firelease.limitTasksInFlight(10);
const releaseScopedFlightLimit: ReleaseLimit = firelease.limitTasksInFlight(0, queueRef);
const waitForFlightPromise: Promise<void> = firelease.waitForTasksInFlight('15s', queueRef);
const currentStats: FireleaseStats = firelease.stats;
const sizeDelta: number | undefined = queueSourceStats.sizeDelta;
const aggregateMode: QueueMode = queueStats.mode;
const aggregateSize: number | null = queueStats.size;
const aggregateSizeDelta: number | undefined = queueStats.sizeDelta;
const aggregateSizeTimestamp: number | undefined = queueStats.sizeTimestamp;
const queuePriority: number = queueStats.priority;
const sourceAcquired: number = queueSourceStats.leaseTransactions.acquired;
const queueContended: number = queueStats.leaseTransactions.contended;
const failedLeaseTransactions: number = queueStats.leaseTransactions.failed;
// @ts-expect-error Lease transaction stats are lifetime totals and cannot be reset.
queueStats.resetLeaseTransactions();
// @ts-expect-error The old global concurrency setting was removed.
firelease.settings.globalMaxConcurrent = 10;
// @ts-expect-error Mutable settings are nested under `settings`.
firelease.captureError = () => undefined;
const shutdownPromise: Promise<void> = firelease.shutdown();
const taskUrls: string[] = firelease.listTasksInProgress();
void shutdownPromise;
void [
  taskUrls, currentStats, sizeDelta, aggregateMode, aggregateSize, aggregateSizeDelta, state,
  startupPromise, releaseFlightLimit,
  releaseScopedFlightLimit, waitForFlightPromise, aggregateSizeTimestamp, sourceAcquired,
  queueContended, failedLeaseTransactions, queuePriority
];
