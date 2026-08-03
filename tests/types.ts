import NodeFire from 'nodefire';
import firelease, {
  RETRY, attachWorker, blacklist, captureError, defaults, extendLease, globalMaxConcurrent,
  listTasksInProgress, pingQueues, shutdown, type Duration, type FireleaseApi,
  type FireleaseError, type FireleaseErrorDetails, type FireleaseErrorLevel, type Lease,
  type LeaseItem, type PingReport, type QueueOptions, type QueueRef, type RetryDirective,
  type Worker, type WorkerItem, type WorkerResult
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
declare const queue: QueueRef;
declare const retry: RetryDirective;
declare const worker: Worker;
declare const workerResult: WorkerResult;
declare const api: FireleaseApi;
void [
  lease, leaseItem, workerItem, fireleaseError, pingReport, queueOptions, duration, errorDetails,
  errorLevel, queue, retry, worker, workerResult, api
];
const namedGlobalMaxConcurrent: number = globalMaxConcurrent;
const namedCaptureError: (error: FireleaseError) => void = captureError;
const namedDefaults: QueueOptions = defaults;
const namedRetry: RetryDirective = RETRY;
void [
  attachWorker, blacklist, extendLease, listTasksInProgress, pingQueues, shutdown,
  namedGlobalMaxConcurrent, namedCaptureError, namedDefaults, namedRetry
];

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
  // @ts-expect-error Lease-delay telemetry was removed with the delay mechanism.
  void report.leaseDelays;
  void tasksAcquired;
});

firelease.globalMaxConcurrent = 10;
firelease.captureError = error => {void error.firelease;};
const shutdownPromise: Promise<void> = firelease.shutdown();
const taskUrls: string[] = firelease.listTasksInProgress();
void shutdownPromise;
void taskUrls;
