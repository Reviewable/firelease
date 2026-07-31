import NodeFire from 'nodefire';

// Verify the package's CommonJS-facing TypeScript API.
// eslint-disable-next-line @typescript-eslint/no-require-imports
import firelease = require('../src');

declare const queueRef: NodeFire;
declare const generatorWorker: () => Generator<unknown, void, unknown>;

// @ts-expect-error Generator workers are no longer supported.
firelease.attachWorker(queueRef, generatorWorker);

firelease.attachWorker(queueRef, item => {
  const payload = item.payload;
  const leaseTimeRemaining: number = item.$leaseTimeRemaining;
  const transaction: unknown = item.$leaseTransaction;
  void payload;
  void leaseTimeRemaining;
  void transaction;
  return firelease.RETRY;
});

firelease.attachWorker(
  [queueRef],
  {bufferSize: Infinity, minLease: '30s', preprocess: item => item},
  async item => {await firelease.extendLease(item, '1m');}
);

firelease.globalMaxConcurrent = 10;
firelease.captureError = error => {void error.firelease;};
const shutdownPromise: Promise<void> = firelease.shutdown();
const taskUrls: string[] = firelease.listTasksInProgress();
void shutdownPromise;
void taskUrls;
