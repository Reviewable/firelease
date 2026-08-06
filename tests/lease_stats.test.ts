import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES, type LeaseTransactionOutcome} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('lease stats capture all transaction outcomes through the hierarchy', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('lease-stats-database', 'queues/jobs');
    source.transactionTries = 3;
    source.transactionPrefetchDuration = 5;
    source.transactionDuration = 25;
    const capturedMetrics: [LeaseTransactionOutcome, number, number][] = [];
    const capturedErrors: Error[] = [];
    firelease.settings.captureError = error => {capturedErrors.push(error);};
    let workerCalls = 0;
    firelease.attachWorker(
      asNodeFire(source),
      {
        captureLeaseTransactionMetrics: (outcome, tries, duration) => {
          capturedMetrics.push([outcome, tries, duration]);
          if (outcome === 'acquired') throw new Error('Metric capture failed');
        }
      },
      item => {
        workerCalls++;
        assert.strictEqual(item.payload, 'acquired');
      }
    );
    const queueStats = firelease.stats.queues[firelease.stats.queues.length - 1];
    const sourceStats = queueStats.sources[0];

    const acquiredTask = source.addTask('acquired', {payload: 'acquired'});
    await waitFor(() => workerCalls === 1 && !acquiredTask.value);

    source.transactionTries = 2;
    source.transactionPrefetchDuration = undefined;
    source.transactionDuration = 10;
    source.beforeTransaction = task => {
      source.beforeTransaction = undefined;
      task.value!._lease = {expiry: source.now + 60_000};
    };
    source.addTask('contended', {payload: 'contended'});
    await waitFor(() => sourceStats.leaseTransactions.contended === 1);

    source.transactionTries = 4;
    source.transactionDuration = 50;
    source.transactionError = new Error('Lease transaction failed');
    source.addTask('failed', {payload: 'failed'});
    await waitFor(() => capturedMetrics.length === 3);

    const expectedCounts = {acquired: 1, contended: 1, failed: 1, tries: 9};
    assert.strictEqual(workerCalls, 1);
    assert.deepStrictEqual(capturedMetrics, [
      ['acquired', 3, 30],
      ['contended', 2, 10],
      ['failed', 4, 50]
    ]);
    assert.deepStrictEqual(
      capturedErrors.map(error => error.message),
      ['Metric capture failed', 'Lease transaction failed'],
    );
    for (const stats of [
      sourceStats.leaseTransactions, queueStats.leaseTransactions, firelease.stats.leaseTransactions
    ]) {
      const {duration, ...counts} = stats;
      assert.deepStrictEqual(counts, expectedCounts);
      assert.ok(Math.abs(duration - 30.2) < Number.EPSILON * 30.2);
    }
    assert.strictEqual(queueStats.tasksAcquired, 1);
    assert.strictEqual(firelease.stats.tasksAcquired, 1);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('metric recording errors cannot interrupt task processing', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('metric-error-database', 'queues/jobs');
    let captureErrorCalls = 0;
    firelease.settings.captureError = () => {
      captureErrorCalls++;
      throw new Error('Error capture failed');
    };
    let workerCalls = 0;
    firelease.attachWorker(
      asNodeFire(source),
      {captureLeaseTransactionMetrics: () => {throw new Error('Metric capture failed');}},
      () => {workerCalls++;},
    );
    const sourceStats = firelease.stats.queues[firelease.stats.queues.length - 1].sources[0];

    const task = source.addTask('acquired', {payload: 'acquired'});
    await waitFor(() => workerCalls === 1 && !task.value);

    assert.strictEqual(captureErrorCalls, 1);
    assert.deepStrictEqual(sourceStats.leaseTransactions, {
      acquired: 1, contended: 0, failed: 0, tries: 1, duration: 0
    });
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('missing transaction metadata only records the outcome', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('missing-metadata-database', 'queues/jobs');
    source.omitTransactionMetadata = true;
    let metricCalls = 0;
    let workerCalls = 0;
    firelease.attachWorker(
      asNodeFire(source),
      {captureLeaseTransactionMetrics: () => {metricCalls++;}},
      () => {workerCalls++;},
    );
    const sourceStats = firelease.stats.queues[firelease.stats.queues.length - 1].sources[0];

    const task = source.addTask('acquired', {payload: 'acquired'});
    await waitFor(() => workerCalls === 1 && !task.value);

    assert.strictEqual(metricCalls, 0);
    assert.deepStrictEqual(sourceStats.leaseTransactions, {
      acquired: 1, contended: 0, failed: 0, tries: 0, duration: 0
    });
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
