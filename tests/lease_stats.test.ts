import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('lease stats capture acquisitions and contention through the hierarchy', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('lease-stats-database', 'queues/jobs');
    source.transactionTries = 3;
    source.transactionDuration = 25;
    let workerCalls = 0;
    firelease.attachWorker(asNodeFire(source), item => {
      workerCalls++;
      assert.strictEqual(item.payload, 'acquired');
    });
    const queueStats = firelease.stats.queues[firelease.stats.queues.length - 1];
    const sourceStats = queueStats.sources[0];

    const acquiredTask = source.addTask('acquired', {payload: 'acquired'});
    await waitFor(() => workerCalls === 1 && !acquiredTask.value);

    source.transactionTries = 2;
    source.transactionDuration = 10;
    source.beforeTransaction = task => {
      source.beforeTransaction = undefined;
      task.value!._lease = {expiry: source.now + 60_000};
    };
    source.addTask('contended', {payload: 'contended'});
    await waitFor(() => sourceStats.leaseTransactions.contended === 1);

    const expected = {acquired: 1, contended: 1, tries: 5, duration: 35};
    assert.strictEqual(workerCalls, 1);
    assert.deepStrictEqual(sourceStats.leaseTransactions, expected);
    assert.deepStrictEqual(queueStats.leaseTransactions, expected);
    assert.deepStrictEqual(firelease.stats.leaseTransactions, expected);
    assert.strictEqual(queueStats.tasksAcquired, 1);
    assert.strictEqual(firelease.stats.tasksAcquired, 1);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
