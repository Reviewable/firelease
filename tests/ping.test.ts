import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES, type FireleaseError} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('ping transaction failures are reported', async () => {
  TESTABLES.resetBetweenTests();
  const capturedErrors: FireleaseError[] = [];
  try {
    firelease.settings.captureError = error => {capturedErrors.push(error);};
    const source = new FakeQueueRef('failed-ping-database', 'queues/failed');
    firelease.attachWorker(asNodeFire(source), {bufferSize: 1}, () => undefined);
    await waitFor(() => source.queries.length === 1);

    source.transactionError = new Error('Ping write denied');
    firelease.pingQueues(undefined, 50);
    await waitFor(() => capturedErrors.some(
      error => error.message === 'Ping write denied' && error.firelease?.phase === 'pinging'));
    firelease.pingQueues(undefined, '30d');

    const pingError = capturedErrors.find(error => error.message === 'Ping write denied')!;
    assert.strictEqual(pingError.level, 'warning');
    assert.strictEqual(pingError.firelease?.phase, 'pinging');
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
