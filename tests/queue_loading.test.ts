import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES, type FireleaseError} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('queue load timeouts fall back from full mode and fail in safe mode', async () => {
  TESTABLES.resetBetweenTests();
  const capturedErrors: FireleaseError[] = [];
  try {
    assert.throws(
      () => {firelease.settings.queueLoadTimeout = 0;},
      /queueLoadTimeout must be a positive finite duration/);
    firelease.settings.safeQueueSize = 10;
    firelease.settings.queueCheckInterval = '30d';
    firelease.settings.queueLoadTimeout = 20;
    firelease.settings.globalMaxConcurrent = 0;
    firelease.settings.captureError = error => {capturedErrors.push(error);};

    const fullSource = new FakeQueueRef('full-timeout-database', 'queues/full-timeout');
    fullSource.deferNextValue = true;
    firelease.attachWorker(asNodeFire(fullSource), {bufferSize: Infinity}, () => undefined);
    const fullStats = firelease.stats.queues[firelease.stats.queues.length - 1].sources[0];
    await waitFor(() => fullStats.mode === 'safe' && fullSource.queries.length === 1);

    const fullTimeout = capturedErrors.find(error =>
      error.firelease?.code === 'queue-load-timeout' &&
      error.firelease.source === fullSource.toString())!;
    assert.strictEqual(fullTimeout.message, 'Firelease queue load timed out');
    assert.strictEqual(fullTimeout.level, 'warning');
    assert.strictEqual(fullTimeout.firelease?.description, 'startup');
    assert.strictEqual(fullTimeout.firelease?.mode, 'full');
    assert.strictEqual(fullTimeout.firelease?.timeout, 20);
    assert(Object.values(fullSource.listeners).every(listeners => listeners.length === 0));

    const safeSource = new FakeQueueRef('safe-timeout-database', 'queues/safe-timeout');
    safeSource.childrenKeysCountOverride = 10;
    safeSource.deferNextValue = true;
    firelease.attachWorker(asNodeFire(safeSource), {bufferSize: Infinity}, () => undefined);
    await waitFor(() => capturedErrors.some(error =>
      error.level === 'fatal' && error.firelease?.code === 'queue-load-timeout' &&
      error.firelease.source === safeSource.toString()));

    const safeTimeout = capturedErrors.find(error =>
      error.level === 'fatal' && error.firelease?.code === 'queue-load-timeout' &&
      error.firelease.source === safeSource.toString())!;
    assert.strictEqual(safeTimeout.message, 'Firelease queue load timed out');
    assert.strictEqual(safeTimeout.firelease?.description, 'startup');
    assert.strictEqual(safeTimeout.firelease?.mode, 'safe');
    assert.strictEqual(safeTimeout.firelease?.timeout, 20);
    assert(Object.values(safeSource.queries[0].listeners)
      .every(listeners => listeners.length === 0));
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
