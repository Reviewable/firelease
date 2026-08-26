import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES, type FireleaseError} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('a full-mode load timeout falls back to safe mode', async () => {
  TESTABLES.resetBetweenTests();
  const capturedErrors: FireleaseError[] = [];
  try {
    assert.throws(
      () => {firelease.settings.queueLoadTimeout = 0;},
      /queueLoadTimeout must be a positive finite duration/);
    firelease.settings.safeQueueSize = 10;
    firelease.settings.queueCheckInterval = '30d';
    firelease.settings.queueLoadTimeout = 20;
    firelease.settings.captureError = error => {capturedErrors.push(error);};
    firelease.limitTasksInFlight(0);

    const source = new FakeQueueRef('full-timeout-database', 'queues/full-timeout');
    source.deferNextValue = true;
    firelease.attachWorker(asNodeFire(source), {bufferSize: Infinity}, () => undefined);
    await firelease.start();
    const sourceStats = firelease.stats.queues[0].sources[0];

    assert.strictEqual(sourceStats.mode, 'safe');
    assert.strictEqual(source.queries.length, 1);
    const timeout = capturedErrors.find(error => error.firelease?.code === 'queue-load-timeout')!;
    assert.strictEqual(timeout.message, 'Firelease queue load timed out');
    assert.strictEqual(timeout.level, 'warning');
    assert.strictEqual(timeout.firelease?.description, 'startup');
    assert.strictEqual(timeout.firelease?.mode, 'full');
    assert.strictEqual(timeout.firelease?.timeout, 20);
    assert(Object.values(source.listeners).every(listeners => listeners.length === 0));
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('a safe-mode load timeout rejects startup and crashes', async () => {
  TESTABLES.resetBetweenTests();
  const capturedErrors: FireleaseError[] = [];
  try {
    firelease.settings.safeQueueSize = 10;
    firelease.settings.queueLoadTimeout = 20;
    firelease.settings.captureError = error => {capturedErrors.push(error);};
    const source = new FakeQueueRef('safe-timeout-database', 'queues/safe-timeout');
    source.childrenKeysCountOverride = 10;
    source.deferNextValue = true;
    firelease.attachWorker(asNodeFire(source), {bufferSize: Infinity}, () => undefined);

    await assert.rejects(firelease.start(), {message: 'timeout'});
    await waitFor(() => capturedErrors.some(error =>
      error.level === 'fatal' && error.firelease?.code === 'queue-load-timeout'));
    const timeout = capturedErrors.find(error =>
      error.level === 'fatal' && error.firelease?.code === 'queue-load-timeout')!;
    assert.strictEqual(timeout.message, 'Firelease queue load timed out');
    assert.strictEqual(timeout.firelease?.description, 'startup');
    assert.strictEqual(timeout.firelease?.mode, 'safe');
    assert.strictEqual(timeout.firelease?.timeout, 20);
    assert(Object.values(source.queries[0].listeners).every(listeners => listeners.length === 0));
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
