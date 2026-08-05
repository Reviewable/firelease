import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES, type FireleaseError} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('adaptive sources probe, promote, demote, and recover independently', async () => {
  TESTABLES.resetBetweenTests();
  const capturedErrors: FireleaseError[] = [];
  const originalRandom = Math.random;
  Math.random = () => 0;
  try {
    firelease.settings.safeQueueSize = 10;
    assert.throws(
      () => {firelease.settings.queueCheckInterval = 0;},
      /queueCheckInterval must be a positive finite duration/);
    assert.throws(
      () => {firelease.settings.queueCheckInterval = 'invalid';},
      /queueCheckInterval must be a positive finite duration/);
    firelease.settings.queueCheckInterval = 20;
    firelease.settings.captureError = error => {capturedErrors.push(error);};
    firelease.settings.globalMaxConcurrent = 0;

    const explicitlyBufferedSource =
      new FakeQueueRef('explicit-buffer-database', 'queues/explicit');
    firelease.attachWorker(
      asNodeFire(explicitlyBufferedSource), {bufferSize: 20}, () => undefined);
    const explicitlyBufferedStats =
      firelease.stats.queues[firelease.stats.queues.length - 1].sources[0];
    await waitFor(() => explicitlyBufferedSource.queries.length === 1);
    assert.strictEqual(explicitlyBufferedSource.queries[0].limit, 20);
    assert.strictEqual(explicitlyBufferedSource.childrenKeysCalls, 0);
    assert.strictEqual(explicitlyBufferedStats.size, null);
    assert.strictEqual(explicitlyBufferedStats.sizeTimestamp, undefined);

    const adaptiveSource = new FakeQueueRef('adaptive-database', 'queues/adaptive');
    for (let index = 0; index < 9; index++) {
      adaptiveSource.addTask(`initial-${index}`, {payload: index});
    }
    let inProgressStarted = false;
    let inProgressFinished = false;
    let releaseInProgress!: () => void;
    const inProgressBlocked = new Promise<void>(resolve => {releaseInProgress = resolve;});
    firelease.attachWorker(asNodeFire(adaptiveSource), {bufferSize: Infinity}, async item => {
      if (item.$ref.key === 'in-progress') {
        inProgressStarted = true;
        await inProgressBlocked;
        inProgressFinished = true;
      }
    });
    const adaptiveStats = firelease.stats.queues[firelease.stats.queues.length - 1].sources[0];
    await waitFor(() => adaptiveStats.mode === 'safe' && adaptiveStats.size === 9);
    assert(adaptiveStats.sizeTimestamp);
    const initialSafeQuery = adaptiveSource.queries[adaptiveSource.queries.length - 1];
    assert.strictEqual(initialSafeQuery.limit, 10);

    adaptiveSource.deferNextValue = true;
    for (let index = 0; index < 9; index++) {
      await adaptiveSource.child(`initial-${index}`).remove();
    }
    await waitFor(() => adaptiveSource.deferredValueCallbacks.length === 1);
    assert.strictEqual(adaptiveStats.mode, 'safe');
    assert(Object.values(initialSafeQuery.listeners).every(listeners => listeners.length === 0));
    const inProgressTask = adaptiveSource.addTask('in-progress', {payload: 'keep alive'});
    for (let index = 0; index < 10; index++) {
      adaptiveSource.addTask(`backlog-${index}`, {payload: index});
    }
    assert.strictEqual(adaptiveStats.mode, 'safe');
    assert.strictEqual(adaptiveStats.size, 0);
    assert(adaptiveStats.sizeTimestamp);
    firelease.settings.globalMaxConcurrent = 1;
    await waitFor(() => inProgressStarted);
    firelease.settings.globalMaxConcurrent = 0;
    adaptiveSource.deferNextValue = true;
    adaptiveSource.releaseDeferredValues();
    await waitFor(
      () => adaptiveStats.mode === 'full' && adaptiveSource.deferredValueCallbacks.length === 1);
    assert(Object.values(adaptiveSource.listeners).every(listeners => listeners.length === 0));
    assert.strictEqual(inProgressFinished, false);
    adaptiveSource.releaseDeferredValues();
    await waitFor(
      () => adaptiveStats.mode === 'safe' && adaptiveStats.size === 11 &&
        adaptiveStats.sizeTimestamp !== undefined);
    assert(adaptiveStats.sizeTimestamp);
    releaseInProgress();
    await waitFor(() => inProgressFinished && !inProgressTask.value);

    adaptiveSource.childrenKeysCountOverride = 109;
    await adaptiveSource.child('backlog-0').remove();
    await waitFor(() => capturedErrors.some(
      error => error.firelease?.code === 'safe-queue-size-mismatch'));
    const mismatchError = capturedErrors.find(
      error => error.firelease?.code === 'safe-queue-size-mismatch')!;
    assert.strictEqual(mismatchError.message, 'Firelease safe queue size mismatch');
    assert.strictEqual(mismatchError.firelease?.count, 109);
    assert.strictEqual(mismatchError.firelease?.delta, 100);
    assert.strictEqual(mismatchError.firelease?.listenerLimit, 10);
    assert.strictEqual(mismatchError.firelease?.liveCount, 9);
    assert.strictEqual(mismatchError.firelease?.source, adaptiveSource.toString());
    assert.strictEqual(adaptiveStats.sizeDelta, 100);
    adaptiveSource.childrenKeysCountOverride = undefined;

    const probesBeforeReconnect = adaptiveSource.childrenKeysCalls;
    adaptiveSource.databaseRoot.setConnected(false);
    for (let index = 0; index < 10; index++) {
      await adaptiveSource.child(`backlog-${index}`).remove();
    }
    adaptiveSource.databaseRoot.setConnected(true);
    await waitFor(
      () => adaptiveSource.childrenKeysCalls > probesBeforeReconnect &&
        adaptiveStats.mode === 'full');
    assert.strictEqual(adaptiveStats.sizeDelta, undefined);

    // Keep intervals beyond the native timer limit from overflowing and firing immediately.
    firelease.settings.queueCheckInterval = '30d';
    const failedProbeSource = new FakeQueueRef('failed-probe-database', 'queues/failed');
    failedProbeSource.childrenKeysError = Object.assign(
      new Error('REST unavailable'), {code: 'timeout'});
    firelease.attachWorker(asNodeFire(failedProbeSource), {bufferSize: Infinity}, () => undefined);
    await waitFor(
      () => capturedErrors.some(error =>
        error.firelease?.code === 'queue-count-failed' &&
        error.firelease.source === failedProbeSource.toString()));
    await new Promise(resolve => {setTimeout(resolve, 25);});
    assert.strictEqual(failedProbeSource.childrenKeysCalls, 1);
    const countError = capturedErrors.find(
      error => error.firelease?.code === 'queue-count-failed')!;
    assert.strictEqual(countError.message, 'Firelease queue count failed');
    assert.strictEqual(countError.firelease?.cause, 'REST unavailable');
    assert.strictEqual(countError.firelease?.reason, 'startup');
    assert.strictEqual(countError.firelease?.source, failedProbeSource.toString());

    const orderedFirst = new FakeQueueRef('ordered-first', 'queues/first');
    const orderedSecond = new FakeQueueRef('ordered-second', 'queues/second');
    orderedFirst.databaseRoot.connected = false;
    orderedSecond.databaseRoot.connected = false;
    firelease.attachWorker(
      [asNodeFire(orderedFirst), asNodeFire(orderedSecond)],
      {bufferSize: Infinity},
      () => undefined);
    orderedSecond.databaseRoot.setConnected(true);
    await waitFor(() => orderedSecond.childrenKeysCalls === 1);
    assert.strictEqual(
      orderedFirst.childrenKeysCalls, 0,
      'a disconnected source must not block another source from initializing');
    orderedFirst.databaseRoot.setConnected(true);
    await waitFor(
      () => orderedFirst.childrenKeysCalls === 1 && orderedSecond.childrenKeysCalls === 1);
    assert(Array.isArray(firelease.stats.sickQueues));
    assert(Array.isArray(firelease.stats.sickSources));
    assert(firelease.stats.queues.every(queue => Array.isArray(queue.sources)));

    const brokenSource = new FakeQueueRef('broken-database', 'queues/broken');
    brokenSource.listenerError = new Error('Permission denied');
    firelease.attachWorker(asNodeFire(brokenSource), () => undefined);
    await waitFor(() => capturedErrors.some(
      error => error.firelease?.code === 'queue-startup-failed' &&
        error.firelease.source === brokenSource.toString()));
    const startupError = capturedErrors.find(
      error => error.firelease?.code === 'queue-startup-failed' &&
        error.firelease.source === brokenSource.toString())!;
    assert.strictEqual(startupError.message, 'Firelease queue startup failed');
    assert.strictEqual(startupError.level, 'fatal');
    assert.strictEqual(startupError.firelease?.cause, 'Permission denied');
    assert.strictEqual(startupError.firelease?.phase, 'crashing');
  } finally {
    Math.random = originalRandom;
    TESTABLES.resetBetweenTests();
  }
});
