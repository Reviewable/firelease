import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES, type FireleaseError} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('finite buffers bypass sizing while adaptive sources probe and reconnect', async () => {
  TESTABLES.resetBetweenTests();
  try {
    firelease.settings.safeQueueSize = 10;
    firelease.settings.queueCheckInterval = '30d';
    firelease.limitTasksInFlight(0);

    const finiteSource = new FakeQueueRef('finite-buffer-database', 'queues/finite');
    const adaptiveSource = new FakeQueueRef('adaptive-database', 'queues/adaptive');
    for (let index = 0; index < 9; index++) {
      adaptiveSource.addTask(`initial-${index}`, {payload: index});
    }
    firelease.attachWorker(asNodeFire(finiteSource), {bufferSize: 20}, () => undefined);
    firelease.attachWorker(asNodeFire(adaptiveSource), {bufferSize: Infinity}, () => undefined);
    await firelease.start();

    const finiteStats = firelease.stats.queues[0].sources[0];
    const adaptiveStats = firelease.stats.queues[1].sources[0];
    assert.strictEqual(finiteSource.queries[0].limit, 20);
    assert.strictEqual(finiteSource.childrenKeysCalls, 0);
    assert.strictEqual(finiteStats.size, null);
    assert.strictEqual(finiteStats.sizeTimestamp, undefined);
    assert.strictEqual(adaptiveStats.mode, 'safe');
    assert.strictEqual(adaptiveStats.size, 9);
    assert(adaptiveStats.sizeTimestamp);

    const probesBeforeReconnect = adaptiveSource.childrenKeysCalls;
    adaptiveSource.databaseRoot.setConnected(false);
    for (let index = 0; index < 9; index++) {
      await adaptiveSource.child(`initial-${index}`).remove();
    }
    adaptiveSource.databaseRoot.setConnected(true);
    await waitFor(() =>
      adaptiveSource.childrenKeysCalls > probesBeforeReconnect && adaptiveStats.mode === 'full');
    assert.strictEqual(firelease.state, 'paused');
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('promotion and demotion keep an already-started task alive', async () => {
  TESTABLES.resetBetweenTests();
  const originalRandom = Math.random;
  Math.random = () => 0;
  try {
    firelease.settings.safeQueueSize = 10;
    firelease.settings.queueCheckInterval = 20;
    firelease.limitTasksInFlight(1);
    const source = new FakeQueueRef('switch-database', 'queues/switch');
    const inProgressTask = source.addTask('a-in-progress', {payload: 'keep alive'});
    for (let index = 0; index < 8; index++) {
      source.addTask(`z-filler-${index}`, {payload: index});
    }
    let runs = 0;
    let finished = false;
    let releaseFirst!: () => void;
    const firstBlocked = new Promise<void>(resolve => {releaseFirst = resolve;});
    firelease.attachWorker(asNodeFire(source), {bufferSize: Infinity}, async item => {
      if (item.$ref.key !== 'a-in-progress') return;
      runs++;
      if (runs === 1) {
        await firstBlocked;
        return 1;
      }
      finished = true;
    });
    await firelease.start();
    const sourceStats = firelease.stats.queues[0].sources[0];
    await waitFor(() => runs === 1 && sourceStats.mode === 'safe');

    for (let index = 0; index < 8; index++) {
      await source.child(`z-filler-${index}`).remove();
    }
    await waitFor(() => sourceStats.mode === 'full');
    for (let index = 0; index < 11; index++) {
      source.addTask(`backlog-${index}`, {payload: index});
    }
    await waitFor(() => sourceStats.mode === 'safe');
    assert.strictEqual(finished, false);
    releaseFirst();
    await waitFor(() => finished && !inProgressTask.value);
  } finally {
    Math.random = originalRandom;
    TESTABLES.resetBetweenTests();
  }
});

test('safe sources report shallow-count mismatches with structured details', async () => {
  TESTABLES.resetBetweenTests();
  const capturedErrors: FireleaseError[] = [];
  const originalRandom = Math.random;
  Math.random = () => 0;
  try {
    firelease.settings.safeQueueSize = 10;
    firelease.settings.queueCheckInterval = 20;
    firelease.settings.captureError = error => {capturedErrors.push(error);};
    firelease.limitTasksInFlight(0);
    const source = new FakeQueueRef('mismatch-database', 'queues/mismatch');
    for (let index = 0; index < 10; index++) source.addTask(`task-${index}`, {payload: index});
    firelease.attachWorker(asNodeFire(source), {bufferSize: Infinity}, () => undefined);
    await firelease.start();

    source.childrenKeysCountOverride = 109;
    await source.child('task-0').remove();
    await waitFor(() => capturedErrors.some(
      error => error.firelease?.code === 'safe-queue-size-mismatch'));
    const error = capturedErrors.find(
      candidate => candidate.firelease?.code === 'safe-queue-size-mismatch')!;
    assert.strictEqual(error.message, 'Firelease safe queue size mismatch');
    assert.strictEqual(error.firelease?.count, 109);
    assert.strictEqual(error.firelease?.delta, 100);
    assert.strictEqual(error.firelease?.listenerLimit, 10);
    assert.strictEqual(error.firelease?.liveCount, 9);
    assert.strictEqual(error.firelease?.source, source.toString());
    assert.strictEqual(firelease.stats.queues[0].sources[0].sizeDelta, 100);
  } finally {
    Math.random = originalRandom;
    TESTABLES.resetBetweenTests();
  }
});

test('failed startup probes fall back to safe mode', async () => {
  TESTABLES.resetBetweenTests();
  const capturedErrors: FireleaseError[] = [];
  try {
    firelease.settings.safeQueueSize = 10;
    firelease.settings.queueCheckInterval = '30d';
    firelease.settings.captureError = error => {capturedErrors.push(error);};
    const source = new FakeQueueRef('failed-probe-database', 'queues/failed');
    source.childrenKeysError = new Error('REST unavailable');
    firelease.attachWorker(asNodeFire(source), {bufferSize: Infinity}, () => undefined);
    await firelease.start();

    const error = capturedErrors.find(
      candidate => candidate.firelease?.code === 'queue-count-failed')!;
    assert.strictEqual(error.message, 'Firelease queue count failed');
    assert.strictEqual(error.firelease?.cause, 'REST unavailable');
    assert.strictEqual(error.firelease?.reason, 'startup');
    assert.strictEqual(firelease.stats.queues[0].sources[0].mode, 'safe');
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('a safe source schedules its next check after the previous check finishes', async () => {
  TESTABLES.resetBetweenTests();
  const originalRandom = Math.random;
  Math.random = () => 0;
  try {
    firelease.settings.safeQueueSize = 10;
    firelease.settings.queueCheckInterval = 20;
    firelease.limitTasksInFlight(0);
    const source = new FakeQueueRef('check-spacing-database', 'queues/check-spacing');
    for (let index = 0; index < 10; index++) source.addTask(`task-${index}`, {payload: index});
    firelease.attachWorker(asNodeFire(source), {bufferSize: Infinity}, () => undefined);
    await firelease.start();

    const starts: number[] = [];
    const finishes: number[] = [];
    let releaseCheck!: () => void;
    source.childrenKeysHandler = () => new Promise<string[]>(resolve => {
      starts.push(performance.now());
      releaseCheck = () => {
        finishes.push(performance.now());
        resolve(Array.from({length: 10}, (_, index) => `task-${index}`));
      };
    });
    await waitFor(() => starts.length === 1);
    await new Promise(resolve => {setTimeout(resolve, 50);});
    assert.strictEqual(starts.length, 1, 'checks must not overlap or schedule from their start');
    releaseCheck();
    await waitFor(() => starts.length === 2);
    assert(starts[1] - finishes[0] >= 15);
    releaseCheck();
    await new Promise(resolve => {setTimeout(resolve, 10);});
  } finally {
    Math.random = originalRandom;
    TESTABLES.resetBetweenTests();
  }
});

test('an initial listener attachment failure rejects startup and crashes', async () => {
  TESTABLES.resetBetweenTests();
  const capturedErrors: FireleaseError[] = [];
  try {
    firelease.settings.captureError = error => {capturedErrors.push(error);};
    const source = new FakeQueueRef('broken-database', 'queues/broken');
    source.listenerError = new Error('Permission denied');
    firelease.attachWorker(asNodeFire(source), () => undefined);

    await assert.rejects(firelease.start(), {message: 'Permission denied'});
    const error = capturedErrors.find(
      candidate => candidate.firelease?.code === 'queue-startup-failed')!;
    assert.strictEqual(error.message, 'Firelease queue startup failed');
    assert.strictEqual(error.level, 'fatal');
    assert.strictEqual(error.firelease?.cause, 'Permission denied');
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
