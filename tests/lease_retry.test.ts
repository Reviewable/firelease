import assert from 'node:assert/strict';
import {test, type TestContext} from 'node:test';

import firelease, {TESTABLES, type FireleaseError} from '../src';
import {asNodeFire, FakeQueueRef} from './fake_firebase';

const START = 1_700_000_000_000;

async function settle(t: TestContext) {
  // Drain transaction/worker continuations and their zero-delay rescheduling callbacks.
  for (let i = 0; i < 20; i++) {
    await Promise.resolve();
    t.mock.timers.tick(0);
  }
}

async function advanceTo(t: TestContext, timestamp: number) {
  t.mock.timers.tick(timestamp - Date.now());
  await settle(t);
}

function mockClock(t: TestContext) {
  t.mock.timers.enable({apis: ['Date', 'setTimeout'], now: START});
  t.mock.method(performance, 'now', () => Date.now() - START);
}

for (const [random, delay] of [[0, 60_000], [0.5, 90_000], [1, 120_000]]) {
  test(`lease failures wait ${delay}ms and repeated failures start a fresh delay`, async t => {
    TESTABLES.resetBetweenTests();
    mockClock(t);
    let retryRandom = random;
    t.mock.method(Math, 'random', () => retryRandom);
    try {
      const source = new FakeQueueRef('lease-retry-bounds', 'queue');
      source.transactionError = new Error('Lease write failed');
      let attempts = 0;
      source.beforeTransaction = () => {attempts++;};
      const errors: FireleaseError[] = [];
      firelease.settings.captureError = error => {errors.push(error);};
      let workerCalls = 0;
      firelease.attachWorker(asNodeFire(source), {minLease: '1h'}, () => {workerCalls++;});
      await settle(t);
      const task = source.addTask('failed', {payload: 'unchanged'});
      await settle(t);
      assert.equal(attempts, 1);
      assert.deepEqual(firelease.listTasksInProgress(), []);
      assert.deepEqual(task.value, {payload: 'unchanged'}, 'cooldown needs no database write');

      await advanceTo(t, START + 3000);
      await advanceTo(t, START + 3100);
      assert.equal(attempts, 1, 'the old three-second retry must not run');
      await advanceTo(t, START + delay - 1);
      assert.equal(attempts, 1);
      retryRandom = 1 - random;
      await advanceTo(t, START + delay);
      assert.equal(attempts, 2, 'retry happens without an external scan or snapshot');
      assert.equal(errors.length, 2);
      assert.equal(errors[1].firelease?.phase, 'leasing');
      assert.equal(workerCalls, 0);

      source.transactionError = undefined;
      const secondDelay = 180_000 - delay;
      await advanceTo(t, START + delay + secondDelay - 1);
      assert.equal(attempts, 2);
      await advanceTo(t, START + delay + secondDelay);
      assert.equal(attempts, 3);
      assert.equal(workerCalls, 1);
      assert.equal(task.value, null);
    } finally {
      TESTABLES.resetBetweenTests();
    }
  });
}

test('cooldowns survive scans and listener replays without consuming quota', async t => {
  TESTABLES.resetBetweenTests();
  mockClock(t);
  t.mock.method(Math, 'random', () => 0.5);
  try {
    const source = new FakeQueueRef('lease-retry-replay', 'queue');
    let attempts = 0;
    let fail = true;
    source.beforeTransaction = task => {
      if (task.key === 'z-failed') attempts++;
      source.transactionError =
        fail && task.key === 'z-failed' ? new Error('Lease failed') : undefined;
    };
    firelease.settings.captureError = () => undefined;
    const completed: string[] = [];
    const task = source.addTask('z-failed', {payload: 1});
    firelease.attachWorker(asNodeFire(source), {
      bufferSize: 1, maxConcurrent: 1, minLease: '1h', leaseJitter: true
    }, item => {completed.push(item.$ref.key!);});
    await settle(t);
    assert.equal(attempts, 1);
    await firelease.shutdown();
    firelease.settings.globalMaxConcurrent = 1;
    await advanceTo(t, START + 1000);
    assert.equal(attempts, 1, 'releasing quota must not bypass the cooldown');
    source.addTask('z-failed', {payload: 2});
    // The one-item query evicts the failed task, then replays it when this task completes.
    source.addTask('a-healthy', {_lease: {expiry: -1}});
    await settle(t);
    assert.deepEqual(completed, ['a-healthy']);
    assert.equal(attempts, 1);

    source.root.setConnected(false);
    source.root.setConnected(true);
    await settle(t);
    await advanceTo(t, START + 89_999);
    assert.equal(attempts, 1);
    fail = false;
    await advanceTo(t, START + 90_000);
    assert.equal(attempts, 2);
    assert.deepEqual(completed, ['a-healthy', 'z-failed']);
    assert.equal(task.value, null);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('a newer Firebase lease still takes precedence over the retry deadline', async t => {
  TESTABLES.resetBetweenTests();
  mockClock(t);
  t.mock.method(Math, 'random', () => 0);
  try {
    const source = new FakeQueueRef('lease-retry-busy', 'queue');
    source.transactionError = new Error('Lease failed');
    let attempts = 0;
    source.beforeTransaction = () => {attempts++;};
    firelease.settings.captureError = () => undefined;
    firelease.attachWorker(asNodeFire(source), () => undefined);
    await settle(t);
    source.addTask('task', {});
    await settle(t);
    source.transactionError = undefined;
    const task = source.addTask('task', {_lease: {expiry: START + 180_000}});
    await advanceTo(t, START + 60_000);
    assert.equal(attempts, 1);
    await advanceTo(t, START + 179_999);
    assert.equal(attempts, 1);
    await advanceTo(t, START + 180_000);
    assert.equal(attempts, 2);
    assert.equal(task.value, null);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
