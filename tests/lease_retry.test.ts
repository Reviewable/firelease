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

for (const healthyPingLatency of [1500, 5000]) {
  test(`ping cleanup recovers with a ${healthyPingLatency}ms health threshold`, async t => {
    TESTABLES.resetBetweenTests();
    mockClock(t);
    try {
      const source = new FakeQueueRef('ping-retry-transient', 'queue');
      let cleanupAttempts = 0;
      source.beforeTransaction = task => {
        source.transactionError = task.key === 'ping' && task.value && ++cleanupAttempts === 1 ?
          new Error('Ping deletion failed') : undefined;
      };
      firelease.settings.captureError = () => undefined;
      firelease.attachWorker(asNodeFire(source), {healthyPingLatency}, () => {
        assert.fail('Ping cleanup must not invoke the application worker');
      });
      await settle(t);
      const reports: {healthy: boolean, latency: number | null}[] = [];
      const captureReport = () => {
        const {healthy, latency} = firelease.stats.queues[0].sources[0];
        reports.push({healthy, latency});
      };
      firelease.pingQueues(captureReport, 1000);
      await advanceTo(t, START + 1000);
      firelease.pingQueues(captureReport, '1h');
      assert.equal(cleanupAttempts, 1);
      assert.equal(reports.length, 0);
      await advanceTo(t, START + 3999);
      assert.equal(cleanupAttempts, 1, 'No immediate ping retry');
      await advanceTo(t, START + 4000);
      assert.equal(cleanupAttempts, 2);
      assert.equal(source.child('ping').value, null);
      assert.deepEqual(reports, [{healthy: healthyPingLatency > 3000, latency: 3000}]);

      firelease.pingQueues(captureReport, 1000);
      await advanceTo(t, START + 5000);
      assert.equal(cleanupAttempts, 3);
      assert.deepEqual(reports[1], {healthy: true, latency: 0});
    } finally {
      TESTABLES.resetBetweenTests();
    }
  });
}

test('persistent ping failures exhaust quick retries across reconnects until cleanup succeeds',
  async t => {
    TESTABLES.resetBetweenTests();
    mockClock(t);
    t.mock.method(Math, 'random', () => 0.5);
    try {
      const source = new FakeQueueRef('ping-retry-persistent', 'queue');
      source.transactionError = new Error('Ping deletion failed');
      let attempts = 0;
      source.beforeTransaction = () => {attempts++;};
      const blockedReports: FireleaseError[] = [];
      firelease.settings.captureError = error => {
        if (error.firelease?.code === 'queue-retries-blocked') blockedReports.push(error);
      };
      firelease.attachWorker(asNodeFire(source), {bufferSize: 1}, () => {
        assert.fail('Ping cleanup must not invoke the application worker');
      });
      await settle(t);
      const ping = source.addTask('ping', {timestamp: source.now, _lease: {expiry: source.now}});
      await settle(t);
      assert.equal(attempts, 1);
      for (const retryAt of [3000, 6000, 9000]) {
        const previousAttempts = attempts;
        source.root.setConnected(false);
        source.root.setConnected(true);
        await settle(t);
        await advanceTo(t, START + retryAt - 1);
        assert.equal(attempts, previousAttempts);
        assert.equal(blockedReports.length, 0, 'Short ping retries do not trigger urgent reports');
        await advanceTo(t, START + retryAt);
        assert.equal(attempts, previousAttempts + 1);
      }
      assert.equal(attempts, 4);
      assert.equal(blockedReports.length, 1, 'Exhausted ping retries can block a limited listener');
      source.root.setConnected(false);
      source.root.setConnected(true);
      await settle(t);
      await advanceTo(t, START + 12_000);
      assert.equal(attempts, 4, 'The quick-retry budget is exhausted');
      await advanceTo(t, START + 98_999);
      assert.equal(attempts, 4);
      await advanceTo(t, START + 99_000);
      assert.equal(attempts, 5);
      assert.equal(blockedReports.length, 1, 'Persistent failures do not repeat the report');
      source.transactionError = undefined;
      await advanceTo(t, START + 188_999);
      assert.equal(attempts, 5, 'Cooldown expiry must not replenish quick retries');
      await advanceTo(t, START + 189_000);
      assert.equal(attempts, 6);
      assert.equal(ping.value, null);

      source.transactionError = new Error('Another transient failure');
      source.addTask('ping', {timestamp: source.now, _lease: {expiry: source.now}});
      await settle(t);
      assert.equal(attempts, 7);
      source.transactionError = undefined;
      await advanceTo(t, START + 191_999);
      assert.equal(attempts, 7);
      await advanceTo(t, START + 192_000);
      assert.equal(attempts, 8, 'Successful cleanup replenishes quick retries');
      assert.equal(ping.value, null);
    } finally {
      TESTABLES.resetBetweenTests();
    }
  });

for (const adaptive of [false, true]) {
  test(`${adaptive ? 'adaptive' : 'fixed'} listeners report complete retry blockage`, async t => {
    TESTABLES.resetBetweenTests();
    mockClock(t);
    t.mock.method(Math, 'random', () => 0.5);
    try {
      const source = new FakeQueueRef('blocked-retry-buffer', 'queue');
      source.transactionError = new Error('Lease failed');
      source.deferNextValue = true;
      source.addTask('a', {});
      source.addTask('b', {});
      source.addTask('c', {});
      firelease.settings.safeQueueSize = 2;
      firelease.settings.queueCheckInterval = '1h';
      const errors: FireleaseError[] = [];
      firelease.settings.captureError = error => {errors.push(error);};
      const blockedReports = () => errors.filter(error =>
        error.firelease?.code === 'queue-retries-blocked');
      const completed: string[] = [];
      firelease.attachWorker(asNodeFire(source), {bufferSize: adaptive ? Infinity : 2}, item => {
        completed.push(item.$ref.key!);
      });
      await settle(t);
      assert.equal(blockedReports().length, 0, 'Wait for the complete initial query snapshot');
      source.releaseDeferredValues();
      await settle(t);
      const blocked = blockedReports()[0];
      assert.equal(blockedReports().length, 1);
      assert.equal(blocked.level, 'error');
      assert.deepEqual(blocked.firelease, {
        code: 'queue-retries-blocked', phase: 'retrying', mode: 'safe',
        queue: source.toString(), source: source.toString(),
        listenerLimit: 2, liveCount: 2, retryAt: START + 90_000
      });
      assert.deepEqual(completed, [], 'The later healthy task is outside the bounded listener');
      assert.equal(source.queries[0].limit, 2);
      source.addTask('a', {payload: 'updated'});
      source.root.setConnected(false);
      source.root.setConnected(true);
      await settle(t);
      await advanceTo(t, START + 90_000);
      assert.equal(blockedReports().length, 1, 'Replays and failed retries do not repeat reports');

      source.transactionError = undefined;
      await advanceTo(t, START + 180_000);
      assert.deepEqual(completed.sort(), ['a', 'b', 'c']);
      source.transactionError = new Error('New failure episode');
      source.addTask('a', {});
      source.addTask('b', {});
      await settle(t);
      assert.equal(blockedReports().length, 2, 'Progress re-arms the blocked-buffer report');
    } finally {
      TESTABLES.resetBetweenTests();
    }
  });
}

test('only fully cooling limited listeners report blockage', async t => {
  TESTABLES.resetBetweenTests();
  mockClock(t);
  const errors: FireleaseError[] = [];
  try {
    firelease.settings.captureError = error => {errors.push(error);};
    for (const kind of ['full', 'underfilled', 'partial', 'leased']) {
      const source = new FakeQueueRef(`not-blocked-${kind}`, 'queue');
      source.beforeTransaction = task => {
        source.transactionError = task.key === 'a' ? new Error('Lease failed') : undefined;
      };
      source.addTask('a', {});
      if (kind === 'partial') source.addTask('b', {_lease: {expiry: START + 180_000}});
      firelease.attachWorker(asNodeFire(source), {
        bufferSize: kind === 'full' ? Infinity : kind === 'leased' ? 1 : 2
      }, () => undefined);
      if (kind === 'leased') {
        source.addTask('a', {_lease: {expiry: START + 180_000}});
      }
      await settle(t);
    }
    assert.equal(errors.filter(error =>
      error.firelease?.code === 'queue-retries-blocked').length, 0);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
