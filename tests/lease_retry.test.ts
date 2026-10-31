import assert from 'node:assert/strict';
import {test, type TestContext} from 'node:test';

import firelease, {TESTABLES, type FireleaseError} from '../src';
import {asNodeFire, FakeQueueRef, FakeSnapshot} from './fake_firebase';

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
  test(`lease failures exhaust quick retries then wait ${delay}ms on each failure`, async t => {
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

      for (const retryAt of [3000, 6000, 9000]) {
        const previousAttempts = attempts;
        await advanceTo(t, START + retryAt - 1);
        assert.equal(attempts, previousAttempts);
        await advanceTo(t, START + retryAt);
        assert.equal(attempts, previousAttempts + 1);
      }
      await advanceTo(t, START + 9000 + delay - 1);
      assert.equal(attempts, 4, 'Only three quick retries are allowed');
      retryRandom = 1 - random;
      await advanceTo(t, START + 9000 + delay);
      assert.equal(attempts, 5, 'retry happens without an external scan or snapshot');
      assert.equal(errors.length, 5);
      assert.equal(errors[4].firelease?.phase, 'leasing');
      assert.equal(workerCalls, 0);

      source.transactionError = undefined;
      const secondDelay = 180_000 - delay;
      await advanceTo(t, START + 9000 + delay + secondDelay - 1);
      assert.equal(attempts, 5);
      await advanceTo(t, START + 9000 + delay + secondDelay);
      assert.equal(attempts, 6);
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
    for (const retryAt of [3000, 6000, 9000]) await advanceTo(t, START + retryAt);
    assert.equal(attempts, 4);
    await firelease.shutdown();
    firelease.settings.globalMaxConcurrent = 1;
    await advanceTo(t, START + 10_000);
    assert.equal(attempts, 4, 'releasing quota must not bypass the cooldown');
    source.addTask('z-failed', {payload: 2});
    // The one-item query evicts the failed task, then replays it when this task completes.
    source.addTask('a-healthy', {_lease: {expiry: -1}});
    await settle(t);
    assert.deepEqual(completed, ['a-healthy']);
    assert.equal(attempts, 4);

    source.root.setConnected(false);
    source.root.setConnected(true);
    await settle(t);
    await advanceTo(t, START + 98_999);
    assert.equal(attempts, 4);
    fail = false;
    await advanceTo(t, START + 99_000);
    assert.equal(attempts, 5);
    assert.deepEqual(completed, ['a-healthy', 'z-failed']);
    assert.equal(task.value, null);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

for (const priorFailures of [0, 2]) {
  test(`disconnect timeouts preserve a budget with ${priorFailures} prior failures`, async t => {
    TESTABLES.resetBetweenTests();
    mockClock(t);
    t.mock.method(Math, 'random', () => 0.5);
    try {
      const source = new FakeQueueRef('disconnect-retry-budget', 'queue');
      source.transactionError = new Error('Lease transaction timeout');
      const task = source.child('task');
      const transaction = task.transaction.bind(task);
      let pending = !priorFailures;
      let rejectTransaction!: (error: Error) => void;
      let attempts = 0;
      t.mock.method(task, 'transaction', update => {
        attempts++;
        return pending ? new Promise<never>((resolve, reject) => {
          rejectTransaction = reject;
        }) : transaction(update);
      });
      const errors: FireleaseError[] = [];
      firelease.settings.captureError = error => {errors.push(error);};
      firelease.attachWorker(asNodeFire(source), {bufferSize: 1}, () => {
        assert.fail('No lease transaction succeeds in this test');
      });
      await settle(t);
      source.addTask('task', {});
      await settle(t);
      for (let i = 1; i < priorFailures; i++) await advanceTo(t, START + i * 3000);
      if (priorFailures) {
        pending = true;
        await advanceTo(t, START + priorFailures * 3000);
      }
      for (let i = 0; i < 4; i++) {
        assert.equal(firelease.listTasksInProgress().length, 1);
        source.root.setConnected(false);
        await advanceTo(t, Date.now() + 1000);
        rejectTransaction(new Error('Lease transaction timeout'));
        await settle(t);
        assert.deepEqual(firelease.listTasksInProgress(), []);
        assert.equal(errors.length, priorFailures, 'Disconnect timeouts remain unreported');
        const attemptsBeforeReconnect = attempts;
        await advanceTo(t, Date.now() + 1000);
        assert.equal(attempts, attemptsBeforeReconnect, 'Nothing starts while disconnected');
        if (i === 3) pending = false;
        source.root.setConnected(true);
        await settle(t);
        assert.equal(attempts, attemptsBeforeReconnect + 1, 'Reconnect needs no new cooldown');
      }
      const reconnectedAt = Date.now();
      const attemptsAfterReconnect = attempts;
      const remainingQuickRetries = 3 - priorFailures;
      for (let i = 1; i <= remainingQuickRetries; i++) {
        await advanceTo(t, reconnectedAt + i * 3000 - 1);
        assert.equal(attempts, attemptsAfterReconnect + i - 1);
        await advanceTo(t, reconnectedAt + i * 3000);
        assert.equal(attempts, attemptsAfterReconnect + i);
      }
      const longRetryAt = reconnectedAt + remainingQuickRetries * 3000 + 90_000;
      await advanceTo(t, longRetryAt - 1);
      assert.equal(attempts, attemptsAfterReconnect + remainingQuickRetries);
      await advanceTo(t, longRetryAt);
      assert.equal(attempts, attemptsAfterReconnect + remainingQuickRetries + 1);
    } finally {
      TESTABLES.resetBetweenTests();
    }
  });
}

test('a reporting error after acquisition does not change the lease or retry budget', async t => {
  TESTABLES.resetBetweenTests();
  mockClock(t);
  try {
    const source = new FakeQueueRef('post-acquisition-error', 'queue');
    const task = source.child('task');
    t.mock.method(task, 'remove', () => Promise.reject(new Error('Cleanup failed')));
    let attempts = 0;
    source.beforeTransaction = () => {attempts++;};
    const errors: FireleaseError[] = [];
    firelease.settings.captureError = error => {
      if (error.message === 'Cleanup failed') throw new Error('Reporter failed');
      errors.push(error);
    };
    firelease.attachWorker(asNodeFire(source), {bufferSize: 1, minLease: '30s'}, () => undefined);
    await settle(t);
    source.addTask('task', {});
    await settle(t);
    assert.equal(attempts, 1);
    assert.equal(errors.length, 1);
    assert.equal(errors[0].message, 'Reporter failed');
    assert.equal(errors[0].firelease?.phase, 'processing');
    assert.deepEqual(firelease.listTasksInProgress(), []);
    await advanceTo(t, START + 29_999);
    assert.equal(attempts, 1, 'The acquired lease stays in effect after a reporting failure');

    source.transactionError = new Error('Lease transaction failed');
    await advanceTo(t, START + 30_000);
    assert.equal(attempts, 2);
    for (const retryAt of [33_000, 36_000, 39_000]) await advanceTo(t, START + retryAt);
    assert.equal(attempts, 5, 'All three quick lease retries remain available');
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

test('each task has its own quick-retry budget and recovers from a transient timeout', async t => {
  TESTABLES.resetBetweenTests();
  mockClock(t);
  t.mock.method(Math, 'random', () => 0.5);
  try {
    const source = new FakeQueueRef('independent-retry-budgets', 'queue');
    const attempts: Record<string, number> = {};
    source.beforeTransaction = task => {
      attempts[task.key] = (attempts[task.key] ?? 0) + 1;
      source.transactionError = task.key === 'a' || attempts[task.key] === 1 ?
        new Error('Lease transaction timeout') : undefined;
    };
    firelease.settings.captureError = () => undefined;
    const completed: string[] = [];
    firelease.attachWorker(asNodeFire(source), {maxConcurrent: 1}, item => {
      completed.push(item.$ref.key!);
    });
    await settle(t);
    source.addTask('a', {});
    await settle(t);
    for (const retryAt of [3000, 6000, 9000]) await advanceTo(t, START + retryAt);
    assert.equal(attempts.a, 4);
    await advanceTo(t, START + 10_000);
    source.addTask('b', {});
    await settle(t);
    assert.equal(attempts.b, 1);
    assert.deepEqual(firelease.listTasksInProgress(), []);
    await advanceTo(t, START + 12_999);
    assert.equal(attempts.b, 1);
    await advanceTo(t, START + 13_000);
    assert.equal(attempts.b, 2, 'Another task exhausting its budget must not consume this one');
    assert.deepEqual(completed, ['b']);
    assert.equal(attempts.a, 4, 'Other tasks can run during the long cooldown');
    await advanceTo(t, START + 99_000);
    assert.equal(attempts.a, 5);
    await advanceTo(t, START + 102_000);
    assert.equal(attempts.a, 5, 'Another task succeeding must not replenish this budget');
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

for (const [idleFor, nextDelay] of [[299_999, 90_000], [300_000, 3000]]) {
  test(`retry history after ${idleFor}ms idle gives a ${nextDelay}ms retry delay`, async t => {
    TESTABLES.resetBetweenTests();
    mockClock(t);
    t.mock.method(Math, 'random', () => 0.5);
    try {
      const source = new FakeQueueRef('idle-retry-history', 'queue');
      source.transactionError = new Error('Lease failed');
      let attempts = 0;
      source.beforeTransaction = () => {attempts++;};
      firelease.settings.captureError = () => undefined;
      firelease.attachWorker(asNodeFire(source), () => undefined);
      await settle(t);
      const task = source.addTask('task', {});
      await settle(t);
      for (const retryAt of [3000, 6000, 9000]) await advanceTo(t, START + retryAt);
      assert.equal(attempts, 4);
      await task.remove();
      await settle(t);
      // Query removal might just be eviction, so it must not immediately reset retry history.
      const reappearAt = START + 9000 + idleFor;
      await advanceTo(t, reappearAt);
      source.addTask('task', {});
      await settle(t);
      assert.equal(attempts, 5);
      await advanceTo(t, reappearAt + nextDelay - 1);
      assert.equal(attempts, 5);
      await advanceTo(t, reappearAt + nextDelay);
      assert.equal(attempts, 6);
    } finally {
      TESTABLES.resetBetweenTests();
    }
  });
}

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

for (const key of ['ping', 'task']) {
  test(`persistent ${key} failures exhaust quick retries until a transaction succeeds`, async t => {
    TESTABLES.resetBetweenTests();
    mockClock(t);
    t.mock.method(Math, 'random', () => 0.5);
    try {
      const source = new FakeQueueRef(`${key}-retry-persistent`, 'queue');
      source.transactionError = new Error('Lease transaction failed');
      let attempts = 0;
      source.beforeTransaction = () => {attempts++;};
      const blockedReports: FireleaseError[] = [];
      firelease.settings.captureError = error => {
        if (error.firelease?.code === 'queue-retries-blocked') blockedReports.push(error);
      };
      let workerCalls = 0;
      firelease.attachWorker(asNodeFire(source), {bufferSize: 1}, () => {
        assert.notEqual(key, 'ping', 'Ping cleanup must not invoke the application worker');
        workerCalls++;
      });
      await settle(t);
      const task = source.addTask(key, {timestamp: source.now, _lease: {expiry: source.now}});
      await settle(t);
      assert.equal(attempts, 1);
      for (const retryAt of [3000, 6000, 9000]) {
        const previousAttempts = attempts;
        source.root.setConnected(false);
        source.root.setConnected(true);
        await settle(t);
        await advanceTo(t, START + retryAt - 1);
        assert.equal(attempts, previousAttempts);
        assert.equal(blockedReports.length, 0, 'Short retries do not trigger urgent reports');
        await advanceTo(t, START + retryAt);
        assert.equal(attempts, previousAttempts + 1);
      }
      assert.equal(attempts, 4);
      assert.equal(blockedReports.length, 1, 'Exhausted retries can block a limited listener');
      source.root.setConnected(false);
      source.root.setConnected(true);
      await settle(t);
      await advanceTo(t, START + 12_000);
      assert.equal(attempts, 4, 'The quick-retry budget is exhausted');
      for (const retryAt of [99_000, 189_000, 279_000, 369_000]) {
        const previousAttempts = attempts;
        await advanceTo(t, START + retryAt - 1);
        assert.equal(
          attempts, previousAttempts, 'Cooldown expiry must not replenish quick retries');
        await advanceTo(t, START + retryAt);
        assert.equal(attempts, previousAttempts + 1);
        assert.equal(blockedReports.length, 1, 'Persistent failures do not repeat the report');
      }
      source.transactionError = undefined;
      await advanceTo(t, START + 458_999);
      assert.equal(attempts, 8);
      await advanceTo(t, START + 459_000);
      assert.equal(attempts, 9);
      assert.equal(task.value, null);

      source.transactionError = new Error('Another transient failure');
      source.addTask(key, {timestamp: source.now, _lease: {expiry: source.now}});
      await settle(t);
      assert.equal(attempts, 10);
      source.transactionError = undefined;
      await advanceTo(t, START + 461_999);
      assert.equal(attempts, 10);
      await advanceTo(t, START + 462_000);
      assert.equal(attempts, 11, 'A successful lease transaction replenishes quick retries');
      assert.equal(task.value, null);
      assert.equal(workerCalls, key === 'ping' ? 0 : 2);
    } finally {
      TESTABLES.resetBetweenTests();
    }
  });
}

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
      for (const retryAt of [3000, 6000, 9000]) await advanceTo(t, START + retryAt);
      assert.equal(blockedReports().length, 0, 'Wait for the complete initial query snapshot');
      source.releaseDeferredValues();
      await settle(t);
      const blocked = blockedReports()[0];
      assert.equal(blockedReports().length, 1);
      assert.equal(blocked.level, 'error');
      assert.deepEqual(blocked.firelease, {
        code: 'queue-retries-blocked', phase: 'retrying', mode: 'safe',
        queue: source.toString(), source: source.toString(),
        listenerLimit: 2, liveCount: 2, retryTimestamp: START + 99_000
      });
      assert.deepEqual(completed, [], 'The later healthy task is outside the bounded listener');
      assert.equal(source.queries[0].limit, 2);
      source.addTask('a', {payload: 'updated'});
      source.root.setConnected(false);
      source.root.setConnected(true);
      await settle(t);
      await advanceTo(t, START + 99_000);
      assert.equal(blockedReports().length, 1, 'Replays and failed retries do not repeat reports');

      source.transactionError = undefined;
      await advanceTo(t, START + 189_000);
      assert.deepEqual(completed.sort(), ['a', 'b', 'c']);
      source.transactionError = new Error('New failure episode');
      source.addTask('a', {});
      source.addTask('b', {});
      await settle(t);
      for (const retryAt of [192_000, 195_000, 198_000]) await advanceTo(t, START + retryAt);
      assert.equal(blockedReports().length, 2, 'Progress re-arms the blocked-buffer report');
    } finally {
      TESTABLES.resetBetweenTests();
    }
  });
}

test('blockage checks read only expiry and skip reported failure cycles', async t => {
  TESTABLES.resetBetweenTests();
  mockClock(t);
  t.mock.method(Math, 'random', () => 0.5);
  try {
    const source = new FakeQueueRef('blocked-retry-read-cost', 'queue');
    source.transactionError = new Error('Lease failed');
    for (let i = 0; i < 8; i++) source.addTask(`task-${i}`, {payload: 'x'.repeat(10_000)});
    const blockedReports: FireleaseError[] = [];
    firelease.settings.captureError = error => {
      if (error.firelease?.code === 'queue-retries-blocked') blockedReports.push(error);
    };
    firelease.attachWorker(asNodeFire(source), {bufferSize: 8}, () => undefined);
    await settle(t);
    const readValue = t.mock.method(FakeSnapshot.prototype, 'val');
    const readChild = t.mock.method(FakeSnapshot.prototype, 'child');
    for (const retryAt of [3000, 6000, 9000]) await advanceTo(t, START + retryAt);
    assert.equal(blockedReports.length, 1);
    assert.equal(readValue.mock.callCount(), 0, 'Blockage checks never copy task payloads');
    assert(readChild.mock.callCount() > 0);
    const childReads = readChild.mock.callCount();
    await advanceTo(t, START + 99_000);
    assert.equal(blockedReports.length, 1);
    assert.equal(readChild.mock.callCount(), childReads, 'Reported failures need no snapshot scan');

    // A real lease update still ends the episode even when failures skip the diagnostic scan.
    source.addTask('task-7', {_lease: {expiry: START + 180_000}});
    await settle(t);
    assert.equal(blockedReports.length, 1);
    source.addTask('task-7', {});
    await settle(t);
    assert.equal(
      blockedReports.length, 2, 'Listener updates still re-arm reporting after progress');
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

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
    for (const retryAt of [3000, 6000, 9000]) await advanceTo(t, START + retryAt);
    assert.equal(errors.filter(error =>
      error.firelease?.code === 'queue-retries-blocked').length, 0);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
