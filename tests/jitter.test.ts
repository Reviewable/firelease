import assert from 'node:assert/strict';
import {test} from 'node:test';
import firelease, {TESTABLES, type LeaseJitterMetric} from '../src';
import {LeaseJitter, LeaseJitterGain, LeaseJitterSchedule} from '../src/jitter';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('affinity is stable across tasks, distinguishes instances, and spreads different keys', () => {
  const a = new LeaseJitter({}, 'instance-a'), b = new LeaseJitter({}, 'instance-b');
  assert.equal(a.delay('owner|repo|1'), new LeaseJitter({}, 'instance-a').delay('owner|repo|1'));
  assert.notEqual(a.delay('owner|repo|1'), b.delay('owner|repo|1'));
  const scores = Array.from({length: 200}, (_, i) => a.delay(`pr-${i}`));
  assert(scores.every(score => score >= 0 && score < 250));
  assert(scores.some(score => score < 25));
  assert(scores.some(score => score > 225));
});

test('controller bounds contention feedback and rejects invalid timings', () => {
  const controller = new LeaseJitter({}, 'test');
  controller.observePropagation(NaN);
  controller.observePropagation(-100);
  controller.observePropagation(60_000);
  controller.record('contended', 0);
  assert.equal(controller.stats.window, 440);
  for (let i = 0; i < 19; i++) controller.record('suppressed', (i + 1) * 1000);
  assert.equal(controller.stats.window, 250);
  for (let i = 0; i < 1000; i++) controller.record('contended', (i + 20) * 1000);
  assert.equal(controller.stats.window, 5000);
  for (let i = 0; i < 1000; i++) controller.record('suppressed', (i + 1020) * 1000);
  assert.equal(controller.stats.window, 0);
});

test('gain is bounded for sparse observations and simultaneous bursts', () => {
  const gain = new LeaseJitterGain();
  assert.equal(gain.next(0), 2);
  for (let i = 1; i < 100; i++) assert.equal(gain.next(0), 2);
  for (let i = 100; i < 200; i++) gain.next(0);
  assert.equal(gain.next(0), 200 / 201);
  // A pause only forgets old rate evidence, never grants an unbounded catch-up adjustment.
  assert.equal(gain.next(3_600_000), 2);
  assert.equal(gain.next(3_599_000), 2, 'Backwards clocks do not add elapsed time');
});

test('busy sources share a per-second gain budget instead of a per-task gain', () => {
  for (const rate of [40, 400, 4000]) {
    const gain = new LeaseJitterGain();
    let budget = 0;
    for (let i = 0; i < rate * 60; i++) {
      const next = gain.next(i * 1000 / rate);
      if (i >= rate * 59) budget += next;
    }
    assert(Math.abs(budget - 40) < 0.11, `rate ${rate}: gain budget ${budget}`);
  }
});

test('wins, contentions and suppressions all contribute to the opportunity rate', () => {
  const changes: number[] = [];
  for (const outcome of ['acquired', 'contended', 'suppressed'] as const) {
    const controller = new LeaseJitter({maxWindow: 1e9});
    controller.stats.window = 1e6;
    for (let i = 0; i < 200; i++) controller.record(outcome, 0);
    const before = controller.stats.window;
    controller.record(outcome, 0);
    changes.push((controller.stats.window - before) / (outcome === 'contended' ? 0.95 : -0.05));
  }
  assert(changes.every(change => Math.abs(change - 20000 / 201) < 1e-8));
});

test('idle controllers retain their window and resume with a bounded update', () => {
  const controller = new LeaseJitter();
  for (let i = 0; i < 100; i++) controller.record('acquired', 0);
  controller.stats.window = 1000;
  const before = controller.delay('affinity');
  assert.equal(controller.stats.window, 1000);
  assert.equal(controller.delay('affinity'), before);
  controller.record('contended', 3_600_000);
  assert.equal(controller.stats.window, 1190);
});

test('deadlines survive rescans and payload updates and renew after lease expiry', () => {
  const controller = new LeaseJitter({}, 'test');
  const schedule = new LeaseJitterSchedule(controller);
  schedule.update(undefined, 'pr', 1000, true, false);
  const deadline = schedule.deadline(1000, true);
  assert(deadline > 1000);
  controller.record('contended');
  schedule.update(undefined, 'changed-affinity', 1001, true, false);
  assert.equal(schedule.deadline(1001, true), deadline);
  assert.equal(controller.stats.scheduled, 1);
  assert.equal(schedule.start(), 1000);
  schedule.update({expiry: 5000, time: 4000, attempts: 1, busy: true}, 'pr', 1001, true, true);
  assert.equal(schedule.deadline(2000, true), 5000);
  assert(schedule.deadline(5000, true) > 5000);
  assert.equal(controller.stats.scheduled, 2);
});

test('backlog, quota waits, failure retries and old expiries bypass jitter', () => {
  const controller = new LeaseJitter({}, 'test');
  const schedule = new LeaseJitterSchedule(controller);
  schedule.update(undefined, 'pr', 1000, false, false);
  assert.equal(schedule.deadline(1000, true), 1000);
  schedule.update({expiry: 2000}, 'pr', 1000, false, false);
  assert.equal(schedule.deadline(2000, false), 2000);
  assert.equal(schedule.deadline(2001, true), 2001);
  schedule.update({expiry: 3000}, 'pr', 2000, true, false);
  schedule.skip();
  assert.equal(schedule.deadline(2001, true), 2001, 'failed optimistic lease must not delay retry');
  schedule.update({expiry: 0}, 'pr', 8000, true, false);
  assert.equal(schedule.deadline(8000, true), 8000);
  assert.equal(controller.stats.scheduled, 0);
});

test('only a competing lease or removal suppresses a pending attempt, once', () => {
  const controller = new LeaseJitter({}, 'test');
  const schedule = new LeaseJitterSchedule(controller);
  schedule.update(undefined, 'pr', 1000, true, false);
  schedule.deadline(1000, true);
  assert.equal(schedule.update(
    {expiry: 5000, time: 4000, attempts: 1, busy: true}, 'pr', 1010, true, false), true);
  assert.equal(schedule.suppress(1011), false);
  assert.equal(controller.stats.suppressed, 1);
  schedule.deadline(5000, true);
  assert.equal(schedule.update({expiry: 9000}, 'pr', 5010, true, false), false);
  assert.equal(controller.stats.suppressed, 1, 'rescheduling is not contention feedback');
});

function slowAffinity() {
  const controller = new LeaseJitter();
  return Array.from({length: 100}, (_, i) => `affinity-${i}`)
    .find(key => controller.delay(key) > 200)!;
}

test('wins decay to zero, but a new race reopens the window', () => {
  const controller = new LeaseJitter();
  for (let i = 0; i < 30; i++) controller.record('acquired', i * 1000);
  assert.equal(controller.stats.window, 0);
  assert.equal(controller.delay('anything'), 0);
  controller.record('contended', 30_000);
  assert.equal(controller.stats.window, 190);
  for (let i = 0; i < 1000; i++) controller.record('contended', (i + 31) * 1000);
  assert.equal(controller.stats.window, 5000);
  for (let i = 0; i < 1000; i++) controller.record('acquired', (i + 1031) * 1000);
  assert.equal(controller.stats.window, 0, 'A previously contended source also scales down');
});

test('custom bounds clamp startup and lower race targets decay more cautiously', () => {
  assert.equal(new LeaseJitter({maxWindow: 100}).stats.window, 100);
  assert.equal(new LeaseJitter({minWindow: 400}).stats.window, 400);
  const lower = new LeaseJitter({targetRaceRate: 0.01, maxWindow: 300});
  lower.record('acquired');
  assert.equal(lower.stats.window, 248);
  lower.record('contended');
  assert.equal(lower.stats.window, 300);
  const zero = new LeaseJitter({maxWindow: 0});
  zero.record('contended');
  assert.equal(zero.stats.window, 0);
  for (const options of [
    {targetRaceRate: 0}, {targetRaceRate: 1}, {targetRaceRate: NaN},
    {minWindow: -1}, {maxWindow: Infinity}, {minWindow: 10, maxWindow: 9}
  ]) assert.throws(() => new LeaseJitter(options), /Invalid leaseJitter/);
  const schedule = new LeaseJitterSchedule(
    new LeaseJitter({minWindow: 10_000, maxWindow: 10_000}, 'test'));
  schedule.update(undefined, 'pr', 0, true, false);
  const deadline = schedule.deadline(0, true);
  assert.equal(schedule.deadline(6000, true), deadline, 'custom long waits keep their deadline');
});

test('queue window bounds normalize once for all sources without mutating caller options', () => {
  TESTABLES.resetBetweenTests();
  try {
    let reads = 0;
    const options = {get minWindow() {reads++; return '400ms';}, maxWindow: '1.5 seconds'};
    firelease.attachWorker(['one', 'two'].map(name => asNodeFire(new FakeQueueRef(name, 'queue'))),
      {leaseJitter: options}, () => undefined);
    assert.equal(firelease.stats.queues[0].sources.length, 2);
    assert(firelease.stats.queues[0].sources.every(source => source.leaseJitter?.window === 400));
    assert.equal(reads, 1, 'Bounds are read once per queue, not once per source');
    assert.deepEqual(options, {minWindow: '400ms', maxWindow: '1.5 seconds'});
    for (const leaseJitter of [
      {minWindow: 'invalid'}, {maxWindow: ''}, {minWindow: '-1ms'},
      {minWindow: '2s', maxWindow: 1000}
    ]) {
      assert.throws(() => firelease.attachWorker(
        asNodeFire(new FakeQueueRef('invalid', 'queue')), {leaseJitter}, () => undefined));
    }
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('propagation excludes prefetch without changing lease duration metrics', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('prefetch-jitter', 'queue');
    source.transactionTries = 2;
    source.transactionDuration = 100;
    source.transactionPrefetchDuration = 1000;
    const captured: unknown[] = [];
    firelease.attachWorker(asNodeFire(source), {
      leaseJitter: true,
      captureLeaseTransactionMetrics: (...metric) => {captured.push(metric);}
    }, () => undefined);
    await waitFor(() => firelease.stats.queues[0]?.mode === 'full');
    const task = source.addTask('new', {});
    await waitFor(() => captured.length === 1 && !task.value);
    // EMA: .9 * 100 + .1 * (100 / 2) = 95ms; a win then subtracts 2 * 95 * .05.
    assert.equal(firelease.stats.leaseJitter?.window, 240.5);
    assert.deepEqual(captured, [['acquired', 2, 1100]]);
    assert.equal(firelease.stats.leaseTransactions.duration, 1100);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('pings bypass jitter, training, affinity, and application callbacks', async () => {
  for (const tries of [undefined, 0, 1, 2]) {
    TESTABLES.resetBetweenTests();
    try {
      const source = new FakeQueueRef('ping-bypass', 'queue');
      source.omitTransactionMetadata = tries === undefined;
      source.transactionTries = tries ?? 1;
      let transactions = 0;
      source.beforeTransaction = () => {transactions++;};
      const unexpected = () => {assert.fail('Pings never enter application callbacks');};
      firelease.attachWorker(asNodeFire(source), {
        leaseJitter: {minWindow: 5000, maxWindow: 5000}, getAffinity: unexpected,
        captureLeaseTransactionMetrics: unexpected, captureLeaseJitterMetrics: unexpected
      }, unexpected);
      await waitFor(() => firelease.stats.queues[0]?.mode === 'full');
      const ping = source.addTask('ping', {timestamp: source.now, _lease: {expiry: source.now}});
      assert.equal(transactions, 1, 'Deletion starts immediately, without a jitter timer');
      await waitFor(() => !ping.value && firelease.listTasksInProgress().length === 0);
      assert.equal(firelease.stats.leaseJitter?.scheduled, 0);
      assert.equal(firelease.stats.leaseJitter?.window, 5000);
      assert.equal(firelease.stats.leaseJitter?.suppressed, 0);
      assert.equal(firelease.stats.leaseTransactions.acquired, 0);
    } finally {
      TESTABLES.resetBetweenTests();
    }
  }
});

test('limited removals cancel waits without treating old snapshots as deletion proof', async () => {
  for (const key of ['fresh', 'ping']) {
    TESTABLES.resetBetweenTests();
    try {
      const source = new FakeQueueRef(`limited-removal-${key}`, 'queue');
      let calls = 0;
      firelease.attachWorker(asNodeFire(source), {
        bufferSize: 1, leaseJitter: {minWindow: 1000, maxWindow: 1000},
        getAffinity: () => slowAffinity(),
        captureLeaseJitterMetrics: () => {calls++;}
      }, () => {calls++;});
      await waitFor(() => firelease.stats.queues[0]?.sources[0].connected);
      const task = source.addTask(key, {_lease: {affinity: slowAffinity(), expiry: source.now}});
      assert.equal(firelease.stats.leaseJitter?.scheduled, key === 'ping' ? 0 : 1);
      await task.remove();
      assert.equal(firelease.stats.leaseJitter?.suppressed, 0);
      assert.equal(firelease.stats.leaseJitter?.window, 1000);
      await new Promise(resolve => {setTimeout(resolve, 1100);});
      assert.equal(calls, 0);
      assert.equal(firelease.stats.leaseTransactions.acquired, 0);
    } finally {
      TESTABLES.resetBetweenTests();
    }
  }
});

test('limited query eviction is not clean feedback', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('limited-eviction', 'queue');
    firelease.attachWorker(asNodeFire(source), {
      bufferSize: 1, leaseJitter: {minWindow: 1000, maxWindow: 1000},
      getAffinity: () => slowAffinity()
    }, () => undefined);
    await waitFor(() => firelease.stats.queues[0]?.sources[0].connected);
    source.addTask('later', {_lease: {affinity: slowAffinity(), expiry: source.now}});
    assert.equal(firelease.stats.leaseJitter?.scheduled, 1);
    // The earlier key occupies the query window, evicting an unchanged pending task.
    source.addTask('earlier', {_lease: {expiry: source.now - 10000}});
    assert.equal(firelease.stats.leaseJitter?.suppressed, 0);
    await waitFor(() => firelease.stats.leaseTransactions.acquired === 2);
    assert.equal(firelease.stats.leaseJitter?.scheduled, 2);
    assert.equal(firelease.stats.leaseJitter?.suppressed, 0);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('live snapshots cancel real scheduled transactions, without taking worker slots', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('jitter', 'queue');
    const events: LeaseJitterMetric[] = [];
    const calls: string[] = [];
    firelease.settings.globalMaxConcurrent = 1;
    firelease.attachWorker(asNodeFire(source), {
      leaseJitter: true,
      getAffinity: () => slowAffinity(),
      captureLeaseJitterMetrics: metric => {events.push(metric);}
    }, item => {calls.push(item.$ref.key!);});
    await waitFor(() => firelease.stats.queues[0]?.mode === 'full');
    source.addTask('pending', {_lease: {affinity: slowAffinity()}});
    assert.equal(firelease.stats.leaseJitter?.scheduled, 1);
    assert.deepEqual(firelease.listTasksInProgress(), []);
    // A second queue still has access to the sole global slot while the first waits.
    const immediate = new FakeQueueRef('jitter', 'other');
    firelease.attachWorker(asNodeFire(immediate), item => {calls.push(item.$ref.key!);});
    immediate.addTask('immediate', {});
    await waitFor(() => calls.length === 1);
    source.addTask('pending', {_lease: {
      affinity: slowAffinity(), expiry: source.now + 60_000, time: 60_000,
      attempts: 1, busy: true
    }});
    await new Promise(resolve => {setTimeout(resolve, 300);});
    assert.deepEqual(calls, ['immediate']);
    assert.deepEqual(events, [{outcome: 'suppressed'}]);
    assert.equal(firelease.stats.leaseJitter?.suppressed, 1);
    assert.equal(firelease.stats.queues[0].leaseTransactions.contended, 0);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('disconnect and blacklist cancel waits without contention feedback', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('cancel', 'queue');
    let calls = 0;
    firelease.attachWorker(asNodeFire(source), {
      leaseJitter: true, getAffinity: () => slowAffinity()
    }, () => {calls++;});
    await waitFor(() => firelease.stats.queues[0]?.mode === 'full');
    const first = source.addTask('blacklisted', {_lease: {affinity: slowAffinity()}});
    firelease.blacklist(first.toString());
    source.addTask('disconnected', {_lease: {affinity: slowAffinity()}});
    source.databaseRoot.setConnected(false);
    await new Promise(resolve => {setTimeout(resolve, 300);});
    assert.equal(calls, 0);
    assert.equal(firelease.stats.leaseJitter?.suppressed, 0);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('metric callback failures cannot break acquisition', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('metric', 'queue');
    let calls = 0;
    firelease.settings.captureError = () => {throw new Error('reporter');};
    firelease.attachWorker(asNodeFire(source), {
      leaseJitter: true,
      captureLeaseJitterMetrics: metric => {
        assert.equal(metric.outcome, 'acquired');
        if (metric.outcome === 'acquired') assert(metric.pickupDelay >= 0);
        throw new Error('metric');
      }
    }, item => {
      assert.equal(item.owner, 'owner');
      calls++;
    });
    await waitFor(() => firelease.stats.queues[0]?.mode === 'full');
    const task = source.addTask('new', {owner: 'owner'});
    await waitFor(() => calls === 1 && !task.value);
    assert.equal(firelease.stats.leaseTransactions.acquired, 1);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('snapshot affinity precedes preprocessing and survives replacement leases', async t => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('derived-affinity', 'queue');
    const ranked: string[] = [];
    t.mock.method(LeaseJitter.prototype, 'delay', (affinity: string) => {
      ranked.push(affinity);
      return 1;
    });
    let hookCalls = 0, processed = 0;
    const attempts = new Map<string, number>();
    firelease.attachWorker(asNodeFire(source), {
      leaseJitter: true,
      getAffinity: item => {
        hookCalls++;
        assert(!Object.prototype.hasOwnProperty.call(item._lease ?? {}, 'affinity'));
        const {owner, repo, prNumber} = JSON.parse(item.payload);
        return `${owner}|${repo}|${prNumber}`;
      },
      preprocess: item => {
        assert(hookCalls > 0, 'Affinity is available before the leasing transaction');
        return item;
      }
    }, item => {
      assert(!Object.prototype.hasOwnProperty.call(item._lease, 'affinity'),
        'No derived metadata is persisted');
      const attempt = (attempts.get(item.$ref.key!) ?? 0) + 1;
      attempts.set(item.$ref.key!, attempt);
      processed++;
      if (attempt === 1) return {expiry: source.now + 10};
    });
    await waitFor(() => firelease.stats.queues[0]?.mode === 'full');
    for (const key of ['user-a', 'user-b']) {
      const task = source.addTask(key, {
        payload: JSON.stringify({owner: 'o', repo: 'r', prNumber: 1})
      });
      await waitFor(() => !task.value);
    }
    assert.equal(processed, 4);
    assert.equal(ranked.length, 4);
    assert(ranked.every(affinity => affinity === 'o|r|1'));
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('missing, nullish, or broken affinity hooks fall back to the task URL', async t => {
  TESTABLES.resetBetweenTests();
  try {
    const ranked: string[] = [], errors: unknown[] = [];
    t.mock.method(LeaseJitter.prototype, 'delay', (affinity: string) => {
      ranked.push(affinity);
      return 0;
    });
    firelease.settings.captureError = error => {errors.push(error);};
    const hooks = [undefined, () => undefined, () => null, () => {throw new Error('hook');},
      (() => 123) as unknown as () => string];
    for (const [index, getAffinity] of hooks.entries()) {
      const source = new FakeQueueRef(`fallback-${index}`, 'queue');
      firelease.attachWorker(asNodeFire(source), {leaseJitter: true, getAffinity}, () => undefined);
      await waitFor(() => firelease.stats.queues[index]?.mode === 'full');
      const task = source.addTask('new', {_lease: {affinity: 'ignored-legacy-value'}});
      await waitFor(() => !task.value);
      assert.equal(ranked[ranked.length - 1], task.toString());
    }
    assert.equal(errors.length, 4, 'Failing hooks are reported on arrival and lease update');
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('disabled jitter never calls the affinity hook', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('disabled-affinity', 'queue');
    firelease.attachWorker(asNodeFire(source), {
      getAffinity: () => {assert.fail('Disabled jitter must not derive affinity');}
    }, () => undefined);
    const task = source.addTask('new', {});
    await waitFor(() => !task.value);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
