import assert from 'node:assert/strict';
import {test} from 'node:test';
import firelease, {TESTABLES, type LeaseJitterMetric} from '../src';
import {LeaseJitter, LeaseJitterSchedule} from '../src/jitter';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('affinity is stable across tasks, distinguishes instances, and spreads different keys', () => {
  const a = new LeaseJitter('instance-a'), b = new LeaseJitter('instance-b');
  assert.equal(a.delay('owner|repo|1'), new LeaseJitter('instance-a').delay('owner|repo|1'));
  assert.notEqual(a.delay('owner|repo|1'), b.delay('owner|repo|1'));
  const scores = Array.from({length: 200}, (_, i) => a.delay(`pr-${i}`));
  assert(scores.every(score => score >= 0 && score < 250));
  assert(scores.some(score => score < 25));
  assert(scores.some(score => score > 225));
});

test('controller bounds contention feedback and rejects invalid timings', () => {
  const controller = new LeaseJitter('test');
  controller.observePropagation(NaN);
  controller.observePropagation(-100);
  controller.observePropagation(60_000);
  controller.record('contended');
  assert.equal(controller.stats.window, 440);
  for (let i = 0; i < 19; i++) controller.record('suppressed');
  assert.equal(controller.stats.window, 250);
  for (let i = 0; i < 1000; i++) controller.record('contended');
  assert.equal(controller.stats.window, 5000);
  for (let i = 0; i < 1000; i++) controller.record('suppressed');
  assert.equal(controller.stats.window, 25);
});

test('deadlines survive rescans and payload updates and renew after lease expiry', () => {
  const controller = new LeaseJitter('test');
  const schedule = new LeaseJitterSchedule(controller);
  schedule.update(undefined, 'pr', 1000, true, false);
  const deadline = schedule.deadline(1000, true);
  assert(deadline > 1000);
  controller.record('contended');
  schedule.update(undefined, 'pr', 1001, true, false);
  assert.equal(schedule.deadline(1001, true), deadline);
  assert.equal(controller.stats.scheduled, 1);
  assert.equal(schedule.start(), 1000);
  schedule.update({expiry: 5000, time: 4000, attempts: 1, busy: true}, 'pr', 1001, true, true);
  assert.equal(schedule.deadline(2000, true), 5000);
  assert(schedule.deadline(5000, true) > 5000);
  assert.equal(controller.stats.scheduled, 2);
});

test('backlog, quota waits, failure retries and old expiries bypass jitter', () => {
  const controller = new LeaseJitter('test');
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
  const controller = new LeaseJitter('test');
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

test('live snapshots cancel real scheduled transactions, without taking worker slots', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('jitter', 'queue');
    const events: LeaseJitterMetric[] = [];
    const calls: string[] = [];
    firelease.settings.globalMaxConcurrent = 1;
    firelease.attachWorker(asNodeFire(source), {
      leaseJitter: true,
      captureLeaseJitterMetrics: metric => {events.push(metric);}
    }, item => {calls.push(item.$ref.key!);});
    await waitFor(() => firelease.stats.queues[0]?.mode === 'full');
    source.addTask('pending', {_lease: {affinityKey: slowAffinity()}});
    assert.equal(firelease.stats.leaseJitter?.scheduled, 1);
    assert.deepEqual(firelease.listTasksInProgress(), []);
    // A second queue still has access to the sole global slot while the first waits.
    const immediate = new FakeQueueRef('jitter', 'other');
    firelease.attachWorker(asNodeFire(immediate), item => {calls.push(item.$ref.key!);});
    immediate.addTask('immediate', {});
    await waitFor(() => calls.length === 1);
    source.addTask('pending', {_lease: {
      affinityKey: slowAffinity(), expiry: source.now + 60_000, time: 60_000,
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
    firelease.attachWorker(asNodeFire(source), {leaseJitter: true}, () => {calls++;});
    await waitFor(() => firelease.stats.queues[0]?.mode === 'full');
    const first = source.addTask('blacklisted', {_lease: {affinityKey: slowAffinity()}});
    firelease.blacklist(first.toString());
    source.addTask('disconnected', {_lease: {affinityKey: slowAffinity()}});
    source.databaseRoot.setConnected(false);
    await new Promise(resolve => {setTimeout(resolve, 300);});
    assert.equal(calls, 0);
    assert.equal(firelease.stats.leaseJitter?.suppressed, 0);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('callback failures cannot break acquisition; affinity stays on the lease', async () => {
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
      assert.equal(item._lease.affinityKey, 'owner|repo|1');
      calls++;
    });
    await waitFor(() => firelease.stats.queues[0]?.mode === 'full');
    const task = source.addTask('new', {_lease: {affinityKey: 'owner|repo|1'}});
    await waitFor(() => calls === 1 && !task.value);
    assert.equal(firelease.stats.leaseTransactions.acquired, 1);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
