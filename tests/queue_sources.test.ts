import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('logical queues coordinate sources, leases, and task controls', async () => {
  TESTABLES.resetBetweenTests();
  try {
    assert.throws(
      () => {firelease.attachWorker([], () => {/* Do nothing. */});},
      /At least one queue ref is required/
    );

    const firstSource = new FakeQueueRef('first-database', 'queues/jobs');
    const secondSource = new FakeQueueRef('second-database', 'other/jobs');
    const calls: string[] = [];
    const leaseTimesRemaining: number[] = [];
    let active = 0;
    let maxActive = 0;
    let releaseFirst!: () => void;
    const firstBlocked = new Promise<void>(resolve => {releaseFirst = resolve;});

    firelease.attachWorker(
      [asNodeFire(firstSource), asNodeFire(secondSource)],
      {bufferSize: Infinity, minLease: '1s'},
      async item => {
        active++;
        maxActive = Math.max(maxActive, active);
        calls.push(item.$ref.toString());
        leaseTimesRemaining.push(item.$leaseTimeRemaining);
        if (calls.length === 1) await firstBlocked;
        active--;
      }
    );
    firelease.limitTasksInFlight(1);
    await firelease.start();

    const firstTask = firstSource.addTask('task', {payload: 1});
    const secondTask = secondSource.addTask('task', {payload: 2});
    const blacklistedTask = secondSource.addTask('blacklisted', {payload: 3});

    await waitFor(() => calls.length === 1);
    assert.deepStrictEqual(firelease.listTasksInProgress(), [firstTask.toString()]);
    assert.strictEqual(firelease.blacklist(blacklistedTask.toString()), true);
    assert.strictEqual(firelease.blacklist(blacklistedTask.toString()), false);
    await new Promise(resolve => {setTimeout(resolve, 50);});
    assert.strictEqual(calls.length, 1, 'the global in-flight limit must cover every source');

    releaseFirst();
    await waitFor(() => calls.length === 2 && !firstTask.value && !secondTask.value);

    assert.deepStrictEqual(calls, [firstTask.toString(), secondTask.toString()]);
    assert(leaseTimesRemaining.every(Number.isFinite));
    assert(leaseTimesRemaining.every(time => time > 0));
    assert.strictEqual(maxActive, 1);
    assert.deepStrictEqual(firelease.listTasksInProgress(), []);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('legacy, duplicate-source, and extension behavior survives explicit startup', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const legacySource = new FakeQueueRef('legacy-database', 'queues/legacy');
    let legacyTaskUrl: string | undefined;
    firelease.attachWorker(asNodeFire(legacySource), item => {
      legacyTaskUrl = item.$ref.toString();
      assert(Number.isFinite(item.$leaseTimeRemaining));
    });

    const duplicateSource = new FakeQueueRef('duplicate-database', 'queues/duplicate');
    let duplicateTaskUrl: string | undefined;
    firelease.attachWorker(
      [asNodeFire(duplicateSource), asNodeFire(duplicateSource)],
      {bufferSize: Infinity},
      item => {duplicateTaskUrl = item.$ref.toString();}
    );

    const extensionSource = new FakeQueueRef('extension-database', 'queues/extension');
    extensionSource.fixedNow = Date.now();
    let extensionComplete = false;
    firelease.attachWorker(asNodeFire(extensionSource), {minLease: '1s'}, async item => {
      const firstExtension = firelease.extendLease(item, '2s');
      const secondExtensionTimestamp = item.$ref.now;
      const secondExtension = firelease.extendLease(item, '3s');
      assert.strictEqual(firstExtension, secondExtension);
      await firstExtension;
      assert(item._lease.expiry >= secondExtensionTimestamp + 3000);
      extensionComplete = true;
    });

    await firelease.start();
    assert.strictEqual(duplicateSource.listeners.child_added.length, 1);

    const legacyTask = legacySource.addTask('task', {payload: 3});
    const duplicateTask = duplicateSource.addTask('task', {payload: 4});
    const extensionTask = extensionSource.addTask('task', {payload: 5});
    await waitFor(() => legacyTaskUrl !== undefined && !legacyTask.value);
    await waitFor(() => duplicateTaskUrl !== undefined && !duplicateTask.value);
    await waitFor(() => extensionComplete && !extensionTask.value);
    assert.strictEqual(legacyTaskUrl, legacyTask.toString());
    assert.strictEqual(duplicateTaskUrl, duplicateTask.toString());
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
