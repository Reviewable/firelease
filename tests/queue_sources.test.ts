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
      {bufferSize: Infinity, maxConcurrent: 1, minLease: '1s'},
      async item => {
        active++;
        maxActive = Math.max(maxActive, active);
        calls.push(item.$ref.toString());
        leaseTimesRemaining.push(item.$leaseTimeRemaining);
        if (calls.length === 1) {
          await firstBlocked;
          active--;
          return;
        }
        active--;
      }
    );

    const firstTask = firstSource.addTask('task', {payload: 1});
    const secondTask = secondSource.addTask('task', {payload: 2});
    const blacklistedTask = secondSource.addTask('blacklisted', {payload: 3});

    await waitFor(() => calls.length === 1);
    assert.deepStrictEqual(firelease.listTasksInProgress(), [firstTask.toString()]);
    assert.strictEqual(firelease.blacklist(blacklistedTask.toString()), true);
    assert.strictEqual(firelease.blacklist(blacklistedTask.toString()), false);
    await new Promise(resolve => {setTimeout(resolve, 50);});
    assert.strictEqual(calls.length, 1, 'logical maxConcurrent must cover every source');

    releaseFirst();
    await waitFor(() => calls.length === 2 && !firstTask.value && !secondTask.value);

    assert.deepStrictEqual(calls, [firstTask.toString(), secondTask.toString()]);
    assert(leaseTimesRemaining.every(Number.isFinite));
    assert(leaseTimesRemaining.every(time => time > 0));
    assert.strictEqual(maxActive, 1);
    assert.deepStrictEqual(firelease.listTasksInProgress(), []);

    const legacySource = new FakeQueueRef('legacy-database', 'queues/legacy');
    let legacyTaskUrl: string | undefined;
    firelease.attachWorker(asNodeFire(legacySource), item => {
      legacyTaskUrl = item.$ref.toString();
      assert(Number.isFinite(item.$leaseTimeRemaining));
    });
    const legacyTask = legacySource.addTask('task', {payload: 3});
    await waitFor(() => legacyTaskUrl !== undefined && !legacyTask.value);
    assert.strictEqual(legacyTaskUrl, legacyTask.toString());

    const duplicateSource = new FakeQueueRef('duplicate-database', 'queues/duplicate');
    let duplicateTaskUrl: string | undefined;
    firelease.attachWorker(
      [asNodeFire(duplicateSource), asNodeFire(duplicateSource)],
      {bufferSize: Infinity},
      item => {
        duplicateTaskUrl = item.$ref.toString();
      }
    );
    await waitFor(() => duplicateSource.listeners.child_added?.length === 1);
    assert.strictEqual(duplicateSource.listeners.child_added.length, 1);
    const duplicateTask = duplicateSource.addTask('task', {payload: 4});
    await waitFor(() => duplicateTaskUrl !== undefined && !duplicateTask.value);
    assert.strictEqual(duplicateTaskUrl, duplicateTask.toString());

    const extensionSource = new FakeQueueRef('extension-database', 'queues/extension');
    // Exercise the exact-boundary path where the first extension already satisfies the second.
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
    const extensionTask = extensionSource.addTask('task', {payload: 5});
    await waitFor(() => extensionComplete && !extensionTask.value);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
