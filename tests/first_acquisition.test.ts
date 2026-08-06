import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

test('firstAcquisition is worker-local and only set on the first lease', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('first-acquisition-database', 'queues/jobs');
    const flags: (true | undefined)[] = [];
    let firstDescriptor: PropertyDescriptor | undefined;
    let releaseFirst!: () => void;
    const firstBlocked = new Promise<void>(resolve => {releaseFirst = resolve;});
    firelease.attachWorker(
      asNodeFire(source),
      {minLease: 100, maxLease: 100},
      async item => {
        flags.push(item._lease.firstAcquisition);
        if (flags.length === 1) {
          firstDescriptor = Object.getOwnPropertyDescriptor(item._lease, 'firstAcquisition');
          await firstBlocked;
          return firelease.RETRY;
        }
      }
    );

    const task = source.addTask('task', {payload: 1});
    await waitFor(() => flags.length === 1);

    assert.deepStrictEqual(firstDescriptor, {
      value: true, writable: false, enumerable: false, configurable: false
    });
    assert.strictEqual(task.value?._lease?.firstAcquisition, undefined);

    releaseFirst();
    await waitFor(() => flags.length === 2 && !task.value);
    assert.deepStrictEqual(flags, [true, undefined]);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('an existing initial value is not treated as a first acquisition', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('existing-initial-database', 'queues/jobs');
    let workerCalled = false;
    let firstAcquisition: true | undefined;
    firelease.attachWorker(asNodeFire(source), item => {
      workerCalled = true;
      firstAcquisition = item._lease.firstAcquisition;
    });

    const task = source.addTask('task', {payload: 1, _lease: {initial: 0, expiry: 0}});
    await waitFor(() => workerCalled && !task.value);
    assert.strictEqual(firstAcquisition, undefined);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});
