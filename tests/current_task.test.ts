import assert from 'node:assert/strict';
import {test} from 'node:test';

import firelease, {TESTABLES, getCurrentTask, type WorkerItem} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => {resolve = done;});
  return {promise, resolve};
}

test('current tasks remain isolated across asynchronous and concurrent workers', async () => {
  TESTABLES.resetBetweenTests();
  const gates = {a: deferred(), b: deferred()};
  const afterCompletion = deferred();
  try {
    const source = new FakeQueueRef('current-task-database', 'queues/jobs');
    const started = new Map<string, WorkerItem>();
    const lateCallbacks: Promise<void>[] = [];
    firelease.attachWorker(asNodeFire(source), {maxConcurrent: 2}, async item => {
      const id = item.id as 'a' | 'b';
      assert.equal(firelease.getCurrentTask(), item);
      assert.equal(getCurrentTask(), item);
      await Promise.resolve();
      started.set(id, item);
      lateCallbacks.push(afterCompletion.promise.then(() => {
        assert.equal(getCurrentTask(), undefined);
      }));
      await gates[id].promise;
      assert.equal(getCurrentTask(), item);
      await firelease.extendLease(getCurrentTask()!, 60000);
    });
    const a = source.addTask('a', {id: 'a'});
    const b = source.addTask('b', {id: 'b'});
    await waitFor(() => started.size === 2);
    assert.equal(getCurrentTask(), undefined);
    assert.notEqual(started.get('a'), started.get('b'));
    gates.b.resolve();
    await waitFor(() => !b.value);
    assert.ok(a.value);
    gates.a.resolve();
    await waitFor(() => !a.value);
    afterCompletion.resolve();
    await Promise.all(lateCallbacks);
    assert.equal(getCurrentTask(), undefined);
  } finally {
    gates.a.resolve();
    gates.b.resolve();
    afterCompletion.resolve();
    TESTABLES.resetBetweenTests();
  }
});

for (const asynchronous of [false, true]) {
  test(`current task is cleared after ${asynchronous ? 'rejection' : 'throw'}`, async () => {
    TESTABLES.resetBetweenTests();
    const afterFailure = deferred();
    try {
      const source = new FakeQueueRef('failed-current-task-database', 'queues/jobs');
      const expectedError = new Error('worker failed');
      let capturedError: Error | undefined;
      let lateCallback: Promise<void> | undefined;
      firelease.settings.captureError = error => {capturedError = error;};
      firelease.attachWorker(asNodeFire(source), {minLease: '1h'}, item => {
        assert.equal(getCurrentTask(), item);
        lateCallback = afterFailure.promise.then(() => {
          assert.equal(getCurrentTask(), undefined);
        });
        if (asynchronous) return Promise.reject(expectedError);
        throw expectedError;
      });
      source.addTask('failure', {});
      await waitFor(() => capturedError !== undefined);
      assert.equal(capturedError, expectedError);
      afterFailure.resolve();
      await lateCallback;
      assert.equal(getCurrentTask(), undefined);
    } finally {
      afterFailure.resolve();
      TESTABLES.resetBetweenTests();
    }
  });
}
