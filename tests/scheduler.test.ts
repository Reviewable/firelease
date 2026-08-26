import assert from 'node:assert';
import {test} from 'node:test';

import firelease, {TESTABLES, type FireleaseError} from '../src';
import {asNodeFire, FakeDatabaseRoot, FakeQueueRef, waitFor} from './fake_firebase';

function createBlockingWorker(calls: string[]) {
  let release!: () => void;
  const blocked = new Promise<void>(resolve => {release = resolve;});
  return {
    release,
    worker: async item => {
      calls.push(item.$ref.toString());
      await blocked;
    }
  };
}

test('scheduler settings and limits validate eagerly', () => {
  TESTABLES.resetBetweenTests();
  try {
    for (const priority of [0, Infinity, NaN]) {
      assert.throws(
        () => firelease.attachWorker(
          asNodeFire(new FakeQueueRef('invalid-priority', 'queues/jobs')),
          {priority},
          () => undefined),
        /Queue priority must be a positive finite number/);
    }
    assert.throws(
      () => {firelease.settings.writeLatencyTarget = Infinity;},
      /writeLatencyTarget must be a positive finite duration/);
    assert.throws(
      () => {firelease.settings.initialTaskStartRate = 100;},
      /initialTaskStartRate cannot exceed maxTaskStartRate/);
    assert.throws(
      () => {firelease.settings.taskStartRateMinSamples = 1.5;},
      /taskStartRateMinSamples must be a positive finite integer/);
    assert.throws(
      () => {firelease.limitTasksInFlight(1.5);},
      /Task in-flight limit must be a non-negative finite integer/);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('startup loads every source before dispatching any task', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const first = new FakeQueueRef('startup-first', 'queues/first');
    const second = new FakeQueueRef('startup-second', 'queues/second');
    first.addTask('task', {payload: 1});
    second.addTask('task', {payload: 2});
    second.deferNextValue = true;
    const calls: string[] = [];
    firelease.attachWorker(asNodeFire(first), item => {calls.push(item.$ref.toString());});
    firelease.attachWorker(asNodeFire(second), item => {calls.push(item.$ref.toString());});

    assert.strictEqual(firelease.state, 'initializing');
    const startup = firelease.start();
    assert.strictEqual(firelease.start(), startup);
    await waitFor(() => second.deferredValueCallbacks.length === 1);
    await new Promise(resolve => {setTimeout(resolve, 30);});
    assert.deepStrictEqual(calls, []);
    assert.strictEqual(firelease.state, 'initializing');

    second.releaseDeferredValues();
    await startup;
    assert.strictEqual(firelease.state, 'running');
    assert.throws(
      () => {firelease.attachWorker(asNodeFire(first), () => undefined);},
      /Cannot attach a worker after Firelease has started/);
    await waitFor(() => calls.length === 2);
    assert.deepStrictEqual(
      calls.sort(), [first.child('task').toString(), second.child('task').toString()].sort());
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('a reconnect reloads its Firebase group while unrelated queues keep running', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const sharedRoot = new FakeDatabaseRoot('shared-reconnect');
    const first = new FakeQueueRef('shared-reconnect', 'queues/first', sharedRoot);
    const second = new FakeQueueRef('shared-reconnect', 'queues/second', sharedRoot);
    const unrelated = new FakeQueueRef('unrelated-reconnect', 'queues/third');
    const calls: string[] = [];
    for (const source of [first, second, unrelated]) {
      firelease.attachWorker(asNodeFire(source), item => {calls.push(item.$ref.toString());});
    }
    await firelease.start();
    assert.deepStrictEqual(
      firelease.stats.databases.map(database => database.ref),
      [sharedRoot.toString(), unrelated.root.toString()]
    );

    first.deferNextValue = true;
    second.deferNextValue = true;
    sharedRoot.setConnected(false);
    sharedRoot.setConnected(true);
    await waitFor(() => first.deferredValueCallbacks.length === 1);
    assert.strictEqual(firelease.state, 'initializing');
    first.addTask('blocked', {payload: 1});
    const unrelatedTask = unrelated.addTask('available', {payload: 2});
    await waitFor(() => calls.includes(unrelatedTask.toString()));
    assert(!calls.includes(first.child('blocked').toString()));

    first.releaseDeferredValues();
    await waitFor(() => second.deferredValueCallbacks.length === 1);
    second.releaseDeferredValues();
    await waitFor(() => firelease.state === 'running');
    await waitFor(() => calls.includes(first.child('blocked').toString()));
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('in-flight limits compose, report wait timeouts, and pause safely', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('flight-limit', 'queues/jobs');
    let calls = 0;
    let releaseFirst!: () => void;
    const firstBlocked = new Promise<void>(resolve => {releaseFirst = resolve;});
    firelease.attachWorker(asNodeFire(source), async () => {
      calls++;
      if (calls === 1) await firstBlocked;
    });
    firelease.limitTasksInFlight(1);
    await firelease.start();
    source.addTask('first', {payload: 1});
    source.addTask('second', {payload: 2});
    await waitFor(() => calls === 1);

    const releasePause = firelease.limitTasksInFlight(0, asNodeFire(source));
    assert.strictEqual(firelease.state, 'paused');
    await assert.rejects(
      firelease.waitForTasksInFlight(20, asNodeFire(source)),
      (error: FireleaseError) => {
        assert.strictEqual(error.message, 'Firelease tasks-in-flight wait timed out');
        assert.deepStrictEqual(error.firelease, {
          code: 'tasks-in-flight-wait-timeout',
          count: 1,
          limit: 0,
          timeout: 20,
          scope: source.toString()
        });
        return true;
      }
    );
    releaseFirst();
    await firelease.waitForTasksInFlight(100, asNodeFire(source));
    await new Promise(resolve => {setTimeout(resolve, 20);});
    assert.strictEqual(calls, 1);
    releasePause();
    releasePause();
    await waitFor(() => calls === 2);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('write latency controls database rate and reconnect resets it', async () => {
  TESTABLES.resetBetweenTests();
  try {
    firelease.settings.writeLatencyTarget = 100;
    firelease.settings.taskStartRateAdjustmentInterval = 1;
    firelease.settings.taskStartRateMinSamples = 2;
    const root = new FakeDatabaseRoot('rate-control');
    const source = new FakeQueueRef('rate-control', 'queues/jobs', root);
    firelease.attachWorker(asNodeFire(source), () => undefined);
    await firelease.start();
    const databaseStats = firelease.stats.databases[0];
    assert.strictEqual(databaseStats.taskStartRate, 5);
    TESTABLES.observeWriteOperation({
      ref: asNodeFire(source), method: 'get', args: [],
      startTime: performance.now(), duration: 1000
    });
    TESTABLES.observeWriteOperation({
      ref: asNodeFire(source), method: 'transaction', args: [],
      startTime: performance.now(), duration: 1000, transaction: {outcome: 'skip', tries: 1}
    });
    assert.strictEqual(databaseStats.writeSamples, 0);

    await new Promise(resolve => {setTimeout(resolve, 3);});
    for (let index = 0; index < 2; index++) {
      TESTABLES.observeWriteOperation({
        ref: asNodeFire(source), method: 'set', args: [],
        startTime: performance.now(), duration: 1000
      });
    }
    assert.strictEqual(databaseStats.taskStartRate, 2.5);
    assert.strictEqual(databaseStats.writeSamples, 2);
    assert(databaseStats.writeLatency! > 100);

    await new Promise(resolve => {setTimeout(resolve, 3);});
    const staleStartTime = performance.now() - 1000;
    for (let index = 0; index < 2; index++) {
      TESTABLES.observeWriteOperation({
        ref: asNodeFire(source), method: 'set', args: [],
        startTime: staleStartTime, duration: 10
      });
    }
    assert.strictEqual(databaseStats.taskStartRate, 2.5);
    for (let index = 0; index < 2; index++) {
      TESTABLES.observeWriteOperation({
        ref: asNodeFire(source), method: 'set', args: [],
        startTime: performance.now(), duration: 10
      });
    }
    assert.strictEqual(databaseStats.taskStartRate, 3.5);

    root.setConnected(false);
    root.setConnected(true);
    await waitFor(() => firelease.state === 'running');
    assert.strictEqual(databaseStats.taskStartRate, 5);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('transaction write latency uses the average duration per try', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('transaction-latency', 'queues/jobs');
    firelease.attachWorker(asNodeFire(source), () => undefined);
    await firelease.start();
    const databaseStats = firelease.stats.databases[0];

    TESTABLES.observeWriteOperation({
      ref: asNodeFire(source), method: 'transaction', args: [],
      startTime: performance.now(), duration: 1000,
      transaction: {outcome: 'commit', tries: 4}
    });

    assert.strictEqual(databaseStats.writeSamples, 1);
    assert.strictEqual(databaseStats.writeLatency, 250);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('the initial task start rate is enforced per database', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('initial-rate', 'queues/jobs');
    const starts: number[] = [];
    source.beforeTransaction = () => {starts.push(performance.now());};
    for (let index = 0; index < 3; index++) source.addTask(`task-${index}`, {payload: index});
    firelease.attachWorker(asNodeFire(source), () => undefined);
    await firelease.start();
    await waitFor(() => starts.length === 3, 1000);
    assert(starts[1] - starts[0] >= 150);
    assert(starts[2] - starts[1] >= 150);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('ref-scoped limits let an admissible background queue run', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const root = new FakeDatabaseRoot('scoped-limits');
    const parent = new FakeQueueRef('scoped-limits', 'queues/group', root);
    const descendant = new FakeQueueRef('scoped-limits', 'queues/group/jobs', root);
    const sibling = new FakeQueueRef('scoped-limits', 'queues/other', root);
    const calls: string[] = [];
    firelease.attachWorker(asNodeFire(descendant), item => {calls.push(item.$ref.toString());});
    firelease.attachWorker(
      asNodeFire(sibling), {priority: 0.5}, item => {calls.push(item.$ref.toString());});
    const release = firelease.limitTasksInFlight(0, asNodeFire(parent));
    await firelease.start();

    const blockedTask = descendant.addTask('blocked', {payload: 1});
    const siblingTask = sibling.addTask('available', {payload: 2});
    await waitFor(() => calls.includes(siblingTask.toString()));
    assert(!calls.includes(blockedTask.toString()));
    assert.strictEqual(firelease.state, 'running');
    release();
    await waitFor(() => calls.includes(blockedTask.toString()));
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('shutdown drains admitted tasks without preventing lease extension', async () => {
  TESTABLES.resetBetweenTests();
  try {
    const source = new FakeQueueRef('shutdown-drain', 'queues/jobs');
    source.fixedNow = Date.now();
    let activeItem: Parameters<typeof firelease.extendLease>[0] | undefined;
    let calls = 0;
    let releaseWorker!: () => void;
    const workerBlocked = new Promise<void>(resolve => {releaseWorker = resolve;});
    firelease.attachWorker(asNodeFire(source), {minLease: '1s'}, async item => {
      calls++;
      activeItem = item;
      await workerBlocked;
    });
    await firelease.start();
    source.addTask('active', {payload: 1});
    await waitFor(() => activeItem !== undefined);

    const shutdown = firelease.shutdown();
    assert.strictEqual(firelease.state, 'shutting-down');
    await firelease.extendLease(activeItem!, '2s');
    source.addTask('blocked', {payload: 2});
    await new Promise(resolve => {setTimeout(resolve, 20);});
    assert.strictEqual(calls, 1);
    releaseWorker();
    await shutdown;
    assert.strictEqual(calls, 1);
  } finally {
    TESTABLES.resetBetweenTests();
  }
});

test('foreground queues are selected proportionally without admitting background work',
  async () => {
    TESTABLES.resetBetweenTests();
    const calls: string[] = [];
    const {release, worker} = createBlockingWorker(calls);
    try {
      TESTABLES.setSchedulerRandom(() => 0.5);
      const background = new FakeQueueRef('weighted-background', 'queues/background');
      const foregroundLow = new FakeQueueRef('weighted-low', 'queues/low');
      const foregroundHigh = new FakeQueueRef('weighted-high', 'queues/high');
      firelease.attachWorker(asNodeFire(background), {priority: 0.5}, worker);
      firelease.attachWorker(asNodeFire(foregroundLow), {priority: 1}, worker);
      firelease.attachWorker(asNodeFire(foregroundHigh), {priority: 3}, worker);
      firelease.limitTasksInFlight(1);
      background.addTask('task', {payload: 'background'});
      foregroundLow.addTask('task', {payload: 'low'});
      foregroundHigh.addTask('task', {payload: 'high'});
      await firelease.start();
      await waitFor(() => calls.length === 1);
      assert.deepStrictEqual(calls, [foregroundHigh.child('task').toString()]);
      assert.deepStrictEqual(firelease.stats.queues.map(queue => queue.priority), [0.5, 1, 3]);
      firelease.limitTasksInFlight(0);
      release();
      await firelease.waitForTasksInFlight(100);
    } finally {
      release();
      TESTABLES.resetBetweenTests();
    }
  });

test('background queues use their relative priorities when no foreground work is admissible',
  async () => {
    TESTABLES.resetBetweenTests();
    const calls: string[] = [];
    const {release, worker} = createBlockingWorker(calls);
    try {
      TESTABLES.setSchedulerRandom(() => 0.2);
      const foreground = new FakeQueueRef('background-empty-foreground', 'queues/foreground');
      const backgroundLow = new FakeQueueRef('background-low', 'queues/low');
      const backgroundHigh = new FakeQueueRef('background-high', 'queues/high');
      firelease.attachWorker(asNodeFire(foreground), {priority: 1}, worker);
      firelease.attachWorker(asNodeFire(backgroundLow), {priority: 0.1}, worker);
      firelease.attachWorker(asNodeFire(backgroundHigh), {priority: 0.9}, worker);
      firelease.limitTasksInFlight(1);
      backgroundLow.addTask('task', {payload: 'low'});
      backgroundHigh.addTask('task', {payload: 'high'});
      await firelease.start();
      await waitFor(() => calls.length === 1);
      assert.deepStrictEqual(calls, [backgroundHigh.child('task').toString()]);
      firelease.limitTasksInFlight(0);
      release();
      await firelease.waitForTasksInFlight(100);
    } finally {
      release();
      TESTABLES.resetBetweenTests();
    }
  });
