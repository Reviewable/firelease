import assert from 'node:assert';
import type NodeFire from 'nodefire';

import firelease from '../src';

interface FakeLease {
  [key: string]: unknown;
}

interface FakeTaskValue {
  _lease?: FakeLease;
  [key: string]: unknown;
}

type Listener = {callback: (snapshot: FakeSnapshot) => void, context?: unknown};


class FakeSnapshot {
  constructor(readonly ref: FakeTaskRef) {}

  val() {
    return clone(this.ref.value);
  }
}


class FakeDatabaseRoot {
  readonly database;

  constructor(readonly name: string) {
    this.database = {
      app: {name},
      goOffline() {/* Do nothing. */},
      goOnline() {/* Do nothing. */}
    };
  }

  child(path: string) {
    assert.strictEqual(path, '.info/connected');
    return {
      on: (
        event: string,
        callback: (snapshot: {val(): boolean}) => void,
        cancelCallback?: (error: Error) => void,
        context?: unknown
      ) => {
        assert.strictEqual(event, 'value');
        callback.call(context, {val: () => true});
      }
    };
  }

  isEqual(other: unknown) {
    return other === this;
  }
}


class FakeTaskRef {
  value: FakeTaskValue | null = null;

  constructor(readonly queueRef: FakeQueueRef, readonly key: string) {}

  get root() {
    return this.queueRef.root;
  }

  get database() {
    return this.queueRef.database;
  }

  get now() {
    return this.queueRef.now;
  }

  toString() {
    return `${this.queueRef}/${this.key}`;
  }

  transaction(
    update: (value: FakeTaskValue | null) => FakeTaskValue | null | undefined
  ) {
    const updated = update(clone(this.value));
    if (updated !== undefined) this.value = clone(updated);
    return Promise.resolve(clone(this.value));
  }

  get() {
    return Promise.resolve(clone(this.value));
  }

  remove() {
    this.value = null;
    this.queueRef.emit('child_removed', new FakeSnapshot(this));
    return Promise.resolve();
  }

  child(path: string) {
    return {
      set: (value: unknown) => {
        if (path === '_lease/busy' && this.value && this.value._lease) {
          this.value._lease.busy = value;
        }
        return Promise.resolve();
      }
    };
  }
}


class FakeQueueRef {
  readonly databaseRoot: FakeDatabaseRoot;
  readonly database;
  readonly key: string;
  readonly listeners: Record<string, Listener[]> = {};
  readonly tasks: Record<string, FakeTaskRef> = {};
  fixedNow?: number;

  constructor(databaseName: string, readonly path: string) {
    this.databaseRoot = new FakeDatabaseRoot(databaseName);
    this.database = this.databaseRoot.database;
    this.key = path.split('/').pop()!;
  }

  get root() {
    return this.databaseRoot;
  }

  get now() {
    return this.fixedNow ?? Date.now();
  }

  toString() {
    return `https://${this.databaseRoot.name}.example.test/${this.path}`;
  }

  orderByChild(path: string) {
    assert.strictEqual(path, '_lease/expiry');
    return this;
  }

  limitToFirst() {
    return this;
  }

  get() {
    return Promise.resolve(null);
  }

  child(key: string) {
    if (!this.tasks[key]) this.tasks[key] = new FakeTaskRef(this, key);
    return this.tasks[key];
  }

  on(
    event: string,
    callback: Listener['callback'],
    cancelCallback?: (error: Error) => void,
    context?: unknown
  ) {
    this.listeners[event] ??= [];
    this.listeners[event].push({callback, context});
  }

  off(event: string, callback: Listener['callback'], context?: unknown) {
    this.listeners[event] = (this.listeners[event] ?? []).filter(
      listener => listener.callback !== callback || listener.context !== context);
  }

  addTask(key: string, value: FakeTaskValue) {
    const ref = this.child(key);
    ref.value = clone(value);
    this.emit('child_added', new FakeSnapshot(ref));
    return ref;
  }

  emit(event: string, snapshot: FakeSnapshot) {
    for (const listener of this.listeners[event] ?? []) {
      listener.callback.call(listener.context, snapshot);
    }
  }
}


function clone<T>(value: T): T {
  return value === null || value === undefined ? value : JSON.parse(JSON.stringify(value));
}

function waitFor(predicate: () => unknown, timeout = 2000) {
  const start = Date.now();
  return new Promise<void>((resolve, reject) => {
    function check() {
      if (predicate()) {
        resolve();
      } else if (Date.now() - start >= timeout) {
        reject(new Error('Timed out waiting for condition'));
      } else {
        setTimeout(check, 10);
      }
    }
    check();
  });
}


async function run() {
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
}


function asNodeFire(ref: FakeQueueRef) {
  return ref as unknown as NodeFire;
}


void run().catch(error => {
  console.error(error);
  process.exit(1);
});
