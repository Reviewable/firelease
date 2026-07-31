'use strict';

const assert = require('assert');
const firelease = require('..');


class FakeSnapshot {
  constructor(ref) {
    this.ref = ref;
  }

  val() {
    return clone(this.ref.value);
  }
}


class FakeDatabaseRoot {
  constructor(name) {
    this.name = name;
    this.database = {
      app: {name},
      goOffline() {/* Do nothing. */},
      goOnline() {/* Do nothing. */}
    };
  }

  child(path) {
    assert.strictEqual(path, '.info/connected');
    return {
      on: (event, callback, cancelCallback, context) => {
        assert.strictEqual(event, 'value');
        callback.call(context, {val: () => true});
      }
    };
  }

  isEqual(other) {
    return other === this;
  }
}


class FakeTaskRef {
  constructor(queueRef, key) {
    this.queueRef = queueRef;
    this.key = key;
    this.value = null;
  }

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

  transaction(update) {
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

  child(path) {
    return {
      set: value => {
        if (path === '_lease/busy' && this.value && this.value._lease) {
          this.value._lease.busy = value;
        }
        return Promise.resolve();
      }
    };
  }
}


class FakeQueueRef {
  constructor(databaseName, path) {
    this.databaseRoot = new FakeDatabaseRoot(databaseName);
    this.database = this.databaseRoot.database;
    this.path = path;
    this.key = path.split('/').pop();
    this.listeners = {};
    this.tasks = {};
  }

  get root() {
    return this.databaseRoot;
  }

  get now() {
    return Date.now();
  }

  toString() {
    return `https://${this.databaseRoot.name}.example.test/${this.path}`;
  }

  orderByChild(path) {
    assert.strictEqual(path, '_lease/expiry');
    return this;
  }

  limitToFirst() {
    return this;
  }

  get() {
    return Promise.resolve(null);
  }

  child(key) {
    if (!this.tasks[key]) this.tasks[key] = new FakeTaskRef(this, key);
    return this.tasks[key];
  }

  on(event, callback, cancelCallback, context) {
    this.listeners[event] = this.listeners[event] || [];
    this.listeners[event].push({callback, context});
  }

  off(event, callback, context) {
    this.listeners[event] = (this.listeners[event] || []).filter(
      listener => listener.callback !== callback || listener.context !== context);
  }

  addTask(key, value) {
    const ref = this.child(key);
    ref.value = clone(value);
    this.emit('child_added', new FakeSnapshot(ref));
    return ref;
  }

  emit(event, snapshot) {
    for (const listener of this.listeners[event] || []) {
      listener.callback.call(listener.context, snapshot);
    }
  }
}


function clone(value) {
  return value === null || value === undefined ? value : JSON.parse(JSON.stringify(value));
}

function waitFor(predicate, timeout = 2000) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
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
  const firstSource = new FakeQueueRef('first-database', 'queues/jobs');
  const secondSource = new FakeQueueRef('second-database', 'other/jobs');
  const calls = [];
  const leaseTimesRemaining = [];
  let active = 0;
  let maxActive = 0;
  let releaseFirst;
  const firstBlocked = new Promise(resolve => {releaseFirst = resolve;});

  firelease.attachWorker(
    [firstSource, secondSource],
    {bufferSize: Infinity, maxConcurrent: 1, minLease: '1s'},
    item => {
      active++;
      maxActive = Math.max(maxActive, active);
      calls.push(item.$ref.toString());
      leaseTimesRemaining.push(item.$leaseTimeRemaining);
      if (calls.length === 1) {
        return firstBlocked.then(() => {active--;});
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
  let legacyTaskUrl;
  firelease.attachWorker(legacySource, item => {
    legacyTaskUrl = item.$ref.toString();
    assert(Number.isFinite(item.$leaseTimeRemaining));
  });
  const legacyTask = legacySource.addTask('task', {payload: 3});
  await waitFor(() => legacyTaskUrl && !legacyTask.value);
  assert.strictEqual(legacyTaskUrl, legacyTask.toString());

  const duplicateSource = new FakeQueueRef('duplicate-database', 'queues/duplicate');
  let duplicateTaskUrl;
  firelease.attachWorker([duplicateSource, duplicateSource], {bufferSize: Infinity}, item => {
    duplicateTaskUrl = item.$ref.toString();
  });
  assert.strictEqual(duplicateSource.listeners.child_added.length, 1);
  const duplicateTask = duplicateSource.addTask('task', {payload: 4});
  await waitFor(() => duplicateTaskUrl && !duplicateTask.value);
  assert.strictEqual(duplicateTaskUrl, duplicateTask.toString());
}


run().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
