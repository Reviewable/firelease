import assert from 'node:assert/strict';
import {afterEach, beforeEach, test} from 'node:test';
import {setTimeout as delay} from 'node:timers/promises';
import NodeFire, {type OperationDescriptor} from 'nodefire';

import firelease, {TESTABLES, type WriteMetric} from '../src';
import {asNodeFire, FakeQueueRef, waitFor} from './fake_firebase';

beforeEach(() => {TESTABLES.resetBetweenTests();});
afterEach(() => {TESTABLES.resetBetweenTests();});

function attach(source: FakeQueueRef) {
  firelease.attachWorker(asNodeFire(source), {bufferSize: 1}, () => undefined);
  return asNodeFire(source);
}

function observe(ref: NodeFire, fields: Partial<OperationDescriptor> = {}) {
  TESTABLES.observeWriteOperation({
    ref, method: 'set', args: [], startTime: performance.now(), duration: 100, ...fields
  });
}

function firebaseError(code: string) {
  return Object.assign(new Error(`Firebase: ${code}`), {firebase: {code}});
}

test('successful write samples use measured durations and transaction phase per try', () => {
  const ref = attach(new FakeQueueRef('write-stats', 'queues/jobs'));
  const metrics: WriteMetric[] = [];
  firelease.settings.captureWriteMetrics = metric => {metrics.push(metric);};
  observe(ref, {duration: 0});
  observe(ref, {method: 'update', duration: 100});
  observe(ref, {method: 'remove', duration: 200});
  observe(ref, {method: 'push', duration: 300});
  observe(ref, {
    method: 'transaction', duration: 1100,
    transaction: {outcome: 'commit', prefetchDuration: 900, duration: 200, tries: 4}
  });

  const stats = firelease.stats.databases[0];
  assert.equal(stats.writeSamples, 5);
  assert.equal(stats.writeLatency, 55.49);
  assert.deepEqual(metrics.map(metric => metric.outcome === 'success' && metric.latency),
    [0, 100, 200, 300, 50]);
  assert.deepEqual(metrics[4], {
    ref: ref.root.toString(), app: 'write-stats', method: 'transaction', tries: 4,
    outcome: 'success', duration: 200, latency: 50
  });
});

test('timeouts and failures have separate counts and never affect successful latency', () => {
  const ref = attach(new FakeQueueRef('write-failures', 'queues/jobs'));
  const metrics: WriteMetric[] = [];
  firelease.settings.captureWriteMetrics = metric => {metrics.push(metric);};
  observe(ref, {error: firebaseError('timeout'), duration: 1000});
  observe(ref, {error: firebaseError('permission_denied'), duration: 25});
  assert.equal(firelease.stats.databases[0].writeLatency, null);
  observe(ref, {duration: 10});
  observe(ref, {
    method: 'transaction', error: firebaseError('timeout'), duration: 1500,
    transaction: {outcome: 'error', tries: 2, prefetchDuration: 1000, duration: 500}
  });
  observe(ref, {
    method: 'transaction', error: firebaseError('stuck'),
    transaction: {outcome: 'error', tries: 3, duration: 90}
  });
  assert.deepEqual(JSON.parse(JSON.stringify(firelease.stats.databases)), [{
    ref: ref.root.toString(), app: 'write-failures', writeLatency: 10,
    writeSamples: 1, writeFailures: 2, writeTimeouts: 2
  }]);
  assert.deepEqual(metrics[3], {
    ref: ref.root.toString(), app: 'write-failures', method: 'transaction', tries: 2,
    outcome: 'timeout', elapsed: 500, code: 'timeout'
  });
  for (const metric of metrics.filter(item => item.outcome !== 'success')) {
    assert.equal('latency' in metric, false);
    assert.equal('duration' in metric, false);
  }
});

test('non-writes, prefetch failures, application errors and invalid timing are ignored', () => {
  const ref = attach(new FakeQueueRef('ignored-writes', 'queues/jobs'));
  const metrics: WriteMetric[] = [];
  firelease.settings.captureWriteMetrics = metric => {metrics.push(metric);};
  observe(ref, {method: 'get'});
  observe(ref, {method: 'once'});
  for (const duration of [undefined, NaN, Infinity, -1]) observe(ref, {duration});
  observe(ref, {startTime: undefined});
  observe(ref, {error: new Error('Application failure')});
  for (const transaction of [
    undefined,
    {outcome: 'skip' as const, duration: 100, tries: 1},
    {outcome: 'error' as const, prefetchDuration: 1000, tries: 0},
    {outcome: 'commit' as const, duration: 100, tries: 0},
    {outcome: 'commit' as const, duration: 100, tries: NaN},
    {outcome: 'commit' as const, tries: 1}
  ]) observe(ref, {method: 'transaction', transaction});
  observe(ref, {
    method: 'transaction', error: new Error('Update function threw'),
    transaction: {outcome: 'error', duration: 100, tries: 1}
  });
  observe(ref, {
    method: 'transaction', startTime: undefined, error: firebaseError('timeout'),
    transaction: {outcome: 'error', prefetchDuration: 1000, tries: 0}
  });
  assert.deepEqual(metrics, []);
  assert.equal(firelease.stats.databases[0].writeSamples, 0);
  assert.equal(firelease.stats.databases[0].writeFailures, 0);
  assert.equal(firelease.stats.databases[0].writeTimeouts, 0);
});

test('database stats persist, appear in ping reports, and reset between tests', async () => {
  const source = new FakeQueueRef('write-lifecycle', 'queues/jobs');
  const ref = attach(source);
  attach(source);
  observe(ref, {duration: 20});
  source.root.setConnected(false);
  source.root.setConnected(true);
  await firelease.shutdown();
  firelease.settings.globalMaxConcurrent = 10;
  assert.equal(firelease.stats.databases.length, 1);
  assert.equal(firelease.stats.databases[0].writeSamples, 1);
  let report: unknown;
  firelease.pingQueues(stats => {report = JSON.parse(JSON.stringify(stats));}, 10);
  await waitFor(() => report !== undefined);
  assert.deepEqual((report as typeof firelease.stats).databases, [{
    ref: ref.root.toString(), app: 'write-lifecycle', writeLatency: 20,
    writeSamples: 1, writeFailures: 0, writeTimeouts: 0
  }]);

  const oldStats = firelease.stats.databases[0];
  const startTime = performance.now();
  firelease.settings.captureWriteMetrics = () => undefined;
  TESTABLES.resetBetweenTests();
  assert.equal(firelease.stats.databases.length, 0);
  assert.equal(firelease.settings.captureWriteMetrics, undefined);
  observe(ref);
  attach(source);
  observe(ref, {startTime});
  observe(ref, {duration: 40});
  assert.equal(oldStats.writeSamples, 1);
  assert.equal(firelease.stats.databases[0].writeSamples, 1);
  assert.equal(firelease.stats.databases[0].writeLatency, 40);
});

type RawSnapshot = {ref: RawReference, val: () => unknown};
type TransactionUpdate = (value: unknown) => unknown;
type TransactionComplete = (error: Error | null, committed: boolean, snap?: RawSnapshot) => void;

class RawReference {
  constructor(
    readonly database: {app: {name: string, options: object}},
    readonly url: string,
    readonly path = '',
    readonly operations: {
      write?: () => Promise<void>,
      prefetch?: (callback: (snap: RawSnapshot) => void) => void,
      transaction?: (update: TransactionUpdate, complete: TransactionComplete) => void
    } = {}
  ) {}

  get ref() {return this;}
  get root() {return new RawReference(this.database, this.url, '', this.operations);}
  get key() {return this.path.split('/').pop() || null;}

  child(path: string) {
    return new RawReference(this.database, this.url, `${this.path}/${path}`, this.operations);
  }

  isEqual(other: RawReference) {
    return other.database === this.database && other.url === this.url && other.path === this.path;
  }

  toString() {return this.url + (this.path || '/');}

  on(event: string, callback: (snap: RawSnapshot) => void) {
    if (this.path === '/.info/connected' || this.path === '/.info/serverTimeOffset') {
      callback({ref: this, val: () => this.path === '/.info/connected' ? true : 0});
    } else {
      this.operations.prefetch?.(callback);
    }
  }

  off() {/* No persistent listeners in this raw reference. */}
  async set() {await this.operations.write?.();}
  async update() {await this.operations.write?.();}
  async remove() {await this.operations.write?.();}
  push(value?: unknown) {
    const ref = this.child('pushed');
    if (value === null || value === undefined) return ref;
    const promise = ref.set();
    return Object.assign(ref, {then: promise.then.bind(promise)});
  }

  transaction(update: TransactionUpdate, complete: TransactionComplete) {
    this.operations.transaction?.(update, complete);
    return Promise.resolve();
  }
}

function nativeRef(app: string, url = 'https://write-native.test') {
  const raw = new RawReference({app: {name: app, options: {}}}, url);
  return {raw, ref: new NodeFire(raw as unknown as ConstructorParameters<typeof NodeFire>[0])};
}

function attachNative(ref: NodeFire, key = 'jobs') {
  const source = new FakeQueueRef(ref.database.app.name, `queues/${key}`);
  Object.defineProperty(source, 'root', {value: ref.root});
  attach(source);
}

test('real NodeFire operations are observed once across registrations and resets', async () => {
  const {raw, ref} = nativeRef('write-native');
  for (let iteration = 0; iteration < 3; iteration++) {
    TESTABLES.resetBetweenTests();
    attachNative(ref);
    attachNative(ref, 'other');
    const metrics: WriteMetric[] = [];
    firelease.settings.captureWriteMetrics = metric => {metrics.push(metric);};
    raw.operations.write = () => delay(5);
    const data = ref.child('outside-queues');
    await data.set({value: 1});
    await data.update({value: 2});
    await data.remove();
    await data.push({value: 3});
    await data.push(null);
    assert.deepEqual(metrics.map(metric => metric.method), ['set', 'update', 'remove', 'push']);
    assert.equal(firelease.stats.databases.length, 1);
    assert.equal(firelease.stats.databases[0].writeSamples, 4);
    assert(metrics.every(metric => metric.outcome === 'success' && metric.latency > 0));
  }
});

test('database identity includes the Firebase app and database URL', async () => {
  const first = nativeRef('identity-first');
  const second = nativeRef('identity-second');
  const thirdRaw = new RawReference(first.raw.database, 'https://other-database.test');
  const third = new NodeFire(thirdRaw as unknown as ConstructorParameters<typeof NodeFire>[0]);
  const unregistered = nativeRef('identity-unregistered');
  for (const ref of [first.ref, second.ref, third]) attachNative(ref);
  await first.ref.set(1);
  await second.ref.set(2);
  await second.ref.update({value: 3});
  await third.remove();
  await unregistered.ref.set(4);
  const identities = firelease.stats.databases.map(
    stats => [stats.app, stats.ref, stats.writeSamples]);
  assert.deepEqual(identities, [
    ['identity-first', 'https://write-native.test/', 1],
    ['identity-second', 'https://write-native.test/', 2],
    ['identity-first', 'https://other-database.test/', 1]
  ]);
});

test('real NodeFire transaction metadata excludes prefetch and skips', async () => {
  const {raw, ref} = nativeRef('write-native-transaction');
  attachNative(ref);
  const metrics: WriteMetric[] = [];
  firelease.settings.captureWriteMetrics = metric => {metrics.push(metric);};
  raw.operations.prefetch = callback => {
    void delay(25).then(() => {callback({ref: raw, val: () => 1});});
  };
  raw.operations.transaction = (update, complete) => {
    update(1);
    const value = update(2);
    void delay(5).then(() => {complete(null, value !== undefined, {ref: raw, val: () => value});});
  };
  const transaction = ref.transaction(value => Number(value) + 1);
  assert.equal(await transaction, 3);
  assert(transaction.transaction.prefetchDuration! >= 20);
  assert.equal(metrics.length, 1);
  assert.deepEqual(metrics[0], {
    ref: ref.toString(), app: 'write-native-transaction', method: 'transaction', tries: 2,
    outcome: 'success', duration: transaction.transaction.duration,
    latency: transaction.transaction.duration! / 2
  });
  await ref.transaction(() => undefined, {prefetchValue: false});
  await assert.rejects(ref.transaction(() => {throw new Error('User callback');},
    {prefetchValue: false}), /User callback/);
  assert.equal(metrics.length, 1);
});

test('real NodeFire timeout is counted once even after the underlying write finishes', async () => {
  const {raw, ref} = nativeRef('write-native-timeout');
  attachNative(ref);
  const metrics: WriteMetric[] = [];
  firelease.settings.captureWriteMetrics = metric => {metrics.push(metric);};
  let finish!: () => void;
  raw.operations.write = () => new Promise<void>(resolve => {finish = resolve;});
  await assert.rejects(ref.set(1, {timeout: 10}), /timeout/);
  finish();
  await delay(0);
  assert.equal(metrics.length, 1);
  assert.equal(metrics[0].outcome, 'timeout');
  assert.equal(firelease.stats.databases[0].writeSamples, 0);
  assert.equal(firelease.stats.databases[0].writeLatency, null);
  assert.equal(firelease.stats.databases[0].writeTimeouts, 1);

  raw.operations.prefetch = () => undefined;
  await assert.rejects(ref.transaction(() => 2, {timeout: 10}), /timeout/);
  assert.equal(metrics.length, 1);

  let complete!: TransactionComplete;
  raw.operations.transaction = (update, callback) => {update(1); complete = callback;};
  const transaction = ref.transaction(() => 2, {prefetchValue: false, timeout: 10});
  await assert.rejects(transaction, /timeout/);
  assert.deepEqual(metrics[1], {
    ref: ref.toString(), app: 'write-native-timeout', method: 'transaction', tries: 1,
    outcome: 'timeout', elapsed: transaction.transaction.duration, code: 'timeout'
  });
  complete(null, true, {ref: raw, val: () => 2});
  await delay(0);
  assert.equal(metrics.length, 2);
  assert.equal(firelease.stats.databases[0].writeTimeouts, 2);
  assert.equal(firelease.stats.databases[0].writeLatency, null);
});

test('reporting failures cannot change real NodeFire operation results', async t => {
  const {raw, ref} = nativeRef('write-native-capture-error');
  attachNative(ref);
  let capturedErrors = 0;
  firelease.settings.captureWriteMetrics = () => {throw new Error('Metric callback');};
  firelease.settings.captureError = error => {
    assert.equal(error.firelease?.phase, 'write-metric');
    capturedErrors++;
    throw new Error('Error callback');
  };
  t.mock.method(console, 'error', () => {throw new Error('Console failed');});
  await ref.set(1);
  raw.operations.write = () => Promise.reject(new Error('permission_denied'));
  await assert.rejects(ref.remove(), /Firebase: permission_denied/);
  assert.equal(capturedErrors, 2);
  assert.equal(firelease.stats.databases[0].writeSamples, 1);
  assert.equal(firelease.stats.databases[0].writeFailures, 1);
});
