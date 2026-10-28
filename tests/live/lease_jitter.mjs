// Deliberately excluded from the unit-test glob. Requires explicit --live and test credentials.
import assert from 'node:assert/strict';
import {fork} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {parseArgs} from 'node:util';
import {fileURLToPath, URL} from 'node:url';
import process from 'node:process';
import console from 'node:console';
import {setTimeout, clearTimeout} from 'node:timers';
import {performance} from 'node:perf_hooks';
import {setTimeout as sleep} from 'node:timers/promises';

const require = createRequire(import.meta.url);
// Resolve through NodeFire so the harness uses the very same Admin SDK as the library.
const nodefireRequire = createRequire(require.resolve('nodefire'));
const {initializeApp, applicationDefault, deleteApp} = nodefireRequire('firebase-admin/app');
const {getDatabase, ServerValue} = nodefireRequire('firebase-admin/database');
const NodeFire = require('nodefire').default;
const {default: firelease, TESTABLES} = require('../../src/index.ts');
const {values} = parseArgs({options: {
  live: {type: 'boolean'}, worker: {type: 'string'}, database: {type: 'string'},
  workers: {type: 'string', default: '8'}, tasks: {type: 'string', default: '120'},
  output: {type: 'string'}
}});
const databaseURL = values.database ?? process.env.FIRELEASE_TEST_DATABASE_URL;
if (!values.live || !databaseURL || !process.env.GOOGLE_APPLICATION_CREDENTIALS) {
  throw new Error(
    'Use --live --database <test URL> and GOOGLE_APPLICATION_CREDENTIALS. No database accessed.');
}
const url = new URL(databaseURL);
assert(url.protocol === 'https:' && /(?:^|-)test(?:-|\.)/.test(url.hostname) &&
  url.hostname.endsWith('.firebaseio.com') && url.pathname === '/',
'Only an explicitly named test Firebase database root is allowed');
const workers = Number(values.workers), taskCount = Number(values.tasks);
assert(Number.isInteger(workers) && workers >= 2 && workers <= 20, 'Use 2–20 workers');
assert(Number.isInteger(taskCount) && taskCount >= 20 && taskCount <= 300,
  'Use 20–300 tasks per mode');
const credential = applicationDefault();
await credential.getAccessToken(); // Fail before spawning workers or opening database connections.
const app = initializeApp({credential, databaseURL});
const database = getDatabase(app);
const deadline = setTimeout(() => {
  console.error('Live harness exceeded 8 minutes');
  process.exit(1);
}, 480_000);
deadline.unref();

async function waitFor(predicate, description, timeout = 15_000) {
  const started = performance.now();
  while (!predicate()) {
    if (performance.now() - started > timeout) throw new Error(`Timed out: ${description}`);
    await sleep(10);
  }
}

if (typeof values.worker === 'string') {
  let stopped = false;
  process.on('disconnect', () => {
    if (stopped) process.exit(0);
    void deleteApp(app).finally(() => {process.exit(0);});
  });
  process.on('message', message => {
    void handleCommand(message).catch(error => {
      process.send({id: message.id, error: error.message});
    });
  });
  async function handleCommand(message) {
    if (message.command === 'configure') {
      TESTABLES.resetBetweenTests();
      firelease.settings.captureError = error => {
        process.send({error: error.message, phase: error.firelease?.phase});
      };
      firelease.attachWorker(new NodeFire(database.ref(message.path)), {
        leaseJitter: message.enabled, minLease: '2s', maxConcurrent: 2,
        bufferSize: message.bufferSize ?? Infinity,
        captureLeaseJitterMetrics: metric => {process.send({metric});}
      }, async item => {
        process.send({work: {
          key: item.$ref.key, affinity: item._lease.affinityKey, attempt: item._lease.attempts,
          firstAcquisition: item._lease.firstAcquisition === true,
          pickupDelay: Math.max(0, item.$ref.now - (item._lease.created ?? item._lease.initial))
        }});
        if (item.extend) await firelease.extendLease(item, 3000);
        await sleep(item.hold ?? 30);
        if (item.retry && item._lease.attempts < 2) return 500;
      });
      // mode is initially safe, then becomes full once the initial snapshot has loaded.
      if (message.bufferSize) await sleep(500);
      else await waitFor(() => firelease.stats.queues[0]?.mode === 'full', 'worker listener ready');
    } else if (message.command === 'quota') {
      firelease.settings.globalMaxConcurrent = message.value;
    } else if (message.command === 'offline') {
      database.goOffline();
      await waitFor(() => !firelease.stats.queues[0].sources[0].connected, 'disconnect');
    } else if (message.command === 'online') {
      database.goOnline();
      await waitFor(() => firelease.stats.queues[0].sources[0].connected, 'reconnect');
    } else if (message.command === 'stop') {
      TESTABLES.resetBetweenTests();
      await deleteApp(app);
      stopped = true;
      process.send({id: message.id});
      process.disconnect();
      return;
    }
    process.send({id: message.id, stats: JSON.parse(JSON.stringify(firelease.stats))});
  }
} else {
  const run = randomUUID();
  const root = database.ref(`firelease-tests/${run}`);
  const clients = [];
  const works = [];
  const errors = [];
  const pickups = [];
  let serial = 0;
  const report = {workers, tasksPerMode: taskCount, scenarios: {}};
  try {
    for (let i = 0; i < workers; i++) {
      const workerProcess = fork(fileURLToPath(import.meta.url), [
        '--live', '--worker', String(i), '--database', databaseURL
      ], {execArgv: ['--import=tsx'], stdio: ['ignore', 'ignore', 'pipe', 'ipc']});
      const pending = new Map();
      const client = {process: workerProcess, pending};
      workerProcess.on('message', message => {
        if (message.work) works.push({...message.work, worker: i});
        if (message.metric?.outcome === 'acquired') pickups.push(message.metric.pickupDelay);
        if (message.error) errors.push(message);
        if (message.id && pending.has(message.id)) {
          const {resolve, reject} = pending.get(message.id);
          pending.delete(message.id);
          if (message.error) reject(new Error(message.error)); else resolve(message.stats);
        }
      });
      workerProcess.stderr.on('data', data => {console.error(`worker ${i}: ${data}`);});
      workerProcess.on('exit', code => {
        if (code) errors.push({error: `worker ${i} exited ${code}`});
        for (const {reject} of pending.values()) reject(new Error(`worker ${i} exited`));
        pending.clear();
      });
      clients.push(client);
    }
    async function command(client, kind, params = {}) {
      const id = ++serial;
      return await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          client.pending.delete(id);
          reject(new Error(`worker command timed out: ${kind}`));
        }, 30_000);
        client.pending.set(id, {
          resolve: value => {clearTimeout(timer); resolve(value);},
          reject: error => {clearTimeout(timer); reject(error);}
        });
        client.process.send({id, command: kind, ...params});
      });
    }
    async function configure(name, enabled, bufferSize) {
      works.length = pickups.length = 0;
      await Promise.all(clients.map(client => command(client, 'configure', {
        path: `firelease-tests/${root.key}/${name}`, enabled, bufferSize
      })));
      return root.child(name);
    }
    async function enqueue(ref, key, extra = {}, affinity = key) {
      await ref.child(key).set({
        ...extra, _lease: {created: ServerValue.TIMESTAMP, affinityKey: affinity, ...extra._lease}
      });
    }
    async function retired(ref, key, expected = 1) {
      await waitFor(
        () => works.filter(work => work.key === key).length >= expected, `acquire ${key}`);
      await ref.child(key).once('value').then(async snapshot => {
        if (!snapshot.exists()) return;
        await new Promise((resolve, reject) => {
          const timer = setTimeout(() => {
            ref.child(key).off('value', observe);
            reject(new Error(`retire ${key}`));
          }, 10_000);
          function observe(snap) {
            if (snap.exists()) return;
            clearTimeout(timer);
            ref.child(key).off('value', observe);
            resolve();
          }
          ref.child(key).on('value', observe);
        });
      });
      // Let the last losing transactions settle before taking counts or replacing task keys.
      await sleep(100);
    }
    function quantile(samples, q) {
      const index = Math.min(samples.length - 1, Math.floor(samples.length * q));
      return [...samples].sort((a, b) => a - b)[index] ?? 0;
    }
    async function summarize() {
      const stats = await Promise.all(clients.map(client => command(client, 'stats')));
      const totals = {acquired: 0, contended: 0, failed: 0, tries: 0, suppressed: 0};
      for (const stat of stats) {
        for (const key of ['acquired', 'contended', 'failed', 'tries']) {
          totals[key] += stat.leaseTransactions[key];
        }
        totals.suppressed += stat.leaseJitter?.suppressed ?? 0;
      }
      return {
        ...totals,
        amplification: (totals.acquired + totals.contended + totals.failed) / totals.acquired,
        pickupMedianMs: quantile(works.map(work => work.pickupDelay), 0.5),
        pickupP95Ms: quantile(works.map(work => work.pickupDelay), 0.95),
        windowsMs: stats.map(stat => Math.round(stat.leaseJitter?.window ?? 0))
      };
    }
    for (const enabled of [false, true]) {
      const name = enabled ? 'adaptive' : 'baseline';
      const ref = await configure(name, enabled);
      for (let i = 0; i < taskCount; i++) {
        const key = `task-${i}`;
        await enqueue(ref, key, {}, `owner|repo|${i % 16}`);
        await retired(ref, key);
      }
      assert.equal(works.length, taskCount, `${name}: each task acquired once`);
      assert(works.every(work => work.firstAcquisition && work.attempt === 1));
      report.scenarios[name] = await summarize();
      console.log(JSON.stringify({scenario: name, ...report.scenarios[name]}));
    }
    assert(report.scenarios.adaptive.amplification < report.scenarios.baseline.amplification,
      'Adaptive jitter must reduce transactions per acquisition');
    assert(report.scenarios.adaptive.suppressed > 0);
    assert.equal(report.scenarios.adaptive.failed, 0);

    const ref = await configure('lifecycle', true);
    // A recurring PR affinity uses distinct task keys, so affinity cannot come from key reuse.
    for (let i = 0; i < 20; i++) {
      await enqueue(ref, `affinity-${i}`, {}, 'owner|cached-repo|123');
      await retired(ref, `affinity-${i}`);
    }
    const affinityCounts = new Map();
    for (const work of works) {
      affinityCounts.set(work.worker, (affinityCounts.get(work.worker) ?? 0) + 1);
    }
    const [preferred, wins] = [...affinityCounts].sort((a, b) => b[1] - a[1])[0];
    assert(wins >= 12, 'At least 60% of recurring affinity should reach the same worker');
    await command(clients[preferred], 'quota', {value: 0});
    await enqueue(ref, 'busy-fallback', {}, 'owner|cached-repo|123');
    await retired(ref, 'busy-fallback');
    assert.notEqual(works.find(work => work.key === 'busy-fallback').worker, preferred);
    await command(clients[preferred], 'quota', {value: 200});

    await enqueue(ref, 'retry', {retry: true});
    await retired(ref, 'retry', 2);
    const retries = works.filter(work => work.key === 'retry');
    assert.deepEqual(retries.map(work => work.attempt), [1, 2]);
    assert.deepEqual(retries.map(work => work.firstAcquisition), [true, false]);
    // Keep working past the original two-second lease, but within the extension.
    await enqueue(ref, 'extended', {extend: true, hold: 2300});
    await retired(ref, 'extended');
    assert.equal(works.filter(work => work.key === 'extended').length, 1);
    await command(clients[preferred], 'offline');
    await enqueue(ref, 'offline-fallback', {}, 'owner|cached-repo|123');
    await retired(ref, 'offline-fallback');
    assert.notEqual(works.find(work => work.key === 'offline-fallback').worker, preferred);
    await command(clients[preferred], 'online');
    report.scenarios.lifecycle = {
      affinityWins: wins, affinityTasks: 20, busyFallback: true, disconnectFallback: true,
      retryAttempts: retries.map(work => work.attempt), extensionAcquisitions: 1
    };
    // Replay must process already eligible backlog with no jitter, including on limited listeners.
    for (const bufferSize of [undefined, 3]) {
      const name = bufferSize ? 'limited-backlog' : 'backlog';
      await root.child(name).set(Object.fromEntries(Array.from({length: 12}, (_, i) => [
        `old-${i}`,
        {payload: i, _lease: {created: Date.now() - 60_000, expiry: Date.now() - 60_000}}
      ])));
      const backlog = await configure(name, true, bufferSize);
      await waitFor(() => works.length >= 12, name);
      for (let i = 0; i < 12; i++) await retired(backlog, `old-${i}`);
      assert.equal(works.length, 12, `${name}: no duplicate processing`);
      const stats = await Promise.all(clients.map(client => command(client, 'stats')));
      assert(stats.every(stat => stat.leaseJitter.scheduled === 0),
        'Do not jitter overdue backlog');
      report.scenarios[name] = {acquired: works.length, jitterScheduled: 0};
    }
    const limited = await configure('limited-arrivals', true, 3);
    for (let i = 0; i < 20; i++) {
      const key = `fresh-${i}`;
      await enqueue(limited, key);
      await retired(limited, key);
    }
    assert.equal(works.length, 20, 'Limited listeners acquire each fresh task once');
    assert(works.every(work => work.firstAcquisition && work.attempt === 1));
    report.scenarios['limited-arrivals'] = await summarize();
    assert(report.scenarios['limited-arrivals'].suppressed > 0);
    assert.equal(errors.length, 0, JSON.stringify(errors));
    await Promise.all(clients.map(client => command(client, 'stop')));
    await Promise.all(clients.map(client => client.process.exitCode === null ?
      new Promise(resolve => {client.process.once('exit', resolve);}) : undefined));
    assert(clients.every(client => client.process.exitCode === 0), 'Workers must exit cleanly');
    assert.equal(errors.length, 0, JSON.stringify(errors));
    report.passed = true;
  } catch (error) {
    report.passed = false;
    report.error = error.message;
    throw error;
  } finally {
    for (const client of clients) if (client.process.connected) client.process.kill();
    // Delete only this UUID-scoped run after all worker connections have been stopped.
    await Promise.all(clients.map(client => client.process.exitCode === null ?
      new Promise(resolve => {client.process.once('exit', resolve);}) : undefined));
    await root.remove();
    report.cleanedUp = !(await root.once('value')).exists();
    await deleteApp(app);
    clearTimeout(deadline);
    console.log(JSON.stringify(report, null, 2));
    if (values.output) await writeFile(values.output, JSON.stringify(report, null, 2) + '\n');
  }
}
