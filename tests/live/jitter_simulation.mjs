// Closed-loop, deterministic scheduler model. No network access; deliberately not a unit test.
import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {parseArgs} from 'node:util';
import console from 'node:console';
const require = createRequire(import.meta.url);
const {LeaseJitter, LeaseJitterGain, LeaseJitterSchedule} = require('../../src/jitter.ts');
const {values} = parseArgs({options: {
  output: {type: 'string'}, seeds: {type: 'string', default: '6'},
  offset: {type: 'string', default: '0'}, candidates: {type: 'string'}
}});

/* eslint-disable no-bitwise */ // Seeded integer mixing and the event heap, not production code.
function random(key) {
  let x = (key + 0x9e3779b9) | 0;
  x = Math.imul(x ^ x >>> 16, 0x21f0aaad);
  x = Math.imul(x ^ x >>> 15, 0x735a2d97);
  return ((x ^ x >>> 15) >>> 0) / 4294967296;
}
function quantile(samples, q) {
  return [...samples].sort((a, b) => a - b)[
    Math.min(samples.length - 1, Math.floor(samples.length * q))] ?? 0;
}
function summary(tasks) {
  const count = tasks.length;
  return {
    tasks: count, amplification: tasks.reduce((n, t) => n + t.attempts, 0) / count,
    pickupP50: quantile(tasks.map(t => t.pickup), 0.5),
    pickupP95: quantile(tasks.map(t => t.pickup), 0.95),
    pickupP99: quantile(tasks.map(t => t.pickup), 0.99),
    completionP95: quantile(tasks.map(t => t.pickup + t.service), 0.95),
    elapsed: count ? Math.max(...tasks.map(t => t.time + t.pickup + t.service)) - tasks[0].time : 0
  };
}

// Heap ordering is stable, including simultaneous arrivals. All input randomness depends on
// task/worker/seed, never on how many events a particular controller happens to generate.
class Events {
  heap = [];
  serial = 0;
  now = 0;
  add(time, fn) {
    const item = {time, fn, serial: this.serial++};
    let i = this.heap.length;
    this.heap.push(item);
    while (i) {
      const parent = (i - 1) >> 1;
      if (!this.before(item, this.heap[parent])) break;
      this.heap[i] = this.heap[parent];
      i = parent;
    }
    this.heap[i] = item;
  }

  before(a, b) {return a.time < b.time || a.time === b.time && a.serial < b.serial;}
  run() {
    while (this.heap.length) {
      const next = this.heap[0], last = this.heap.pop();
      if (this.heap.length) {
        let i = 0;
        while (2 * i + 1 < this.heap.length) {
          let child = 2 * i + 1;
          if (child + 1 < this.heap.length && this.before(this.heap[child + 1], this.heap[child])) {
            child++;
          }
          if (!this.before(this.heap[child], last)) break;
          this.heap[i] = this.heap[child];
          i = child;
        }
        this.heap[i] = last;
      }
      this.now = next.time;
      next.fn(next.time);
    }
  }
}

function run(phases, seed, tuning) {
  const events = new Events(), tasks = [], windows = [];
  const workers = Array.from({length: 20}, (_, index) => {
    const gain = tuning === 'fixed' ? {next: () => 2} :
      tuning === 'default' ? new LeaseJitterGain() : new LeaseJitterGain(...tuning);
    const controller = new LeaseJitter({}, `${seed}/${index}`, gain);
    const record = controller.record.bind(controller);
    controller.record = (outcome, now = events.now) => record(outcome, now);
    // Cache the unchanged SHA-256 rank only to make large sweeps cheap.
    const delay = controller.delay.bind(controller), ranks = new Map();
    controller.delay = key => {
      if (!ranks.has(key)) {
        const window = controller.stats.window;
        controller.stats.window = 1;
        ranks.set(key, delay(key));
        controller.stats.window = window;
      }
      return ranks.get(key) * controller.stats.window;
    };
    return {controller, slots: 0, pending: new Set(), index};
  });
  const population = {active: workers};
  let phaseStart = 0, taskSerial = 0;
  function wake(worker, now) {
    for (const ctx of worker.pending) {
      if (worker.slots >= ctx.task.capacity) break;
      if (!ctx.started && ctx.ready <= now) attempt(ctx, now);
    }
  }
  function attempt(ctx, now) {
    const {worker, task, schedule} = ctx;
    if (ctx.known || ctx.started) return;
    if (worker.slots >= task.capacity) {
      schedule.skip(); // Natural staggering consumes this eligibility generation.
      return;
    }
    ctx.started = true;
    worker.pending.delete(ctx);
    worker.slots++;
    const trains = schedule.start() !== undefined;
    task.attempts++;
    events.add(now + ctx.up, committed => {
      const won = task.winner === undefined;
      if (won) {
        task.winner = worker.index;
        for (const other of task.contexts) {
          events.add(committed + other.down, observed => {
            other.known = true;
            other.worker.pending.delete(other);
            other.schedule.update({expiry: committed + 10000, time: 10000, attempts: 1, busy: true},
              task.affinity, observed, true, other === ctx);
          });
        }
      }
      events.add(committed + ctx.down, completed => {
        if (won) {
          worker.controller.observePropagation(ctx.up + ctx.down);
          task.pickup = completed - task.time;
        }
        if (trains) worker.controller.record(won ? 'acquired' : 'contended', completed);
        events.add(completed + (won ? task.service : 0), finished => {
          worker.slots--;
          wake(worker, finished);
        });
      });
    });
  }
  for (const [phase, spec] of phases.entries()) {
    const start = phaseStart;
    events.add(start, () => {
      population.active = spec.survivor ? [workers.reduce((a, b) =>
        a.controller.stats.window > b.controller.stats.window ? a : b)] :
        workers.slice(0, spec.workers ?? 20);
      windows.push({phase, start, active: population.active.map(w => w.index),
        windows: workers.map(w => w.controller.stats.window)});
    });
    let elapsed = spec.gap ?? 0;
    while (elapsed < spec.seconds * 1000) {
      const id = taskSerial++;
      const service = spec.service ?? 30;
      const task = {id, phase, time: start + elapsed, service, attempts: 0,
        capacity: spec.capacity ?? 20,
        affinity: spec.hot && random(seed * 337 + id) < 0.8 ? 'hot' : `pr-${id % 64}`,
        contexts: []};
      tasks.push(task);
      events.add(task.time, () => {
        for (const worker of population.active) {
          const latency = (spec.latency ?? 80) * (spec.heterogeneous ? 0.4 + worker.index / 8 : 1);
          const up = latency / 2 * (0.8 + 0.4 * random(seed * 99991 + id * 719 + worker.index));
          const down = latency / 2 * (0.8 + 0.4 * random(seed * 199999 + id * 347 + worker.index));
          const schedule = new LeaseJitterSchedule(worker.controller);
          const ctx = {worker, task, schedule, up, down};
          task.contexts.push(ctx);
          events.add(task.time + down, observed => {
            if (ctx.known) return;
            schedule.update({expiry: task.time}, task.affinity, observed, true, false);
            ctx.ready = Math.max(
              observed, schedule.deadline(observed, worker.slots < task.capacity));
            worker.pending.add(ctx);
            events.add(ctx.ready, now => {attempt(ctx, now);});
          });
        }
      });
      elapsed += spec.burst ? (id % 20 === 19 ? 5000 : 0) :
        -Math.log(Math.max(0.000001, random(seed * 100003 + id))) * 1000 / spec.rate;
    }
    phaseStart += spec.seconds * 1000;
  }
  events.run();
  assert(tasks.every(t => Number.isFinite(t.pickup)), 'Every task must be acquired');
  return {seed, ...summary(tasks),
    phases: phases.length === 1 ? undefined : phases.map((_, phase) => ({
      ...summary(tasks.filter(t => t.phase === phase)),
      first20P95: quantile(
        tasks.filter(t => t.phase === phase).slice(0, 20).map(t => t.pickup), 0.95),
      last20P95: quantile(tasks.filter(t => t.phase === phase).slice(-20).map(t => t.pickup), 0.95)
    })), windows: phases.some(spec => spec.survivor) ? windows : undefined,
    finalWindowRange: [Math.min(...workers.map(w => w.controller.stats.window)),
      Math.max(...workers.map(w => w.controller.stats.window))]};
}

const scenarios = {
  sparse: [{rate: 0.2, seconds: 1200}],
  light: [{rate: 2, seconds: 180}],
  fast: [{rate: 40, seconds: 120}],
  veryFast: [{rate: 200, seconds: 60, capacity: 200}],
  two: [{workers: 2, rate: 20, seconds: 120}],
  eight: [{workers: 8, rate: 20, seconds: 120}],
  single: [{workers: 1, rate: 2, seconds: 120}],
  scale: [{workers: 1, rate: 5, seconds: 60}, {rate: 5, seconds: 120},
    {survivor: true, rate: 5, seconds: 120}],
  fastScale: [{rate: 40, seconds: 120}, {survivor: true, rate: 40, seconds: 120}],
  rate: [{rate: 2, seconds: 120}, {rate: 40, seconds: 120}, {rate: 2, seconds: 120}],
  latency: [{rate: 5, seconds: 120}, {rate: 5, seconds: 120, latency: 240}],
  heterogeneous: [{rate: 5, seconds: 180, heterogeneous: true}],
  burst: [{burst: true, seconds: 180}],
  saturated: [{workers: 8, rate: 50, seconds: 60, service: 500, capacity: 2}],
  hot: [{rate: 10, seconds: 180, hot: true}],
  resume: [{rate: 10, seconds: 120}, {rate: 10, seconds: 180, gap: 60000}]
};
const candidates = {fixed: 'fixed', default: 'default'};
for (const cap of [2, 4, 8]) {
  for (const budget of [4, 10, 20]) {
    candidates[`cap${cap}-budget${budget}`] = [cap, budget, 1000];
  }
}
for (const cap of [2, 3, 4]) {
  for (const budget of [20, 40]) {
    candidates[`cap${cap}-budget${budget}-smooth5`] = [cap, budget, 5000];
  }
}
const defaultGain = new LeaseJitterGain();
const report = {model: 'Closed-loop scheduling model, not Firebase or a cache benchmark',
  defaultGain: {maximum: defaultGain.maximum, budget: defaultGain.budget,
    smoothingMs: defaultGain.smoothingMs}, candidates,
  assumptions: ['Exogenous seeded Poisson/burst arrivals and per-task network delays',
    'Actual controller, rank and scheduling state machine; simplified atomic server winner',
    '20 slots per worker, 200 in veryFast, 2 in saturation; attempts hold slots',
    'Every loser counts one transaction',
    'Graceful scale changes affect new arrivals, keeping controller state',
    'No Firebase retry, bandwidth, lease-expiry, or application-cache model'],
  scenarios, results: {}};
for (const [name, tuning] of Object.entries(candidates)) {
  if (values.candidates && !values.candidates.split(',').includes(name)) continue;
  report.results[name] = {};
  for (const [scenario, phases] of Object.entries(scenarios)) {
    const runs = Array.from({length: Number(values.seeds)}, (_, i) =>
      run(phases, Number(values.offset) + i + 1, tuning));
    const aggregate = Object.fromEntries(['amplification', 'pickupP95', 'pickupP99'].map(key =>
      [key, runs.reduce((sum, result) => sum + result[key], 0) / runs.length]));
    report.results[name][scenario] = {aggregate, runs};
    console.log(JSON.stringify({candidate: name, scenario, ...aggregate}));
  }
}
if (values.output) await writeFile(values.output, JSON.stringify(report, null, 2) + '\n');
