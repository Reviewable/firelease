import _ from 'lodash';
import NodeFire, {type OperationDescriptor} from 'nodefire';

import type {FireleaseRuntime} from './runtime';
import {DatabaseStats} from './stats';
import type {LeaseSnapshot} from './types';

const WRITE_LATENCY_EMA_ALPHA = 0.1;
const WRITE_LATENCY_LOW_WATERMARK = 0.75;
const TASK_START_RATE_DECREASE = 0.5;
const TASK_START_RATE_INCREASE = 1;
const MINIMUM_TASK_START_RATE = 0.1;
const WRITE_METHODS = new Set(['set', 'update', 'remove', 'push', 'transaction']);

interface ConnectionParticipant {
  readonly loaded: boolean;
  setConnected(connected: boolean, epoch: number, connectionStartTime: number): void;
}

class TokenBucket {
  tokens = 1;
  lastRefill = performance.now();

  constructor(public rate: number) {}

  refill(now: number) {
    const elapsed = Math.max(0, now - this.lastRefill);
    this.tokens = Math.min(1, this.tokens + elapsed * this.rate / 1000);
    this.lastRefill = now;
  }

  delay(now: number) {
    this.refill(now);
    return this.tokens >= 1 ? 0 : (1 - this.tokens) * 1000 / this.rate;
  }

  consume(now: number) {
    this.refill(now);
    this.tokens -= 1;
  }

  setRate(rate: number) {
    this.refill(performance.now());
    this.rate = rate;
  }

  reset(rate: number) {
    this.rate = rate;
    this.tokens = 1;
    this.lastRefill = performance.now();
  }
}

export class TaskStartController {
  readonly stats: DatabaseStats;
  readonly bucket: TokenBucket;
  rate: number;
  observationStartTime = 0;
  nextAdjustmentAt = 0;
  observationSamples = 0;
  observationLatency: number | null = null;

  constructor(readonly runtime: FireleaseRuntime, readonly root: NodeFire) {
    this.rate = runtime.config.initialTaskStartRate;
    this.bucket = new TokenBucket(this.rate);
    this.stats = new DatabaseStats(root.toString(), this.rate);
    runtime.stats.databases.push(this.stats);
    this.restartObservation();
  }

  delay(now: number) {
    return this.bucket.delay(now);
  }

  consume(now: number) {
    this.bucket.consume(now);
  }

  reset(reason: 'startup' | 'reconnect') {
    const oldRate = this.rate;
    this.rate = Math.min(
      this.runtime.config.initialTaskStartRate, this.runtime.config.maxTaskStartRate);
    this.stats.taskStartRate = this.rate;
    this.bucket.reset(this.rate);
    if (reason === 'reconnect' && oldRate !== this.rate) this.logAdjustment(oldRate, reason);
    this.restartObservation();
  }

  restartObservation(now = performance.now()) {
    this.observationStartTime = now;
    this.nextAdjustmentAt = now + jitter(this.runtime.config.taskStartRateAdjustmentInterval);
    this.observationSamples = 0;
    this.observationLatency = null;
  }

  clampToMaximum() {
    if (this.rate <= this.runtime.config.maxTaskStartRate) return;
    this.setRate(this.runtime.config.maxTaskStartRate, 'configuration');
  }

  record(op: OperationDescriptor) {
    if (!WRITE_METHODS.has(op.method) || !Number.isFinite(op.duration)) return;
    if (op.method === 'transaction' && op.transaction?.outcome === 'skip') return;

    let sample = op.duration! / (op.transaction?.tries || 1);
    if (op.error !== undefined) {
      if (!isFirebaseLatencyError(op.error)) return;
      sample = Math.max(sample, this.runtime.config.writeLatencyTarget * 2);
    }
    this.stats.writeLatency = this.stats.writeLatency === null ? sample :
      (1 - WRITE_LATENCY_EMA_ALPHA) * this.stats.writeLatency + WRITE_LATENCY_EMA_ALPHA * sample;
    this.stats.writeSamples++;

    if ((op.startTime ?? performance.now()) < this.observationStartTime) return;
    this.observationLatency = this.observationLatency === null ? sample :
      (1 - WRITE_LATENCY_EMA_ALPHA) * this.observationLatency +
      WRITE_LATENCY_EMA_ALPHA * sample;
    this.observationSamples++;
    const now = performance.now();
    if (this.observationSamples < this.runtime.config.taskStartRateMinSamples ||
        now < this.nextAdjustmentAt) return;

    if (this.observationLatency > this.runtime.config.writeLatencyTarget) {
      this.setRate(
        Math.max(MINIMUM_TASK_START_RATE, this.rate * TASK_START_RATE_DECREASE), 'latency high');
    } else if (this.observationLatency <
      this.runtime.config.writeLatencyTarget * WRITE_LATENCY_LOW_WATERMARK) {
      this.setRate(
        Math.min(this.runtime.config.maxTaskStartRate, this.rate + TASK_START_RATE_INCREASE),
        'latency low');
    } else {
      this.restartObservation(now);
    }
  }

  setRate(rate: number, reason: 'latency high' | 'latency low' | 'configuration') {
    const oldRate = this.rate;
    this.rate = Math.round(rate * 1000) / 1000;
    this.stats.taskStartRate = this.rate;
    this.bucket.setRate(this.rate);
    if (oldRate !== this.rate) this.logAdjustment(oldRate, reason);
    this.restartObservation();
    this.runtime.scheduler.wake();
  }

  logAdjustment(oldRate: number, reason: string) {
    console.log(
      `Firebase ${this.root} task start rate changed from ${oldRate}/s to ${this.rate}/s`,
      {
        reason,
        samples: this.observationSamples,
        writeLatency: this.observationLatency === null ? null : Math.round(this.observationLatency)
      }
    );
  }
}

export class ConnectionGroup {
  readonly connectionRef: NodeFire;
  readonly sources: ConnectionParticipant[] = [];
  readonly controller: TaskStartController;
  connected = false;
  epoch = 0;
  started = false;
  connectionStartTime = 0;
  controlledEpoch = -1;

  constructor(readonly runtime: FireleaseRuntime, readonly root: NodeFire) {
    this.connectionRef = root.child('.info/connected');
    this.controller = new TaskStartController(runtime, root);
  }

  start() {
    if (this.started) return;
    this.started = true;
    this.connectionRef.on('value', this.onConnection);
  }

  readonly onConnection = (snap: LeaseSnapshot) => {
    const connected = Boolean(snap.val());
    if (this.connected === connected) return;
    this.connected = connected;
    this.epoch++;
    if (connected) this.connectionStartTime = performance.now();
    for (const source of this.sources) {
      source.setConnected(connected, this.epoch, this.connectionStartTime);
    }
    this.runtime.connectionChanged();
  };

  reset() {
    if (this.started) this.connectionRef.off('value', this.onConnection);
    this.started = false;
    this.connected = false;
    this.epoch++;
    this.controlledEpoch = -1;
  }
}

export class DatabaseSystem {
  readonly groups: ConnectionGroup[] = [];

  constructor(readonly runtime: FireleaseRuntime) {}

  getGroup(ref: NodeFire) {
    let group = _.find(this.groups, candidate => candidate.root.isEqual(ref.root));
    if (!group) {
      group = new ConnectionGroup(this.runtime, ref.root);
      this.groups.push(group);
    }
    return group;
  }

  observe(op: OperationDescriptor) {
    const group = _.find(this.groups, candidate => candidate.root.isEqual(op.ref.root));
    group?.controller.record(op);
  }

  start() {
    for (const group of this.groups) group.start();
  }

  restartObservations() {
    for (const group of this.groups) group.controller.restartObservation();
  }

  clampRatesToMaximum() {
    for (const group of this.groups) group.controller.clampToMaximum();
  }

  reset() {
    for (const group of this.groups) group.reset();
    this.groups.length = 0;
    this.runtime.stats.databases.length = 0;
  }
}

function jitter(value: number) {
  return Math.max(0, Math.round(value * (0.95 + Math.random() * 0.1)));
}

function isFirebaseLatencyError(error: unknown) {
  const record = error as {message?: string, firebase?: {code?: string}};
  const code = _.toLower(record.firebase?.code ?? record.message ?? '');
  if (code === 'timeout' || code === 'disconnect' || code === 'set') return true;
  return Boolean(record.firebase) && !/(permission|validation|invalid)/.test(code);
}
