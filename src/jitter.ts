import _ from 'lodash';
import {hash, randomUUID} from 'node:crypto';
import ms from 'ms';
import type {Duration} from './firelease';
import type {LeaseJitterStats} from './stats';

const INSTANCE_ID = randomUUID();
// Freshness/measurement limits are independent of the caller's chosen delay cap.
const FRESHNESS_LIMIT = 5000;
const CONTROLLER_GAIN = 2;

export interface LeaseJitterOptions {
  /** Fraction of jittered opportunities allowed to race; must be strictly between 0 and 1. */
  targetRaceRate?: number;
  /** Numeric milliseconds or human-readable durations. Zero disables delay at the floor. */
  minWindow?: Duration;
  maxWindow?: Duration;
}

export type LeaseJitterMetric =
  {outcome: 'suppressed'} | {outcome: 'acquired', pickupDelay: number};
export type CaptureLeaseJitterMetrics = (metric: LeaseJitterMetric) => undefined;

/** One controller per physical source; one stable hash seed across this process. */
export class LeaseJitter {
  readonly stats: LeaseJitterStats = {scheduled: 0, suppressed: 0, window: 250};
  readonly options: {targetRaceRate: number, minWindow: number, maxWindow: number};
  // EMA of notification delay or successful transaction time per try, in milliseconds.
  private propagation = 100;

  constructor(
    options: LeaseJitterOptions = {}, private readonly instanceId: string = INSTANCE_ID
  ) {
    this.options = {
      targetRaceRate: options.targetRaceRate ?? 0.05,
      minWindow: _.isString(options.minWindow) ?
        ms(options.minWindow as ms.StringValue) : options.minWindow ?? 0,
      maxWindow: _.isString(options.maxWindow) ?
        ms(options.maxWindow as ms.StringValue) : options.maxWindow ?? 5000
    };
    const {targetRaceRate, minWindow, maxWindow} = this.options;
    if (!Number.isFinite(targetRaceRate) || targetRaceRate <= 0 || targetRaceRate >= 1 ||
        !Number.isFinite(minWindow) || minWindow < 0 ||
        !Number.isFinite(maxWindow) || maxWindow < minWindow) {
      throw new Error('Invalid leaseJitter targetRaceRate or window bounds');
    }
    // Keep the tested startup default, clamped to the requested interval, rather than making
    // a larger maximum (which is only a safety cap) inflate every initial delay.
    this.stats.window = Math.max(minWindow, Math.min(maxWindow, this.stats.window));
  }

  /** Stable rank for this process/affinity pair, scaled to the current window. */
  delay(affinity: string) {
    // One-shot SHA-256 keeps uniform, uncorrelated ranks without a custom hash or dependency.
    // 48 bits fit exactly in a JS number; framing avoids ambiguous key combinations.
    const digest = hash('sha256', JSON.stringify([this.instanceId, affinity]), 'buffer');
    return digest.readUIntBE(0, 6) / 2 ** 48 * this.stats.window;
  }

  /** Smooth valid propagation observations (alpha=.1); reject clock/timeout outliers. */
  observePropagation(duration: number) {
    if (!Number.isFinite(duration) || duration < 0 || duration > FRESHNESS_LIMIT) return;
    this.propagation = 0.9 * this.propagation + 0.1 * Math.max(1, duration);
  }

  /** Train once per jittered opportunity; failures and bypassed backlog do not train. */
  record(outcome: 'acquired' | 'contended' | 'suppressed') {
    if (outcome === 'suppressed') this.stats.suppressed++;
    const {minWindow, maxWindow, targetRaceRate} = this.options;
    // Integral feedback: E[delta] = gain * propagation * (observed race rate - target).
    // At the defaults and 100ms propagation, a race adds 190ms; a win/suppression removes 10ms.
    // Gain 2 is a dimensionless responsiveness choice: correct by about two propagation times
    // per race, then decay cautiously. Counting wins also lets a lone worker reach zero delay.
    // Gain 1 would halve every step; gain 3 would multiply it by 1.5, without changing the
    // balance point at the target. Larger steps adapt faster but amplify noise and overshoot.
    this.stats.window = Math.max(minWindow, Math.min(maxWindow,
      this.stats.window + CONTROLLER_GAIN * this.propagation *
        ((outcome === 'contended' ? 1 : 0) - targetRaceRate)));
  }
}

/**
 * Per-task state: replay/bypass -> disabled; fresh eligibility -> armed -> pending deadline.
 * A start consumes the pending opportunity. A competing lease/removal suppresses it instead.
 * An expiry change re-arms the next generation; payload-only updates never move its deadline.
 * Timers and worker slots live on Task, not here.
 */
export class LeaseJitterSchedule {
  // Last observed lease generation, including the initial snapshot (whose expiry may be absent).
  private initialized = false;
  private expiry?: number;
  private attempts?: number;
  // Current affinity and eligibility timestamp; a pending deadline captures them only once.
  private affinity = '';
  private eligibleAt = 0;
  // Armed unless backlog, quota waiting, recovery, or a start/suppression bypassed it.
  private enabled = false;
  private pending?: {eligibleAt: number, deadline: number};

  constructor(readonly controller: LeaseJitter) {}

  /** Observe a snapshot; return whether it canceled a pending competing attempt. */
  update(
    lease: {expiry?: number, time?: number, attempts?: number, busy?: boolean} | undefined,
    affinity: string, now: number, live: boolean, working: boolean
  ) {
    const acquiredElsewhere = live && !working && lease?.busy && lease.time &&
      lease.expiry! > now && lease.attempts !== this.attempts;
    if (acquiredElsewhere) {
      this.controller.observePropagation(now - (lease.expiry! - lease.time!));
    }
    const suppressed = acquiredElsewhere ? this.suppress(now) : false;
    this.attempts = lease?.attempts;
    this.affinity = affinity;
    if (this.initialized && lease?.expiry === this.expiry) return suppressed;
    this.initialized = true;
    this.expiry = lease?.expiry;
    this.pending = undefined;
    this.eligibleAt = lease?.expiry ?? now;
    // Initial/replayed backlog runs immediately. A future expiry is a fresh race even on replay.
    this.enabled = this.eligibleAt > now || live && now - this.eligibleAt <= FRESHNESS_LIMIT;
    return suppressed;
  }

  /** Arm a deadline on eligibility, or bypass jitter if local capacity already staggered us. */
  deadline(now: number, hasQuota: boolean) {
    if (!this.enabled) return now;
    if (now < this.eligibleAt) return this.eligibleAt;
    if (!hasQuota || now - this.eligibleAt > FRESHNESS_LIMIT && !this.pending) this.skip();
    if (!this.enabled) return now;
    if (!this.pending) {
      this.pending = {
        eligibleAt: this.eligibleAt,
        deadline: this.eligibleAt + this.controller.delay(this.affinity)
      };
      this.controller.stats.scheduled++;
    }
    return this.pending.deadline;
  }

  /** Consume the deadline and return its origin for feedback and pickup measurement. */
  start() {
    const eligibleAt = this.pending?.eligibleAt;
    this.skip();
    return eligibleAt;
  }

  /** Disable this generation without recording a contention observation. */
  skip() {
    this.enabled = false;
    this.pending = undefined;
  }

  /** Record a competing lease/removal once, and only after this task became eligible. */
  suppress(now: number) {
    if (!this.pending || now < this.pending.eligibleAt) return false;
    this.skip();
    this.controller.record('suppressed');
    return true;
  }
}
