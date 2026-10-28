import {createHash, randomUUID} from 'node:crypto';
import type {LeaseJitterStats} from './stats';

const INSTANCE_ID = randomUUID();
const MIN_WINDOW = 25;
const MAX_WINDOW = 5000;
const TARGET_RACE_RATE = 0.05;

export type LeaseJitterMetric =
  {outcome: 'suppressed'} | {outcome: 'acquired', pickupDelay: number};
export type CaptureLeaseJitterMetrics = (metric: LeaseJitterMetric) => undefined;

/** One controller per physical source; one stable hash seed across this process. */
export class LeaseJitter {
  readonly stats: LeaseJitterStats = {scheduled: 0, suppressed: 0, window: 250};
  private propagation = 100;

  constructor(private readonly instanceId: string = INSTANCE_ID) {}

  delay(affinity: string) {
    // 48 bits fit exactly in a JS number. Length framing avoids ambiguous key combinations.
    const hash = createHash('sha256').update(JSON.stringify([this.instanceId, affinity])).digest();
    return hash.readUIntBE(0, 6) / 2 ** 48 * this.stats.window;
  }

  observePropagation(duration: number) {
    if (!Number.isFinite(duration) || duration < 0 || duration > MAX_WINDOW) return;
    this.propagation = 0.9 * this.propagation + 0.1 * Math.max(1, duration);
  }

  record(outcome: 'contended' | 'suppressed') {
    if (outcome === 'suppressed') this.stats.suppressed++;
    this.stats.window = Math.max(MIN_WINDOW, Math.min(MAX_WINDOW,
      this.stats.window + 2 * this.propagation *
        ((outcome === 'contended' ? 1 : 0) - TARGET_RACE_RATE)));
  }
}

/** Local eligibility state, independent of the lease reservation and worker concurrency. */
export class LeaseJitterSchedule {
  private initialized = false;
  private expiry?: number;
  private attempts?: number;
  private affinity = '';
  private eligibleAt = 0;
  private enabled = false;
  private pending?: {eligibleAt: number, deadline: number};

  constructor(readonly controller: LeaseJitter) {}

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
    this.enabled = this.eligibleAt > now || live && now - this.eligibleAt <= MAX_WINDOW;
    return suppressed;
  }

  deadline(now: number, hasQuota: boolean) {
    if (!this.enabled) return now;
    if (now < this.eligibleAt) return this.eligibleAt;
    if (!hasQuota || now - this.eligibleAt > MAX_WINDOW) this.skip();
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

  start() {
    const eligibleAt = this.pending?.eligibleAt;
    this.skip();
    return eligibleAt;
  }

  skip() {
    this.enabled = false;
    this.pending = undefined;
  }

  suppress(now: number) {
    if (!this.pending || now < this.pending.eligibleAt) return false;
    this.skip();
    this.controller.record('suppressed');
    return true;
  }
}
