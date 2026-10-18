import _ from 'lodash';
import NodeFire, {type OperationDescriptor} from 'nodefire';

import type {FireleaseError, FireleaseSettings} from './firelease';
import {DatabaseStats, type FireleaseStats} from './stats';

const WRITE_LATENCY_ALPHA = 0.1;
const WRITE_METHODS = new Set(['set', 'update', 'remove', 'push', 'transaction']);

export type WriteMethod = 'set' | 'update' | 'remove' | 'push' | 'transaction';

/** Timings are milliseconds. Transaction timings exclude prefetch. */
export type WriteMetric = {
  readonly ref: string;
  readonly app: string;
  readonly method: WriteMethod;
  readonly tries: number;
} & ({
  readonly outcome: 'success';
  readonly duration: number;
  /** Duration per transaction try, or the duration of an ordinary write. */
  readonly latency: number;
} | {
  readonly outcome: 'failure' | 'timeout';
  /** Time until rejection, not the time until a write completed. */
  readonly elapsed: number;
  readonly code: string;
});

export type CaptureWriteMetrics = (metric: WriteMetric) => undefined;

export class DatabaseWriteObserver {
  private readonly databases: {
    root: NodeFire, stats: DatabaseStats, registeredAt: number
  }[] = [];

  constructor(
    private readonly stats: FireleaseStats,
    private readonly settings: Pick<FireleaseSettings, 'captureWriteMetrics' | 'captureError'>
  ) {
    // NodeFire has no interceptor removal API. Install once, and only clear the registry on reset.
    NodeFire.interceptOperations(op => {this.observe(op);}, 'after');
  }

  register(ref: NodeFire) {
    const root = ref.root;
    if (_.some(this.databases, database => database.root.isEqual(root))) return;
    const stats = new DatabaseStats(root.toString(), root.database.app.name);
    this.databases.push({root, stats, registeredAt: performance.now()});
    this.stats.databases.push(stats);
  }

  observe(op: OperationDescriptor) {
    try {
      this.record(op);
    } catch (cause) {
      // After-interceptor errors would otherwise change the result of the database operation.
      try {
        const error: FireleaseError = _.isError(cause) ? cause : new Error(String(cause));
        error.firelease = {...error.firelease, phase: 'write-metric'};
        this.settings.captureError(error);
      } catch (error) {
        try {
          console.error('Error capturing write metric error:', error);
        } catch {
          // Observability must never interrupt a write or task processing.
        }
      }
    }
  }

  private record(op: OperationDescriptor) {
    if (!WRITE_METHODS.has(op.method) || !Number.isFinite(op.startTime)) return;
    const transaction = op.method === 'transaction' ? op.transaction : undefined;
    if (op.method === 'transaction' &&
        (!transaction || transaction.outcome === 'skip' ||
          !Number.isInteger(transaction.tries) || transaction.tries! < 1)) return;
    const duration = transaction ? transaction.duration : op.duration;
    if (!Number.isFinite(duration) || duration! < 0) return;

    const root = op.ref.root;
    const database = _.find(this.databases, candidate => candidate.root.isEqual(root));
    // Also exclude completions of writes started before registration or before a test reset.
    if (!database || op.startTime! < database.registeredAt) return;
    const {stats} = database;
    const tries = transaction ? transaction.tries! : 1;
    const identity = {ref: stats.ref, app: stats.app, method: op.method as WriteMethod, tries};
    let metric: WriteMetric;
    if (op.error === undefined) {
      if (transaction && transaction.outcome !== 'commit') return;
      const latency = duration! / tries;
      stats.writeLatency = stats.writeLatency === null ? latency :
        (1 - WRITE_LATENCY_ALPHA) * stats.writeLatency + WRITE_LATENCY_ALPHA * latency;
      stats.writeSamples++;
      metric = {...identity, outcome: 'success', duration: duration!, latency};
    } else {
      const error = op.error as Error & {firebase?: {code?: string}};
      // In particular, user transaction-update errors are not Firebase write observations.
      if (!error.firebase) return;
      const code = _.toLower(error.firebase.code ?? '');
      const outcome = code === 'timeout' ? 'timeout' : 'failure';
      if (outcome === 'timeout') stats.writeTimeouts++;
      else stats.writeFailures++;
      metric = {...identity, outcome, elapsed: duration!, code};
    }
    this.settings.captureWriteMetrics?.(metric);
  }

  reset() {
    this.databases.length = 0;
    this.stats.databases.length = 0;
  }
}
