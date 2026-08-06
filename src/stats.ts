import _ from 'lodash';

export type QueueSourceMode = 'full' | 'safe';
export type QueueMode = QueueSourceMode | 'mixed';

export interface LeaseTransactionStats {
  acquired: number;
  contended: number;
  failed: number;
  tries: number;
  duration: number;
}

function createLeaseTransactionStats(): LeaseTransactionStats {
  return {acquired: 0, contended: 0, failed: 0, tries: 0, duration: 0};
}

function rollUpLeaseTransactions(items: LeaseTransactionStats[]) {
  const result = createLeaseTransactionStats();
  result.acquired = _.sumBy(items, 'acquired');
  result.contended = _.sumBy(items, 'contended');
  result.failed = _.sumBy(items, 'failed');
  result.tries = _.sumBy(items, 'tries');
  const attemptedItems = _.filter(items, countLeaseAttempts);
  if (attemptedItems.length) result.duration = _.meanBy(attemptedItems, 'duration');
  return result;
}

function countLeaseAttempts(stats: LeaseTransactionStats) {
  return stats.acquired + stats.contended + stats.failed;
}

function exposeGetters(instance: object, properties: string[]) {
  const prototype = Object.getPrototypeOf(instance);
  for (const property of properties) {
    const descriptor = Object.getOwnPropertyDescriptor(prototype, property);
    Object.defineProperty(instance, property, {...descriptor, enumerable: true});
  }
}

export class QueueSourceStats {
  connected = false;
  mode: QueueSourceMode = 'safe';
  size: number | null = null;
  declare sizeDelta?: number;
  declare sizeTimestamp?: number;
  healthy = true;
  latency: number | null = null;
  declare pingTimestamp?: number;
  readonly leaseTransactions = createLeaseTransactionStats();

  constructor(readonly ref: string) {}
}

export class QueueStats {
  tasksAcquired = 0;

  constructor(
    readonly ref: string,
    readonly key: string | null,
    readonly sources: QueueSourceStats[]
  ) {
    exposeGetters(
      this,
      ['mode', 'size', 'sizeDelta', 'sizeTimestamp', 'healthy', 'maxLatency', 'leaseTransactions'],
    );
  }

  get mode(): QueueMode {
    const modes = _.uniq(_.map(this.sources, 'mode'));
    return modes.length === 1 ? modes[0] : 'mixed';
  }

  get size() {
    const sizes = _.map(this.sources, 'size');
    return _.every(sizes, _.isNumber) ? _.sum(sizes) : null;
  }

  get sizeDelta() {
    const deltas = _.map(this.sources, 'sizeDelta');
    return _.every(deltas, _.isNumber) ? _.sum(deltas) : undefined;
  }

  get sizeTimestamp() {
    return _(this.sources).map('sizeTimestamp').filter(_.isNumber).min();
  }

  get healthy() {
    return _.every(this.sources, source => source.connected && source.healthy);
  }

  get maxLatency() {
    return _(this.sources).map('latency').max() || 0;
  }

  get leaseTransactions() {
    return rollUpLeaseTransactions(_.map(this.sources, 'leaseTransactions'));
  }
}

export class FireleaseStats {
  readonly #getStuckTasks: () => number;
  readonly queues: QueueStats[] = [];

  constructor(getStuckTasks: () => number) {
    this.#getStuckTasks = getStuckTasks;
    exposeGetters(
      this,
      [
        'healthy', 'sickQueues', 'sickSources', 'stuckTasks', 'maxLatency', 'leaseTransactions',
        'tasksAcquired'
      ],
    );
  }

  get healthy() {
    return _.every(this.queues, 'healthy');
  }

  get sickQueues() {
    return _(this.queues).reject('healthy').map('key').value();
  }

  get sickSources() {
    return _(this.queues)
      .flatMap(queue => queue.sources)
      .reject(source => source.connected && source.healthy)
      .map(source => source.ref)
      .uniq()
      .value();
  }

  get stuckTasks() {
    return this.#getStuckTasks();
  }

  get maxLatency() {
    return _(this.queues).map('maxLatency').max() || 0;
  }

  get leaseTransactions() {
    return rollUpLeaseTransactions(_.map(this.queues, 'leaseTransactions'));
  }

  get tasksAcquired() {
    return _.sumBy(this.queues, queue => queue.tasksAcquired);
  }
}
