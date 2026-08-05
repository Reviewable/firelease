import _ from 'lodash';

export type QueueSourceMode = 'full' | 'safe';

function exposeGetters(instance: object, properties: string[]) {
  const prototype = Object.getPrototypeOf(instance);
  for (const property of properties) {
    const descriptor = Object.getOwnPropertyDescriptor(prototype, property)!;
    Object.defineProperty(instance, property, {...descriptor, enumerable: true});
  }
}

export interface QueueSourceErrorStats {
  message: string;
  timestamp: number;
  code?: string;
  delta?: number;
}

export class QueueSourceStats {
  connected = false;
  mode: QueueSourceMode = 'safe';
  size: number | null = null;
  declare sizeTimestamp?: number;
  healthy = true;
  latency: number | null = null;
  declare pingTimestamp?: number;
  declare lastError?: QueueSourceErrorStats;

  constructor(readonly ref: string) {}
}

export class QueueStats {
  tasksAcquired = 0;

  constructor(
    readonly ref: string,
    readonly key: string | null,
    readonly sources: QueueSourceStats[]
  ) {
    exposeGetters(this, ['healthy', 'maxLatency']);
  }

  get healthy() {
    return _.every(this.sources, source => source.connected && source.healthy);
  }

  get maxLatency() {
    return _.max(_.map(this.sources, source => source.latency ?? 0)) ?? 0;
  }
}

export class FireleaseStats {
  readonly #getStuckTasks: () => number;
  readonly queues: QueueStats[] = [];

  constructor(getStuckTasks: () => number) {
    this.#getStuckTasks = getStuckTasks;
    exposeGetters(
      this,
      ['healthy', 'sickQueues', 'sickSources', 'stuckTasks', 'maxLatency', 'tasksAcquired'],
    );
  }

  get healthy() {
    return _.every(this.queues, queue => queue.healthy);
  }

  get sickQueues() {
    return _(this.queues).reject(queue => queue.healthy).map(queue => queue.key).value();
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
    return _.max(_.map(this.queues, queue => queue.maxLatency)) ?? 0;
  }

  get tasksAcquired() {
    return _.sumBy(this.queues, queue => queue.tasksAcquired);
  }
}
