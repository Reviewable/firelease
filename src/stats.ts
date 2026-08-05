import _ from 'lodash';

export type QueueSourceMode = 'full' | 'safe';

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
    return _(this.sources).map('latency').max() || 0;
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

  get tasksAcquired() {
    return _.sumBy(this.queues, queue => queue.tasksAcquired);
  }
}
