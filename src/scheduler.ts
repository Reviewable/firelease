import _ from 'lodash';
import ms from 'ms';
import type NodeFire from 'nodefire';
import * as timers from 'safe-timers';

import type {Queue, QueueSource} from './queues';
import type {FireleaseRuntime} from './runtime';
import type {Task} from './tasks';
import type {Duration, FireleaseError, ReleaseLimit} from './types';

interface RefScope {
  origin: string;
  path: string;
  label: string;
}

interface FlightLimiter {
  limit: number;
  scope?: RefScope;
  active: number;
  released: boolean;
}

interface FlightWaiter {
  scope?: RefScope;
  timeout: number;
  timer?: timers.Timeout;
  resolve: () => void;
  reject: (error: Error) => void;
}

export class TaskScheduler {
  timer?: timers.Timeout;
  timerDue = Infinity;
  wakeQueued = false;
  running = false;
  enabled = true;
  random = Math.random;
  readonly flightLimiters: FlightLimiter[] = [];
  readonly inFlightTasks = new Set<Task>();
  readonly flightWaiters = new Set<FlightWaiter>();

  constructor(readonly runtime: FireleaseRuntime) {}

  get inFlightCount() {
    return this.inFlightTasks.size;
  }

  start() {
    this.enabled = true;
    this.scheduleWake(ms('1m'));
  }

  reset() {
    this.timer?.clear();
    this.timer = undefined;
    this.timerDue = Infinity;
    this.wakeQueued = false;
    this.running = false;
    this.enabled = false;
    this.random = Math.random;
    this.flightLimiters.length = 0;
    this.inFlightTasks.clear();
    for (const waiter of this.flightWaiters) {
      waiter.timer?.clear();
      waiter.resolve();
    }
    this.flightWaiters.clear();
  }

  schedule(task: Task, wake = true) {
    task.timeout?.clear();
    task.timeout = undefined;
    task.ready = false;
    task.readyVersion++;
    const source = task.source as QueueSource;
    if (!this.enabled || this.runtime.tasks.get(task.key) !== task || task.removed ||
        task.working || !source.connected || !source.loaded) return;
    const delay = task.expiry - task.ref.now;
    if (delay <= 0) {
      source.enqueueReady(task);
    } else {
      task.timeout = timers.setTimeout(() => {this.schedule(task);}, Math.min(delay, ms('1m')));
    }
    if (wake) this.wake();
  }

  rebuild() {
    if (!this.enabled) return;
    for (const source of this.runtime.queues.sources) {
      if (!source.activeListener?.activated) continue;
      for (const snap of source.activeListener.snapshots.values()) {
        source.addTask(snap, false, false);
      }
    }
    for (const task of this.runtime.tasks.values()) this.schedule(task, false);
    this.wake();
  }

  wake() {
    if (!this.enabled || this.wakeQueued) return;
    this.wakeQueued = true;
    queueMicrotask(() => {
      this.wakeQueued = false;
      this.dispatch();
    });
  }

  scheduleWake(delay: number) {
    if (!this.enabled) return;
    const boundedDelay = Math.max(0, Math.min(delay, ms('1m')));
    const due = performance.now() + boundedDelay;
    if (this.timer && this.timerDue <= due) return;
    this.timer?.clear();
    this.timerDue = due;
    this.timer = timers.setTimeout(() => {
      this.timer = undefined;
      this.timerDue = Infinity;
      this.rebuild();
    }, boundedDelay);
  }

  dispatch() {
    if (!this.enabled || this.running) return;
    this.running = true;
    try {
      if (!this.runtime.canDispatch()) return;
      let starts = 0;
      while (starts < 100) {
        const result = this.takeNextTask();
        if (!result.task) {
          if (Number.isFinite(result.rateDelay)) this.scheduleWake(result.rateDelay);
          break;
        }
        const task = result.task;
        if (!task.prepare()) {
          this.schedule(task, false);
          continue;
        }
        this.consumeAdmission(task.source as QueueSource);
        this.beginTask(task);
        starts++;
      }
      if (starts === 100) timers.setTimeout(() => {this.wake();}, 0);
    } finally {
      this.running = false;
      this.scheduleWake(ms('1m'));
    }
  }

  takeNextTask(): {task?: Task, rateDelay: number} {
    const now = performance.now();
    const readyQueues = _.filter(this.runtime.queues.queues, queue => queue.hasReadyTask());
    const queueTiers = _.partition(readyQueues, queue => queue.options.priority >= 1);
    const availability = {rateDelay: Infinity};
    for (const candidateQueues of queueTiers) {
      while (candidateQueues.length) {
        // Drawing without replacement preserves the relative weights of whichever queues prove
        // admissible while letting us check their sources lazily.
        const queue = this.takeWeightedQueue(candidateQueues);
        const task = queue.takeReadyTask(source => {
          if (!this.hasFlightCapacity(source)) return false;
          const delay = source.group.controller.delay(now);
          availability.rateDelay = Math.min(availability.rateDelay, delay);
          return delay <= 0;
        });
        if (task) return {task, rateDelay: availability.rateDelay};
      }
    }
    return {rateDelay: availability.rateDelay};
  }

  takeWeightedQueue(candidateQueues: Queue[]) {
    const scale = _.maxBy(candidateQueues, queue => queue.options.priority)!.options.priority;
    let selection = this.random() *
      _.sumBy(candidateQueues, queue => queue.options.priority / scale);
    for (let index = 0; index < candidateQueues.length; index++) {
      selection -= candidateQueues[index].options.priority / scale;
      if (selection < 0) return candidateQueues.splice(index, 1)[0];
    }
    return candidateQueues.pop()!;
  }

  hasFlightCapacity(source: QueueSource) {
    return _.every(this.flightLimiters, limiter =>
      limiter.released || !scopeMatchesSource(limiter.scope, source) ||
      limiter.active < limiter.limit);
  }

  consumeAdmission(source: QueueSource) {
    source.group.controller.consume(performance.now());
  }

  beginTask(task: Task) {
    const source = task.source as QueueSource;
    for (const limiter of this.flightLimiters) {
      if (!limiter.released && scopeMatchesSource(limiter.scope, source)) limiter.active++;
    }
    this.inFlightTasks.add(task);
    void task.process().catch(cause => {
      const error = new Error('Unexpected Firelease task scheduler error') as FireleaseError;
      error.firelease = {
        cause: cause.message, itemKey: task.key, phase: 'scheduling',
        queue: task.queue.ref.toString(), source: source.ref.toString()
      };
      this.runtime.config.captureError(error);
    }).finally(() => {
      for (const limiter of this.flightLimiters) {
        if (!limiter.released && scopeMatchesSource(limiter.scope, source)) limiter.active--;
      }
      this.inFlightTasks.delete(task);
      if (task.removed) this.runtime.tasks.delete(task.key);
      this.checkFlightWaiters();
      this.runtime.taskFinished(this.inFlightTasks.size);
      this.wake();
    });
  }

  limitTasksInFlight(limit: number, ref?: NodeFire): ReleaseLimit {
    return this.addFlightLimiter(limit, ref ? scopeFromRef(ref) : undefined, true);
  }

  stopStartingTasks() {
    this.addFlightLimiter(0, undefined, false);
  }

  addFlightLimiter(limit: number, scope: RefScope | undefined, releasable: boolean) {
    if (!Number.isFinite(limit) || limit < 0 || Math.floor(limit) !== limit) {
      throw new Error('Task in-flight limit must be a non-negative finite integer');
    }
    const limiter: FlightLimiter = {
      limit,
      scope,
      active: _.sumBy([...this.inFlightTasks], task =>
        scopeMatchesSource(scope, task.source as QueueSource) ? 1 : 0),
      released: false
    };
    this.flightLimiters.push(limiter);
    this.checkFlightWaiters();
    this.wake();
    return makeRelease(() => {
      if (!releasable) return;
      limiter.released = true;
      this.checkFlightWaiters();
      this.wake();
    });
  }

  waitForTasksInFlight(timeout: Duration, ref?: NodeFire): Promise<void> {
    const normalizedTimeout = this.runtime.duration(timeout);
    if (!Number.isFinite(normalizedTimeout) || normalizedTimeout < 0) {
      throw new Error('Tasks-in-flight wait timeout must be a non-negative finite duration');
    }
    const scope = ref ? scopeFromRef(ref) : undefined;
    if (this.flightCount(scope) <= this.effectiveFlightLimit(scope)) return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const waiter: FlightWaiter = {scope, timeout: normalizedTimeout, resolve, reject};
      waiter.timer = timers.setTimeout(() => {
        this.flightWaiters.delete(waiter);
        const error = new Error('Firelease tasks-in-flight wait timed out') as FireleaseError;
        error.firelease = {
          code: 'tasks-in-flight-wait-timeout',
          count: this.flightCount(scope),
          limit: this.effectiveFlightLimit(scope),
          timeout,
          ...(scope ? {scope: scope.label} : {})
        };
        reject(error);
      }, normalizedTimeout);
      this.flightWaiters.add(waiter);
    });
  }

  flightCount(scope: RefScope | undefined) {
    return _.sumBy([...this.inFlightTasks], task =>
      scopeMatchesSource(scope, task.source as QueueSource) ? 1 : 0);
  }

  effectiveFlightLimit(scope: RefScope | undefined) {
    const limits = _(this.flightLimiters)
      .reject('released')
      .filter(limiter => scopeContains(limiter.scope, scope))
      .map('limit')
      .value();
    return limits.length ? Math.min(...limits) : Infinity;
  }

  checkFlightWaiters() {
    for (const waiter of this.flightWaiters) {
      if (this.flightCount(waiter.scope) > this.effectiveFlightLimit(waiter.scope)) continue;
      this.flightWaiters.delete(waiter);
      waiter.timer?.clear();
      waiter.resolve();
    }
  }

  allSourcesPaused() {
    return Boolean(this.runtime.queues.sources.length) &&
      _.every(this.runtime.queues.sources, source =>
        _.some(this.flightLimiters, limiter =>
          !limiter.released && limiter.limit === 0 && scopeMatchesSource(limiter.scope, source)));
  }

  setRandom(random: () => number) {
    this.random = random;
  }
}

function makeRelease(release: () => void): ReleaseLimit {
  let released = false;
  return () => {
    if (released) return;
    released = true;
    release();
  };
}

function scopeFromRef(ref: NodeFire): RefScope {
  const url = new URL(ref.toString());
  return {
    origin: url.origin,
    path: url.pathname.replace(/^\/+|\/+$/g, ''),
    label: ref.toString()
  };
}

function scopeContains(container: RefScope | undefined, contained: RefScope | undefined) {
  if (!container) return true;
  if (!contained || container.origin !== contained.origin) return false;
  return !container.path || contained.path === container.path ||
    _.startsWith(contained.path, `${container.path}/`);
}

function scopeMatchesSource(scope: RefScope | undefined, source: QueueSource) {
  return scopeContains(scope, scopeFromRef(source.ref));
}
