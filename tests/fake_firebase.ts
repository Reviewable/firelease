import assert from 'node:assert';
import type NodeFire from 'nodefire';
import type {TransactionMetadata} from 'nodefire';

interface FakeLease {
  [key: string]: unknown;
}

interface FakeTaskValue {
  _lease?: FakeLease;
  [key: string]: unknown;
}

type Listener = {callback: (snapshot: FakeSnapshot) => void, context?: unknown};
type ConnectionListener = {
  callback: (snapshot: {val(): boolean}) => void;
  context?: unknown;
};

class FakeSnapshot {
  constructor(readonly ref: FakeTaskRef) {}

  val() {
    return clone(this.ref.value);
  }
}

class FakeDatabaseRoot {
  readonly database;
  connected = true;
  connectionListeners: ConnectionListener[] = [];

  constructor(readonly name: string) {
    this.database = {
      app: {name}
    };
  }

  child(path: string) {
    assert.strictEqual(path, '.info/connected');
    return {
      on: (
        event: string,
        callback: ConnectionListener['callback'],
        cancelCallback?: (error: Error) => void,
        context?: unknown
      ) => {
        assert.strictEqual(event, 'value');
        this.connectionListeners.push({callback, context});
        callback.call(context, {val: () => this.connected});
      },
      off: (event: string, callback: ConnectionListener['callback'], context?: unknown) => {
        assert.strictEqual(event, 'value');
        this.connectionListeners = this.connectionListeners.filter(
          listener => listener.callback !== callback || listener.context !== context);
      }
    };
  }

  setConnected(connected: boolean) {
    this.connected = connected;
    for (const listener of this.connectionListeners) {
      listener.callback.call(listener.context, {val: () => connected});
    }
  }

  isEqual(other: unknown) {
    return other === this;
  }
}

export class FakeTaskRef {
  value: FakeTaskValue | null = null;

  constructor(readonly queueRef: FakeQueueRef, readonly key: string) {}

  get root() {
    return this.queueRef.root;
  }

  get database() {
    return this.queueRef.database;
  }

  get now() {
    return this.queueRef.now;
  }

  toString() {
    return `${this.queueRef}/${this.key}`;
  }

  transaction(
    update: (value: FakeTaskValue | null) => FakeTaskValue | null | undefined
  ) {
    this.queueRef.beforeTransaction?.(this);
    const metadata: TransactionMetadata = {
      outcome: this.queueRef.transactionError ? 'error' : 'commit',
      tries: this.queueRef.transactionTries,
      duration: this.queueRef.transactionDuration
    };
    if (this.queueRef.transactionError) {
      return Object.assign(Promise.reject(this.queueRef.transactionError), {transaction: metadata});
    }
    const previous = clone(this.value);
    const updated = update(clone(this.value));
    if (updated !== undefined) this.value = clone(updated);
    this.queueRef.notifyTaskChange(this, previous);
    return Object.assign(Promise.resolve(clone(this.value)), {transaction: metadata});
  }

  get() {
    return Promise.resolve(clone(this.value));
  }

  remove() {
    const previous = clone(this.value);
    this.value = null;
    this.queueRef.notifyTaskChange(this, previous);
    return Promise.resolve();
  }

  child(path: string) {
    return {
      set: (value: unknown) => {
        if (path === '_lease/busy' && this.value && this.value._lease) {
          this.value._lease.busy = value;
        }
        return Promise.resolve();
      }
    };
  }
}

class FakeQuery {
  readonly listeners: Record<string, Listener[]> = {};
  knownKeys = new Set<string>();
  limit = Infinity;

  constructor(readonly queueRef: FakeQueueRef) {
    queueRef.queries.push(this);
  }

  orderByChild(path: string) {
    assert.strictEqual(path, '_lease/expiry');
    return this;
  }

  limitToFirst(limit: number) {
    this.limit = limit;
    return this;
  }

  on(
    event: string,
    callback: Listener['callback'],
    cancelCallback?: (error: Error) => void,
    context?: unknown
  ) {
    this.listeners[event] ??= [];
    this.listeners[event].push({callback, context});
    if (event === 'child_added') {
      const selected = this.selectedTasks();
      this.knownKeys = new Set(selected.map(task => task.key));
      for (const task of selected) callback.call(context, new FakeSnapshot(task));
    } else if (event === 'value') {
      this.queueRef.dispatchValue(callback, context);
    }
  }

  off(event: string, callback: Listener['callback'], context?: unknown) {
    this.listeners[event] = (this.listeners[event] ?? []).filter(
      listener => listener.callback !== callback || listener.context !== context);
  }

  selectedTasks() {
    return Object.values(this.queueRef.tasks)
      .filter(task => task.value !== null)
      .sort((left, right) => {
        const leftExpiry = Number(left.value?._lease?.expiry ?? 0);
        const rightExpiry = Number(right.value?._lease?.expiry ?? 0);
        return leftExpiry - rightExpiry || left.key.localeCompare(right.key);
      })
      .slice(0, this.limit);
  }

  reconcile(changedTask?: FakeTaskRef) {
    const selected = this.selectedTasks();
    const nextKeys = new Set(selected.map(task => task.key));
    for (const key of this.knownKeys) {
      if (!nextKeys.has(key)) {
        this.emit('child_removed', new FakeSnapshot(this.queueRef.child(key)));
      }
    }
    for (const task of selected) {
      if (!this.knownKeys.has(task.key)) {
        this.emit('child_added', new FakeSnapshot(task));
      } else if (changedTask?.key === task.key) {
        this.emit('child_moved', new FakeSnapshot(task));
      }
    }
    this.knownKeys = nextKeys;
  }

  emit(event: string, snapshot: FakeSnapshot) {
    for (const listener of [...(this.listeners[event] ?? [])]) {
      listener.callback.call(listener.context, snapshot);
    }
  }
}

export class FakeQueueRef {
  readonly databaseRoot: FakeDatabaseRoot;
  readonly database;
  readonly key: string;
  readonly listeners: Record<string, Listener[]> = {};
  readonly tasks: Record<string, FakeTaskRef> = {};
  readonly queries: FakeQuery[] = [];
  deferredValueCallbacks: {callback: Listener['callback'], context?: unknown}[] = [];
  deferNextValue = false;
  childrenKeysError?: Error;
  childrenKeysCountOverride?: number;
  childrenKeysCalls = 0;
  listenerError?: Error;
  transactionError?: Error;
  transactionTries = 1;
  transactionDuration = 0;
  beforeTransaction?: (ref: FakeTaskRef) => void;
  fixedNow?: number;

  constructor(databaseName: string, readonly path: string) {
    this.databaseRoot = new FakeDatabaseRoot(databaseName);
    this.database = this.databaseRoot.database;
    this.key = path.split('/').pop()!;
  }

  get root() {
    return this.databaseRoot;
  }

  get now() {
    return this.fixedNow ?? Date.now();
  }

  toString() {
    return `https://${this.databaseRoot.name}.example.test/${this.path}`;
  }

  orderByChild(path: string) {
    assert.strictEqual(path, '_lease/expiry');
    return new FakeQuery(this);
  }

  get() {
    return Promise.resolve(null);
  }

  childrenKeys(options?: {timeout?: number}) {
    assert.strictEqual(options?.timeout, 15_000);
    this.childrenKeysCalls++;
    if (this.childrenKeysError) return Promise.reject(this.childrenKeysError);
    if (this.childrenKeysCountOverride !== undefined) {
      return Promise.resolve(
        Array.from({length: this.childrenKeysCountOverride}, (_, index) => `count-${index}`));
    }
    return Promise.resolve(
      Object.entries(this.tasks).filter(([, task]) => task.value !== null).map(([key]) => key));
  }

  child(key: string) {
    if (!this.tasks[key]) this.tasks[key] = new FakeTaskRef(this, key);
    return this.tasks[key];
  }

  on(
    event: string,
    callback: Listener['callback'],
    cancelCallback?: (error: Error) => void,
    context?: unknown
  ) {
    if (this.listenerError) throw this.listenerError;
    this.listeners[event] ??= [];
    this.listeners[event].push({callback, context});
    if (event === 'child_added') {
      for (const task of Object.values(this.tasks)) {
        if (task.value !== null) callback.call(context, new FakeSnapshot(task));
      }
    } else if (event === 'value') {
      this.dispatchValue(callback, context);
    }
  }

  off(event: string, callback: Listener['callback'], context?: unknown) {
    this.listeners[event] = (this.listeners[event] ?? []).filter(
      listener => listener.callback !== callback || listener.context !== context);
  }

  addTask(key: string, value: FakeTaskValue) {
    const ref = this.child(key);
    const previous = clone(ref.value);
    ref.value = clone(value);
    this.notifyTaskChange(ref, previous);
    return ref;
  }

  emit(event: string, snapshot: FakeSnapshot) {
    for (const listener of [...(this.listeners[event] ?? [])]) {
      listener.callback.call(listener.context, snapshot);
    }
  }

  notifyTaskChange(task: FakeTaskRef, previous: FakeTaskValue | null) {
    if (previous === null && task.value !== null) {
      this.emit('child_added', new FakeSnapshot(task));
    } else if (previous !== null && task.value === null) {
      this.emit('child_removed', new FakeSnapshot(task));
    } else if (task.value !== null) {
      this.emit('child_changed', new FakeSnapshot(task));
    }
    for (const query of this.queries) query.reconcile(task);
  }

  dispatchValue(callback: Listener['callback'], context?: unknown) {
    if (this.deferNextValue) {
      this.deferNextValue = false;
      this.deferredValueCallbacks.push({callback, context});
      return;
    }
    callback.call(context, new FakeSnapshot(this.child('__queue_value__')));
  }

  releaseDeferredValues() {
    const callbacks = this.deferredValueCallbacks.splice(0);
    for (const {callback, context} of callbacks) {
      callback.call(context, new FakeSnapshot(this.child('__queue_value__')));
    }
  }
}

function clone<T>(value: T): T {
  return value === null || value === undefined ? value : JSON.parse(JSON.stringify(value));
}

export function waitFor(predicate: () => unknown, timeout = 2000) {
  const start = performance.now();
  return new Promise<void>((resolve, reject) => {
    function check() {
      if (predicate()) {
        resolve();
      } else if (performance.now() - start >= timeout) {
        reject(new Error('Timed out waiting for condition'));
      } else {
        setTimeout(check, 10);
      }
    }
    check();
  });
}

export function asNodeFire(ref: FakeQueueRef) {
  return ref as unknown as NodeFire;
}
