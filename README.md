Firelease
=========

[![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](http://www.repostatus.org/badges/latest/active.svg)](http://www.repostatus.org/#active)

A Firebase queue consumer for Node with at-least-once and at-most-once semantics, fine-grained concurrency controls, and support for promises.  Built on top of [Nodefire](https://github.com/pkaminski/nodefire).

API
---

All durations can be specified as either a human-readable string, or a number of milliseconds.

The module exposes these functions:

```function attachWorker(refOrRefs, options, worker)```

Attaches a worker function to consume tasks from a queue.  You should normally attach no more
than one worker per path in any given process, but it's OK to run multiple processes on the same
paths concurrently.

* `@param {Nodefire | Nodefire[]} refOrRefs` A Nodefire ref, or an array of refs, to the queue roots
  in Firebase.  When multiple refs are supplied, their tasks form one logical queue: they use the
  same worker and share `maxConcurrent` and the other queue options.  The refs may point to
  different paths and databases.  Individual tasks will be children of these roots and must be
  objects.  Duplicate refs in the same array are ignored; as with the single-ref API, don't attach
  separate logical queues to the same path in one process.  The `_lease` key is reserved for use by
  Firelease in each task.

* `@param {Object} options` Optional options, supporting the following values:
  * `maxConcurrent: {number}` max number of tasks to handle concurrently for this worker.
  * `bufferSize: {number}` upper bound on how many tasks to keep buffered from each source and
    potentially go through leasing transactions in parallel.  It defaults to `Infinity`, which is
    preferred for both efficiency and correctness unless you know that the queue will spend most of
    its time above `settings.safeQueueSize`.  With `Infinity`, Firelease uses adaptive buffering: a
    shallow REST count chooses between a full listener that loads every task and a safe listener
    limited to `settings.safeQueueSize` tasks, and the source changes modes as the queue grows and
    shrinks.  Mode changes stop the old listener before starting its replacement; tasks already in
    progress keep running.  Set a finite value only for a queue expected to remain large most of the
    time, to keep it permanently on a limited listener.  An explicit finite value is used as-is and
    may be greater than `settings.safeQueueSize`, which applies only to adaptive buffers.
  * `minLease: {number | string}` minimum duration of each lease, which should equal the maximum
    expected time a worker will take to handle a task.
  * `maxLease: {number | string}` maximum duration of each lease; the lease duration is doubled each
    time a task fails until it reaches `maxLease`.
  * `preprocess: {function(Object):Object}` a function to use to preprocess each item during the
    leasing transaction.  This function must be fast, synchronous, idempotent, and should return the
    modified item (passed as the sole argument, OK to mutate).  One use for preprocessing is to
    clean up items written to a queue by a process outside your control (e.g., webhooks).
  * `healthyPingLatency: {number | string}` the maximum response latency to pings that is considered
    "healthy" for this queue.

* `@param {function(Object):RETRY | number | string | undefined}` worker The worker function that
  handles enqueued tasks.  It will be given a task object as argument, with a special $ref attribute
  set to the Nodefire ref of that task.  The worker can perform arbitrary computation whose duration
  should not exceed the queue's minLease value.  It can manipulate the task itself in Firebase as
  well, e.g. to delete it (to get at-most-once queue semantics) or otherwise modify it.  The worker
  can return any of the following:
  * undefined or null to cause the task to be retired from the queue.
  * firelease.RETRY to cause the task to be retried after the current lease expires (and reset the
    lease backoff counter).
  * A duration after which the task should be retried relative to when it was started.
  * An epoch in milliseconds greater than 1000000000000 at which the task should be tried.
  * A function that takes the task as argument and returns one of the values above.  This function
    will be executed in a transaction to ensure atomicity.
  All of these values can also be wrapped in a promise.


```function pingQueues(callback, interval)```

Sets up regular pinging of all queues.  Can be called either before or after workers are attached,
and will always ping all queues.  Can be called more than once to change the parameters.

* `@param {Function(Object) | null} callback` The callback to invoke with `firelease.stats` each
  time we ping all the queues.  If not specified, reports are silently dropped.

* `@param {number | string} interval` The interval at which to ping queues, to both check the
  current response latency and make sure no tasks are stuck.  Defaults to 1 minute.

```function blacklist(taskKey)```

Blacklist the given task key from ever being processed again.

* `@param {string} taskKey` The task key to blacklist.  This is the full Firebase URL of the task
  and can be obtained from an error using `error.firelease.itemKey`.

* `@return {boolean}` True if the task key was added to the list, false if it was already present.


```function extendLease(item, timeNeeded)```

Extends the lease on a task to give the worker more time to finish.  Checks a bunch of validity
constraints along the way and throws an error if the worker needs to abort.

 * `@param {Object} item` The original task object provided to a worker function.

 * `@param {number | string} timeNeeded` The minimum time needed counting from the current time.
   The actual lease may be extended by up to twice this amount, to prevent excessive churn.

 * `@return {Promise}` A promise that will be resolved when the lease has been extended, and
   rejected if something went wrong and the worker should abort.


```function shutdown()```

Shuts down firelease by refusing to take new tasks, and returns a promise that resolves once all currently running tasks have completed.

```function listTasksInProgress()```

Returns an array of the URLs of all tasks that are currently being worked on.

The module also exports a mutable `settings` object:

```settings.globalMaxConcurrent: {number}```

Set this to the maximum number of concurrent tasks being executed at any moment across all queues.

```settings.safeQueueSize: {number}```

The maximum number of tasks loaded by an adaptive safe-mode listener.  Defaults to 6,000.  Adaptive
sources enter full mode only below `safeQueueSize * 0.85` and demote from full mode above
`safeQueueSize`.  Configure this before attaching workers.

```settings.queueCheckInterval: {number | string}```

How often safe-mode sources enqueue a shallow REST count check, with 5% random jitter.  Defaults to
5 minutes.

```settings.captureError: {function(Error)}```

A function used to capture errors.  Defaults to logging the stack to the console, but you may want to change it to something else in production.  The function should take a single exception argument.

```defaults: {Object}```

Mutable default option values for all subsequent attachWorker calls.  See that function for details.

```stats: {Object}```

The live stats object also passed to the ping callback.  Global fields include `healthy`,
`sickQueues`, `sickSources`, `stuckTasks`, `maxLatency`, and `tasksAcquired`.  Each entry in `queues`
includes its own health, latency, acquisition count, and all physical `sources`.  Source stats
include `connected`, current `mode` (`full` or `safe`), last known `size`, `sizeTimestamp` when the
size came from a shallow REST check, ping health and latency, and the latest structured error when
present.  Queue size is reported only for adaptive sources; it remains `null` for sources with an
explicit finite `bufferSize` because a limited listener cannot determine the true queue size.
