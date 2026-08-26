Firelease
=========

[![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](http://www.repostatus.org/badges/latest/active.svg)](http://www.repostatus.org/#active)

A Firebase queue consumer for Node with at-least-once and at-most-once semantics, fine-grained admission controls, and support for promises.  Built on top of [Nodefire](https://github.com/pkaminski/nodefire).

API
---

All durations can be specified as either a human-readable string, or a number of milliseconds.

Firelease 5 requires NodeFire 7.2 or newer and an explicit `start()` call after workers are attached.
The previous
`maxConcurrent` queue option and `settings.globalMaxConcurrent` have been replaced by the composable
`limitTasksInFlight()` control.  Task starts are automatically rate-controlled per Firebase
database from observed write latency.

The module exposes these functions:

```function attachWorker(refOrRefs, options, worker)```

Attaches a worker function to consume tasks from a queue.  You should normally attach no more
than one worker per path in any given process, but it's OK to run multiple processes on the same
paths concurrently.

* `@param {Nodefire | Nodefire[]} refOrRefs` A Nodefire ref, or an array of refs, to the queue roots
  in Firebase.  When multiple refs are supplied, their tasks form one logical queue: they use the
  same worker and queue options.  The refs may point to
  different paths and databases.  Individual tasks will be children of these roots and must be
  objects.  Duplicate refs in the same array are ignored; as with the single-ref API, don't attach
  separate logical queues to the same path in one process.  The `_lease` key is reserved for use by
  Firelease in each task.

* `@param {Object} options` Optional options, supporting the following values:
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
  * `priority: {number}` positive relative task-start priority.  Values greater than or equal to 1
    identify foreground queues.  Values below 1 identify background queues, which are considered
    only when no foreground queue has an admissible task.  Selection within either tier is random
    and proportional to priority.  Defaults to 1.
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
  * `captureLeaseTransactionMetrics: {function(string, number, number)}` a callback invoked after
    each acquired, contended, or failed task lease transaction.  It receives the acquisition
    outcome, NodeFire transaction tries, and transaction duration in milliseconds.  Missing optional
    NodeFire metadata is reported as zero.  The callback must be synchronous.  Callback errors are
    reported through `settings.captureError` and do not affect task processing.

* `@param {function(Object):RETRY | number | string | undefined}` worker The worker function that
  handles enqueued tasks.  It will be given a task object as argument, with a special $ref attribute
  set to the Nodefire ref of that task.  On a task's first acquisition, the worker-facing `_lease`
  object also has a non-enumerable `firstAcquisition: true` property that is not saved to Firebase;
  the property is absent on subsequent acquisitions.  The worker can perform arbitrary computation
  whose duration should not exceed the queue's minLease value.  It can manipulate the task itself in
  Firebase as well, e.g. to delete it (to get at-most-once queue semantics) or otherwise modify it.
  The worker can return any of the following:
  * undefined or null to cause the task to be retired from the queue.
  * firelease.RETRY to cause the task to be retried after the current lease expires (and reset the
    lease backoff counter).
  * A duration after which the task should be retried relative to when it was started.
  * An epoch in milliseconds greater than 1000000000000 at which the task should be tried.
  * A function that takes the task as argument and returns one of the values above.  This function
    will be executed in a transaction to ensure atomicity.
  All of these values can also be wrapped in a promise.

`attachWorker()` only registers the queue.  Attach every worker before calling `start()`.

```function start()```

Starts Firelease and returns a promise that resolves after every attached source has connected and
loaded its initial listener.  No task is dispatched until all initial sources are loaded.  Queues are
initialized in attachment order.  Calling `start()` more than once returns the same promise;
attaching another worker after startup begins throws an error.

For each available task-start opportunity, Firelease finds logical queues with an admissible source:
one that is connected and loaded, has a ready task and in-flight capacity, and whose database is
ready to admit another start.  If any foreground queues are eligible, it selects one randomly in
proportion to their priorities.  Otherwise it selects proportionally from eligible background
queues.  Sources within the selected logical queue remain round-robin.  Queue selection has no
memory, so an idle queue neither accumulates credit nor incurs a penalty when work arrives later.

On a Firebase disconnect, every source using that database stops its listener.  Once the database
reconnects, those sources reload in attachment order before they resume dispatching.  Sources on
other databases remain available throughout.

A failed task lease transaction is retried with an uncapped exponential backoff starting at 1
second.  The first two consecutive failures are not reported; reporting begins with the third
failure.  Any completed lease transaction resets the failure streak.

```function limitTasksInFlight(limit, ref)```

Limits the number of tasks between the start of their lease transaction and the completion of all
worker and post-processing work.  The optional second argument scopes the limit to queue sources at
or below that Firebase ref; without a ref it covers every current and future source.  Limits compose,
so every global, ancestor, and exact-source limit must admit a task.  Passing 0 pauses matching
sources.  Lowering a limit never interrupts a task that has already started, and the returned
idempotent release function removes only that limit.

```function waitForTasksInFlight(timeout, ref)```

Resolves once the actual number of tasks in flight in the optional subtree is at or below its
current effective limit.  `timeout` is required.  A timeout rejects with the constant message
`Firelease tasks-in-flight wait timed out`; count, limit, timeout, scope, and the distinct
`tasks-in-flight-wait-timeout` code are available in `error.firelease`.


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

Permanently shuts down Firelease by applying a global zero in-flight limit, and returns a promise
that resolves once all running tasks complete.  Running tasks may continue extending their leases
and performing post-processing while Firelease drains.

```function listTasksInProgress()```

Returns an array of the URLs of all tasks that are currently being worked on.

The module also exports a mutable `settings` object:

```settings.safeQueueSize: {number}```

The maximum number of tasks loaded by an adaptive safe-mode listener.  Defaults to 6,000.  Adaptive
sources enter full mode only below `safeQueueSize * 0.85` and demote from full mode above
`safeQueueSize`.  Configure this before calling `start()`.

```settings.queueCheckInterval: {number | string}```

How often safe-mode sources enqueue a shallow REST count check, with 5% random jitter.  Defaults to
5 minutes.  Checks, promotions, and demotions share one serialized in-process queue; a source's
next scheduled check is measured from the completion of its previous check.

```settings.queueLoadTimeout: {number | string}```

How long Firelease waits for a queue listener's initial value before treating the load as stalled.
Defaults to 1 minute.  A stalled full-mode load falls back to safe mode; a stalled safe-mode load
is fatal because no bounded listener is available to process the queue.

```settings.writeLatencyTarget: {number | string}```

The target Firebase write latency used to control task starts.  Defaults to 2 seconds.  Firelease
installs a NodeFire completion observer and creates one controller for every Firebase database used
by an attached source.  Each controller observes every NodeFire write to its database, including
writes outside Firelease.  Transaction samples use the average duration per try.

After enough fresh samples have arrived, a controller halves its task-start rate when its write
latency EMA exceeds the target.  When latency is below 75% of the target, it adds one task per second.
The rate remains unchanged inside that hysteresis band.  Each adjustment starts a fresh observation
window, so operations already in flight cannot cause consecutive adjustments.  Firebase latency
errors such as timeouts contribute high-latency samples; permanent errors such as permission or
validation failures do not.

```settings.initialTaskStartRate: {number}```

The task-start rate per database, in tasks per second, used after startup and each reconnect.
Defaults to 5.  The controller uses a one-token bucket, so starts are smoothed rather than released
in bursts.

```settings.maxTaskStartRate: {number}```

The maximum task-start rate per database, in tasks per second.  Defaults to 50.  The internal
minimum is 0.1 tasks per second so that an overloaded database can recover without stopping queue
progress altogether.

```settings.taskStartRateAdjustmentInterval: {number | string}```

The minimum time between control decisions, with 5% random jitter.  Defaults to 5 seconds.

```settings.taskStartRateMinSamples: {number}```

The minimum number of fresh write samples required for a control decision.  Defaults to 20.

```settings.captureError: {function(Error)}```

A function used to capture errors.  Defaults to logging the stack to the console, but you may want to change it to something else in production.  The function should take a single exception argument.

```defaults: {Object}```

Mutable default option values for all subsequent attachWorker calls.  See that function for details.

```stats: {Object}```

The live stats object also passed to the ping callback.  Its `databases` array contains each
database root, current `taskStartRate`, write latency EMA, and lifetime `writeSamples`.  Global fields
include `healthy`,
`sickQueues`, `sickSources`, `stuckTasks`, `maxLatency`, `leaseTransactions`, and the legacy
`tasksAcquired`.  `leaseTransactions` contains lifetime `acquired`, `contended`, `failed`, and
`tries` counts for task lease transactions.  `tries` includes failed transactions and comes from
NodeFire transaction metadata.  `duration` is an exponential moving average of the NodeFire
transaction duration in milliseconds, using an alpha of 0.1.  These stats are available for every
physical source.  Logical queue and global counts are additive, while their duration is the average
of the underlying source or queue duration values that have recorded at least one attempt.  The
legacy `tasksAcquired` field also remains
lifetime-cumulative.  Each entry in `queues` includes its configured priority, health, latency,
leasing totals, and all physical `sources`.  Queue-level
`size` and `sizeDelta` sum their source values when all are known, `sizeTimestamp` is the oldest
source timestamp, and `mode` is `full`, `safe`, or `mixed`.  Source stats include `connected`,
current `mode` (`full` or `safe`), last known `size`, `sizeTimestamp` when the size came from a
shallow REST check, `sizeDelta` when the safe listener was below its limit (the REST count minus its
buffered task count), and ping health and latency.  Queue size is reported only for adaptive
sources; it remains `null` for sources with an explicit finite `bufferSize` because a limited
listener cannot determine the true queue size.

```state: 'initializing' | 'running' | 'paused' | 'shutting-down'```

A live getter for the current lifecycle state.  It is `initializing` while initial or reconnecting
sources load, `paused` when zero in-flight limits suspend every attached source,
`running` otherwise after the required listeners load, and permanently `shutting-down` after
`shutdown()` begins.
Firelease rechecks scheduler state at least once per minute even if no ordinary timer wakes it.
