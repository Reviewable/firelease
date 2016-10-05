Firelease
=========

A Firebase queue consumer for Node with at-least-once and at-most-once semantics, fine-grained concurrency controls, and support for promises and generators.  Built on top of [Nodefire](https://github.com/pkaminski/nodefire).

API
---

All durations can be specified as either a human-readable string, or a number of milliseconds.

The module exposes these functions:

```function attachWorker(ref, options, worker)```

Attaches a worker function to consume tasks from a queue.  You should normally attach no more
than one worker per path in any given process, but it's OK to run multiple processes on the same
paths concurrently.  If you do, you probably want to set `maxLeaseDelay` to something greater
than zero, to properly balance task distribution between the processes.

* `@param {Nodefire} ref` A Nodefire ref to the queue root in Firebase.  Individual tasks will be
  children of this root and must be objects.  The `_lease` key is reserved for use by
  Firelease in each task.

* `@param {Object} options` Optional options, supporting the following values:
  * `maxConcurrent: {number}` max number of tasks to handle concurrently for this worker.
  * `bufferSize: {number}` upper bound on how many tasks to keep buffered and potentially go
    through leasing transactions in parallel; not worth setting higher than `maxConcurrent`,
    or higher than about 10.
  * `minLease: {number | string}` minimum duration of each lease, which should equal the maximum
    expected time a worker will take to handle a task.
  * `maxLease: {number | string}` maximum duration of each lease; the lease duration is doubled each
    time a task fails until it reaches `maxLease`.
  * `leaseDelay: {number | string}` duration by which to delay leasing an item after it becomes
    available; useful for setting up "backup" servers that only grab tasks that aren't taken up fast
    enough by the primary.
  * `maxLeaseDelay: {number | string}` if non-zero, enables automatic `leaseDelay` adjustment and
    sets the maximum duration to wait before attempting to acquire a ready task.  This is often
    necessary to compensate for differences in machine or network speed, or for Firebase's
    consistent order for sending event notifications to multiple clients.
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
  All of these values can also be wrapped in a promise or a generator, which will be dealt with
  appropriately.


```function pingQueues(callback, interval)```

Sets up regular pinging of all queues.  Can be called either before or after workers are attached,
and will always ping all queues.  Can be called more than once to change the parameters.

* `@param {Function(Object) | null} callback` The callback to invoke with a report each time we ping
  all the queues.  The report looks like: `{healthy: true, maxLatency: 1234}`.  If not
  specified, reports are silently dropped.

* `@param {number | string} interval` The interval at which to ping queues, to both check the
  current response latency and make sure no tasks are stuck.  Defaults to 1 minute.


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

There are also some module-level settings you can change:

```globalMaxConcurrent: {number}```

Set this to the maximum number of concurrent tasks being executed at any moment across all queues.

```defaults: {Object}```

Default option values for all subsequent attachWorker calls.  See that function for details.

```captureError: {function(Error)}```

A function used to capture errors.  Defaults to logging the stack to the console, but you may want to change it to something else in production.  The function should take a single exception argument.
