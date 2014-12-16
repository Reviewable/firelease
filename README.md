Firelease
=========

A Firebase queue consumer for Node with at-least-once and at-most-once semantics, fine-grained concurrency controls, and support for promises and generators.  Built on top of [Nodefire](https://github.com/pkaminski/nodefire).

API
---

The module exposes one function:

```function attachWorker(ref, options, worker)```

Attaches a worker function to consume tasks from a queue.  You should normally attach no more
than one worker per path in any given process, but it's OK to run multiple processes on the same
paths concurrently.

* `@param {Nodefire} ref` A Nodefire ref to the queue root in Firebase.  Individual tasks will be
  children of this root and must be objects.  The `_lease` key is reserved for use by
  Firelease in each task.

* `@param {Object} options` Optional options, supporting the following values:
  * `maxConcurrent: {number}` max number of tasks to handle concurrently for this worker.
  * `bufferSize: {number}` upper bound on how many tasks to keep buffered and potentially go
    through leasing transactions in parallel; not worth setting higher than `maxConcurrent`,
    or higher than about 10.
  * `minLease: {number | string}` minimum duration of each lease, which should equal the maximum
    expected time a worker will take to handle a task; specified as either a number of
    milliseconds, or a human-readable duration string.
  * `maxLease: {number | string}` maximum duration of each lease, same format as `minLease`; the
    lease duration is doubled each time a task fails until it reaches `maxLease`.

* `@param {function(Object):RETRY | number | string | undefined} worker` The worker function that
  handles enqueued tasks.  It will be given a task object as argument, with a special `$ref`
  attribute set to the Nodefire ref of that task.  The worker can perform arbitrary
  computation whose duration should not exceed the queue's `minLease` value.  It can
  manipulate the task itself in Firebase as well, e.g. to delete it (to get at-most-once
  queue semantics) or otherwise modify it.  The worker can return `RETRY` to cause the task to
  be retried after the current lease expires (and reset the lease backoff counter), or a
  duration after which the task should be retried relative to when it was started (as either
  a number of milliseconds or a human-readable duration string).  If the worker returns
  nothing then the task is considered completed and removed from the queue.  All of these
  values can also be wrapped in a promise or a generator, which will be dealt with
  appropriately.

There are also some module-level settings you can change:

```globalMaxConcurrent: {number}```

Set this to the maximum number of concurrent tasks being executed at any moment across all queues.

```defaults: {Object}```

Default option values for all subsequent attachWorker calls.  See that function for details.

```captureError: {function(Error)}```

A function used to capture errors.  Defaults to logging the stack to the console, but you may want to change it to something else in production.  The function should take a single exception argument.
