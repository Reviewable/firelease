Firelease
=========

[![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](http://www.repostatus.org/badges/latest/active.svg)](http://www.repostatus.org/#active)

A Firebase queue consumer for Node with at-least-once and at-most-once semantics, fine-grained concurrency controls, and support for promises.  Built on top of [Nodefire](https://github.com/pkaminski/nodefire).

Install NodeFire 7.2.0 or later in the 7.x series alongside Firelease. It is a peer dependency so
the application and Firelease share one NodeFire instance and its operation interceptors.
Since Firelease 4.5.0, applications must declare `nodefire: "^7.2.0"` (or a compatible 7.x version)
explicitly instead of relying on Firelease to install it. Package managers that do not install
peers automatically otherwise leave NodeFire missing, causing Firelease to fail at startup.
This shared instance enables the per-database write statistics and optional
`settings.captureWriteMetrics` callback documented below.

Affinity jitter
---------------

Enable `{leaseJitter: true}` on queues that can tolerate a small pickup delay. Each process hashes
its startup identity with the string returned by the optional `getAffinity(task)` queue option,
or the full task URL when the hook is absent or returns `null`/`undefined`.
Related tasks with the same affinity tend to run on the same process; different affinities can
prefer different processes. This is a preference, not ownership, fairness, or serialization. Busy or disconnected
processes do not prevent others from acquiring tasks. A process restart changes its identity.

For example, use the same hook across queues handling a pull request:

```js
getAffinity: task => `${task.owner}|${task.repo}|${task.prNumber}`
```

The hook receives task snapshots before `preprocess` runs, with the same non-enumerable, read-only
`$ref` property as worker items. Use `task.$ref.key` for the child key or `task.$ref.toString()` for
the URL; keys retain NodeFire escaping. Hooks using `$ref` require Firelease 4.6.1 or later (declare
`firelease: "^4.6.1"`, not `"^4.6.0"`). Unlike worker items, these snapshots need not have a lease
and do not expose `$leaseTimeRemaining` or acquired-lease guarantees.
The hook must be fast, synchronous, deterministic, and not mutate the task. It may be called again
on updates; pending deadlines remain stable, and new lease generations use the latest affinity.
Errors are reported through `captureError` and fall back to the task URL. No affinity field is persisted or
reserved in `_lease`, so older tasks work without a schema change and replacement leases need no
special handling. Only jitter-enabled, non-ping tasks call the hook.
Every enabled source on a process uses the same identity, but learns its own delay
window, so affinity across sources is approximate.
Different learned windows can favor one idle worker across many affinities; at a zero window the
hash no longer adds a preference. Local capacity limits still let other workers pick up work.

Jitter applies to live arrivals and lease expiries. Initial backlog, overdue tasks, retries after
transaction failures, and work held up by local concurrency limits run without additional jitter.
Each eligibility period gets one deadline; ordinary updates and scans cannot postpone it. Waiting
does not reserve a worker slot. An observed competing lease/deletion cancels a pending transaction;
Firebase transactions still provide the correctness guarantee when races remain. Disconnection,
blacklisting, and listener replacement cancel waits without treating them as contention feedback.

The window starts at 250 ms and stays between 0 and 5 s. Contended jittered attempts increase it;
successful and suppressed attempts decrease it, targeting a 5% race rate among all three outcomes.
A lone worker therefore converges to zero delay; a subsequent collision raises the window again.
Successful acquisitions and observed lease broadcasts estimate propagation time. Failures do not
train the controller. A window is the maximum possible delay, not the expected winning delay; with
many idle workers the earliest hashed deadline is typically much smaller. This trades pickup latency for
fewer transactions and is most useful on lightly loaded queues with many idle consumers.

Pass an object to customize the controller, e.g.
`{leaseJitter: {targetRaceRate: 0.01, minWindow: 0, maxWindow: '2s'}}`.
Bounds accept numbers of milliseconds or human-readable strings such as `'100ms'` or `'2s'`.
After parsing they must be finite and nonnegative with `minWindow <= maxWindow`; the target
must be strictly between zero and one. Missing fields use the defaults above. The initial 250 ms
window is clamped to the requested bounds, not scaled with them. A lower target reduces the decay
per uncontended opportunity and slightly increases growth per race: it usually trades a larger
window for fewer collisions, but the cap and changing workloads can prevent reaching the target.
Setting both bounds to zero retains observation/feedback but never delays acquisition.

The update is `window += gain * propagation * (raced - targetRaceRate)`, clamped to the bounds.
`raced` is one for a contended attempt, zero for a win or suppression. `propagation` is a 0.1-alpha
EMA in milliseconds, initially 100 ms, estimated from successful transaction time per try
(excluding prefetch) or the delay in observing a competing lease.

Gain is `min(2, 40 / rate)`, where `rate` estimates eligible feedback opportunities per second,
including wins, races, and suppressions, but excluding failures and bypassed work. An exponentially
decaying count is updated on each observation: `count = count * exp(-elapsedMs / 5000) + 1`, then
`rate = count / 5`. Elapsed time uses a monotonic process clock. The current observation is included
before computing gain, so simultaneous bursts are counted too. No background timer runs, and idle
windows are never aged or reset. A pause forgets old rate evidence only when feedback resumes;
the gain cap prevents a large catch-up step.

Below about 20 eligible opportunities/s, gain remains 2: at 100 ms propagation a race adds 190 ms
and a clean opportunity removes 10 ms. Above that rate, observations share approximately 40 units
of gain per second rather than making adaptation arbitrarily faster with traffic. Quiet queues
remain sample-limited. The five-second smoothing and conservative cap preserve the existing
low-rate behavior; they are internal tuning constants, not new queue options. This is a response
normalization, not a promise of lower contention or latency on every workload. Queue-specific
latency policy still belongs in the window bounds and race target.

The [gain experiments](evidence/lease-jitter-gain.md) compare fixed gain with normalized candidates,
including latency and transaction-cost tradeoffs. They do not establish a universal optimum.

Ping tasks bypass jitter and never train the controller or enter application handlers or
application metric callbacks. Ping latency measures queue responsiveness without the intentional
acquisition delay; it is not a measurement of jittered task pickup latency.

Enabled sources expose `leaseJitter: {scheduled, suppressed, window}` in stats. The counts are
lifetime totals; queue/global rollups sum them and average windows across enabled children. Window
and scheduled counts are available for logs and diagnostics; exporting every value as a separate
metric is unnecessary. Measure amplification from existing transaction counts and use the jitter
callback for an aggregated suppression count and sampled pickup timing. Do not put affinity or
task keys in metric attributes.
Suppression counts are confirmed observations, not a complete count of avoided transactions.
Limited queries report both deletions and query-window evictions using the old child snapshot;
ambiguous removals cancel pending waits but do not train the controller or increment suppression.
Confirming each removal would require extra reads on losing
instances. Full listeners do not have this ambiguity.

On-demand live testing
----------------------

`yarn test` uses local fakes and never connects to Firebase. The separate live harness requires a
test database URL, service-account credentials, and an explicit `--live` flag:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/test-service-account.json \
  yarn test:live --live --database https://your-test-project-default-rtdb.firebaseio.com \
  --workers 8 --tasks 120 --output live-results.json
```

The harness launches independent Node processes using the installed NodeFire/Admin SDK, compares
baseline and adaptive contention, and checks recurring affinity, busy/disconnected fallback,
retries, extension, pings, and backlog replay with full and limited listeners. It checks first-acquisition
flags and duplicate processing, saves actual counts and pickup percentiles, and removes only its
UUID-scoped `/firelease-tests` subtree. It never changes security rules. Admin credentials work
with `.read` and `.write` both `false`. An optional `_lease/expiry` index on test queue paths avoids
limited-query warnings; the small fixtures also work without it.

Defaults use eight workers plus one coordinator; hard bounds are 20 workers and 300 tasks per
phase. The default comparison enqueues small tasks sequentially. Add `--compare-gain` for paired
fixed/normalized trials with independent timed arrivals, rate changes, bursts, scale-down to the
highest-window survivor, and saturation. `--reverse` reverses mode order. Modes share worker
identities but never mix controllers on the same queue. Experimental gain diagnostics stay in the
local report, not application metrics. Use a dedicated test project, keep credentials outside the
checkout, and do not run this in ordinary CI. Abrupt termination can leave a UUID subtree for manual
cleanup.

`yarn test:simulation --seeds 16 --offset 100 --candidates fixed,default --output gain.json`
runs the deterministic closed-loop model without Firebase. It uses actual controller/scheduling
code, paired seeded arrival/network traces, and simplified server/capacity behavior. It does not
model Firebase retries or application caches; validate finalists live rather than treating its
transaction counts or latency percentiles as production predictions.

API
---

Lease and ping durations, including jitter window bounds, can be human-readable strings or
numbers of milliseconds.

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
  * `captureLeaseTransactionMetrics: {function(string, number, number)}` a callback invoked after
    each acquired, contended, or failed task lease transaction.  It receives the acquisition
    outcome, NodeFire transaction tries, and transaction duration in milliseconds.  Missing optional
    NodeFire metadata is reported as zero.  The callback must be synchronous.  Callback errors are
    reported through `settings.captureError` and do not affect task processing.
  * `leaseJitter: {boolean | Object}` opt into adaptive affinity jitter. Defaults to `false`.
    `true` uses the defaults; an object customizes `targetRaceRate`, `minWindow`, and `maxWindow`.
  * `getAffinity: {function(Object): string | null | undefined}` derive affinity from an unprocessed
    task snapshot decorated with a non-enumerable, read-only `$ref` (since 4.6.1).
    Its `$ref.key` retains NodeFire escaping; use `NodeFire.unescape` on key components if needed.
    The lease is optional here; only worker items have acquired-lease guarantees.
    Nullish results or errors fall back to the full task URL. The hook must be
    fast, synchronous, deterministic, and must not mutate the task. It is only called for
    jitter-enabled application tasks, never pings.
  * `captureLeaseJitterMetrics: {function(Object)}` a synchronous callback receiving either
    `{outcome: 'suppressed'}` when a pending attempt is canceled by an observed lease/deletion,
    or `{outcome: 'acquired', pickupDelay}` after a jittered acquisition. `pickupDelay` is the
    time in milliseconds from locally observed eligibility to completion of acquisition, including
    the transaction (new arrivals start at receipt; scheduled leases start at their expiry).
    Callback errors do not affect scheduling. The callback itself is not sampled; aggregate
    counters and sample timing distributions in the consuming application.

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


```function getCurrentTask()```

Returns the current worker's task, including its lease and `$ref`, through asynchronous calls.
Returns `undefined` outside a worker or after that worker finishes. Concurrent workers have
independent contexts. Available as both a named export and `firelease.getCurrentTask()`.

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

```settings.queueLoadTimeout: {number | string}```

How long Firelease waits for a queue listener's initial value before treating the load as stalled.
Defaults to 1 minute.  A stalled full-mode load falls back to safe mode; a stalled safe-mode load
is fatal because no bounded listener is available to process the queue.

```settings.captureError: {function(Error)}```

A function used to capture errors.  Defaults to logging the stack to the console, but you may want to change it to something else in production.  The function should take a single exception argument.

```settings.captureWriteMetrics: {function(WriteMetric) | undefined}```

Optional synchronous callback for every observed write to a database used by an attached worker,
including writes outside queue paths. Requires NodeFire 7.2.0 or later. Each metric includes the
database root URL (`ref`), Firebase app name (`app`), `method` (`set`, `update`, `remove`, `push`, or
`transaction`), and `tries` (1 for ordinary writes, possibly 0 for failed transactions).
Successful observations have `outcome: 'success'`, `duration` in milliseconds, and `latency` equal to
`duration / tries`. For transactions, duration is the measured transaction phase, excluding prefetch;
per-try latency is an average, not individual
round-trip measurements. No synthetic latency floor is applied.

Firebase errors instead have `outcome: 'failure'` or `'timeout'`, `code`, and `elapsed` milliseconds
until rejection, excluding transaction prefetch. They have no successful duration or latency.
A timeout does not establish when or whether the underlying write completes. Skipped/aborted
transactions, transactions that fail during prefetch before the transaction phase starts, and
application errors without Firebase error metadata are omitted. Rethrowing a prior Firebase error
from a transaction update function can count it again as a write failure. Pushes without a value
create no write observation.
Writes made directly through the Firebase SDK are outside NodeFire's observation scope.
Once the transaction phase starts, failures and timeouts count even if Firebase has not yet invoked
the update function (`tries: 0`). They never contribute a successful latency sample.

The callback can feed latency distributions and separate failure/timeout counters into your metrics
system. Firelease does not publish metrics to Sentry or any other service. Callback errors are
reported through `captureError` with phase `write-metric` and cannot change write results.
The callback must not write through NodeFire to an observed database, even outside queue paths:
each such write triggers another metric callback, creating an endless feedback loop.

```defaults: {Object}```

Mutable default option values for all subsequent attachWorker calls.  See that function for details.

```stats: {Object}```

The live stats object also passed to the ping callback.  Global fields include `healthy`,
`sickQueues`, `sickSources`, `stuckTasks`, `maxLatency`, `leaseTransactions`, and the legacy
`tasksAcquired`.  `leaseTransactions` contains lifetime `acquired`, `contended`, `failed`, and
`tries` counts for task lease transactions.  `tries` includes failed transactions and comes from
NodeFire transaction metadata.  `duration` is an exponential moving average of the NodeFire
transaction duration in milliseconds.  These stats are available for every
physical source.  Logical queue and global counts are additive, while their duration is the average
of the underlying source or queue duration values that have recorded at least one attempt.  The
legacy `tasksAcquired` field also remains
lifetime-cumulative.  Each entry in `queues` includes its own health, latency, leasing totals, and
all physical `sources`.  Queue-level
`size` and `sizeDelta` sum their source values when all are known, `sizeTimestamp` is the oldest
source timestamp, and `mode` is `full`, `safe`, or `mixed`.  Source stats include `connected`,
current `mode` (`full` or `safe`), last known `size`, `sizeTimestamp` when the size came from a
shallow REST check, `sizeDelta` when the safe listener was below its limit (the REST count minus its
buffered task count), and ping health and latency.  Queue size is reported only for adaptive
sources; it remains `null` for sources with an explicit finite `bufferSize` because a limited
listener cannot determine the true queue size.

`databases` contains one entry per Firebase database connection used by attached workers. Each
entry identifies the root URL (`ref`) and Firebase app name (`app`); distinct apps accessing the
same URL remain separate. `writeLatency` is an exponential moving average of successful
write latency in milliseconds, initially `null`. `writeSamples` counts those successful operations
(one sample per transaction, using duration per try). `writeFailures` counts observed Firebase
errors other than timeouts, and `writeTimeouts` counts timeouts. Both failure counters are disjoint
from successful samples. Permission and validation errors count as failures, not latency samples.
Use `captureWriteMetrics` for distributions; an EMA is not a percentile.

Database counts accumulate from worker attachment and persist across pings, reconnects, shutdown,
and resuming queues through `settings.globalMaxConcurrent`. They do not reset at each ping. The
stats remain JSON-serializable and contain no database references or callbacks. Test resets clear
the database registry, stats, and callback without adding another NodeFire observer. Writes that
started before attachment are included if they complete after attachment; tests must drain any
in-flight writes before resetting. Write telemetry does not affect queue health or concurrency.
