# Affinity jitter: live Firebase evidence

The on-demand [harness](../tests/live/lease_jitter.mjs) ran independent worker processes
against a real Firebase database. Each comparison enqueued 120 tasks sequentially across 16 PR
affinities, first without jitter and then with adaptive jitter. Each phase starts with a fresh
controller, so these results include adaptation rather than discarding warmup.

Unmodified harness reports: [8 workers](lease-jitter-8.json), [20 workers](lease-jitter-20.json).

## Contention versus pickup latency

Amplification is (acquired + contended + failed transactions) / acquired tasks. Tries are NodeFire
transaction attempts, including retries. Pickup is measured from the database creation timestamp
to entry into the worker; it includes notification and acquisition latency, not just the jitter.

| Workers | Mode | Acquired | Contended | Tries | Transactions/acquisition | Median pickup (ms) | p95 pickup (ms) |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | baseline | 120 | 825 | 1770 | 7.88 | 69 | 86 |
| 8 | adaptive | 120 | 54 | 228 | 1.45 | 119 | 261 |
| 20 | baseline | 120 | 2264 | 4648 | 19.87 | 69 | 74 |
| 20 | adaptive | 120 | 158 | 436 | 2.32 | 102 | 198 |

With 20 workers, adaptive jitter used **88.3% fewer lease transactions** and **90.6% fewer tries**.
The median pickup increase was 33 ms; the p95 increase was 124 ms. Both runs had zero failed
transactions, and every comparison task was processed exactly once.

## Lifecycle and limited listeners

Both worker counts passed the same assertions:

- Twenty tasks with distinct keys and the same PR affinity all reached the same worker (20/20).
- Making that preferred worker unavailable through either quota exhaustion or disconnection allowed
  another worker to acquire the next task.
- Retry processing reported attempts 1 and 2, with first-acquisition flags true and false.
- An extended task remained singly acquired while processing continued past its original lease
  expiry.
- Twelve overdue tasks were each processed once with no jitter scheduled, using both full and
  limited listeners.
- Twenty fresh arrivals were each processed once with limited listeners. Suppressions were 94 with
  8 workers and 290 with 20 workers; amplification was 2.10 and 3.35 respectively.
- Workers exited cleanly, and each run's isolated database subtree was removed and checked absent.

These are controlled same-host worker tests over a live Firebase connection, not a prediction for
heterogeneous production networks or a long-term steady-state benchmark. The same-affinity result
is a preference, not an ownership guarantee. The production rollout should compare amplification,
tries per acquisition, and sampled pickup latency before enabling additional queues.

The bounded fixtures completed on the free plan. Limited queries emitted missing-index warnings;
no rules or indexes were changed. Normal CI does not run this harness.
