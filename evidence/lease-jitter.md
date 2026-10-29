# Affinity jitter: live Firebase evidence

The on-demand [harness](../tests/live/lease_jitter.mjs) ran independent worker processes
against a real Firebase database. Each comparison enqueued 120 tasks sequentially across 16 PR
affinities, first without jitter and then with adaptive jitter. Each phase starts with a fresh
controller, so these results include adaptation rather than discarding warmup.

Unmodified harness reports: [1 worker](lease-jitter-1.json), [2 workers](lease-jitter-2.json),
[8 workers](lease-jitter-8.json), [20 workers](lease-jitter-20.json).

## Contention versus pickup latency

Amplification is (acquired + contended + failed transactions) / acquired tasks. Tries are NodeFire
transaction attempts, including retries. Pickup is measured from the database creation timestamp
to entry into the worker; it includes notification and acquisition latency, not just the jitter.

| Workers | Mode | Acquired | Contended | Tries | Transactions/acquisition | Median pickup (ms) | p95 pickup (ms) |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | baseline | 120 | 0 | 120 | 1.00 | 71 | 76 |
| 1 | adaptive | 120 | 0 | 120 | 1.00 | 73 | 199 |
| 2 | baseline | 120 | 120 | 360 | 2.00 | 65 | 96 |
| 2 | adaptive | 120 | 9 | 138 | 1.08 | 67 | 187 |
| 8 | baseline | 120 | 829 | 1777 | 7.91 | 70 | 76 |
| 8 | adaptive | 120 | 60 | 240 | 1.50 | 130 | 285 |
| 20 | baseline | 120 | 2251 | 4622 | 19.76 | 69 | 75 |
| 20 | adaptive | 120 | 177 | 474 | 2.48 | 99 | 169 |

With 20 workers, adaptive jitter used **87.5% fewer lease transactions** and **89.7% fewer tries**.
The median pickup increase was 30 ms; the p95 increase was 94 ms. All four runs had zero failed
transactions, and every comparison task was processed exactly once.

The single-worker window reached zero. Its last 20 adaptive pickups had a median of 73 ms versus
71 ms without jitter; the 199 ms whole-run p95 includes startup adaptation. The two-worker run
ended with windows of 0 and 758 ms. Independent local windows can favor one idle worker across
many affinities; this is a cache preference, not a fairness or load-balancing guarantee.

## Lifecycle and limited listeners

All worker counts passed the same assertions (fallback requires at least two workers):

- Twenty tasks with distinct keys and the same PR affinity all reached the same worker (20/20).
- Making that preferred worker unavailable through either quota exhaustion or disconnection allowed
  another worker to acquire the next task.
- Retry processing reported attempts 1 and 2, with first-acquisition flags true and false.
- An extended task remained singly acquired while processing continued past its original lease
  expiry.
- Twelve overdue tasks were each processed once with no jitter scheduled, using both full and
  limited listeners.
- Twenty production-shaped pings (server-timestamp expiry, no created or affinity field) were
  deleted without invoking an application handler. Pending ping suppressions were 0, 19, 135, and
  345 with 1, 2, 8, and 20 workers respectively. Pings did not increment task acquisition stats.
- Workers exited cleanly, and each run's isolated database subtree was removed and checked absent.

Twenty fresh arrivals were also processed exactly once with limited listeners:

| Workers | Transactions/acquisition | Suppressed | Median pickup (ms) | p95 pickup (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1.00 | 0 | 186 | 271 |
| 2 | 1.15 | 16 | 107 | 921 |
| 8 | 1.95 | 103 | 107 | 1034 |
| 20 | 3.30 | 295 | 103 | 145 |

With only 20 arrivals, the reported p95 is the largest observed pickup. These short scenarios
include startup adaptation and occasional outliers; they do not establish a production percentile.

These are controlled same-host worker tests over a live Firebase connection, not a prediction for
heterogeneous production networks or a long-term steady-state benchmark. The same-affinity result
is a preference, not an ownership guarantee. The production rollout should compare amplification
and sampled pickup latency before enabling additional queues; lifetime tries remain available in
Firelease stats.

The bounded fixtures completed on the free plan. Limited queries emitted missing-index warnings;
no rules or indexes were changed. Normal CI does not run this harness.

## Hashing cost

A separate [microbenchmark](../tests/live/hash_benchmark.cjs), run with
`node tests/live/hash_benchmark.cjs`, compared score computation including JSON key framing.
It uses 1,000 representative keys, 200,000 iterations per sample, and five samples after one warmup.
The [unmodified samples](hash-benchmark.json) recorded these medians on Node 24:

| Implementation | Median time per score |
| --- | ---: |
| SHA-256 via createHash/update/digest | 1.04 µs |
| SHA-256 via one-shot crypto.hash | 0.79 µs |
| FNV-1a with an avalanche step | 0.15 µs |

The implementation uses one-shot SHA-256, preserving the existing ranks without a custom hash or
dependency. These are measurements on one runtime, not a universal claim about CPU performance.
