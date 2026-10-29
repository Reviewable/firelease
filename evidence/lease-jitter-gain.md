# Bounded rate-normalized leasing gain

The selected controller uses a maximum gain of 2, a gain budget of
40/second, and a 5-second
exponentially decaying opportunity-rate estimate. It has no timer and never ages an idle window.
All queue options and production telemetry remain unchanged.

## Selection and limitations

This is a conservative normalization of response speed, not another order-of-magnitude reduction
in contention. The cap preserves the existing sparse-queue behavior. The higher budget avoids
most of the extra acquisition work incurred by the lower-budget candidate. A larger cap reduces
some contention but worsens scale-down tail delay. Rate normalization itself also slows recovery
after a high-rate scale-down: see fastScale, including its per-phase raw reports. This is the cost
of bounding adaptation per second, not a universal latency improvement. The selected budget is
more conservative than 20; bounds and race target remain the per-queue policy controls.

An initial parameter sweep used seeds 1–4. The table below uses separate seeds 101–116, pairing
identities, arrivals, and network delays across candidates. Outcomes are generated closed-loop,
not replayed: different gains change scheduling and subsequent contention. Means of per-seed
percentiles are reported, not pooled production percentiles. The model does not predict Firebase
retry costs, bandwidth, or cache hit rate; amplification here counts acquisition transactions only.

Raw data: [held-out model reports](gain-holdout.json). Reproduce with

    yarn test:simulation --seeds 16 --offset 100 \
      --candidates fixed,default,cap2-budget20-smooth5,cap3-budget20-smooth5 \
      --output gain-holdout.json

| Candidate | Fast tx/acquisition | Fast pickup p95 ms | Scale-change pickup p99 ms |
| --- | ---: | ---: | ---: |
| fixed | 2.072 | 279 | 2058 |
| default | 2.086 | 275 | 2058 |
| cap2-budget20-smooth5 | 2.138 | 271 | 2058 |
| cap3-budget20-smooth5 | 2.110 | 277 | 2352 |

## Model coverage: fixed gain 2 → selected normalization

Scenarios include sparse/fast arrivals, 1/2/8/20 workers, rate steps, 1→20→1 workers (keeping the
highest-window survivor), latency steps, heterogeneous links, bursts, saturation, hot affinity,
and resumption after inactivity. All generated tasks must complete. Saturation intentionally
overloads the model; its large delays are retained rather than discarded.

| Scenario | Tx/acquisition | Pickup p95 ms | Pickup p99 ms |
| --- | ---: | ---: | ---: |
| sparse | 2.930 → 2.930 | 223 → 223 | 289 → 289 |
| light | 2.702 → 2.702 | 236 → 236 | 310 → 310 |
| fast | 2.072 → 2.086 | 279 → 275 | 358 → 357 |
| veryFast | 2.032 → 2.054 | 283 → 280 | 367 → 363 |
| two | 1.093 → 1.093 | 610 → 622 | 779 → 793 |
| eight | 1.447 → 1.447 | 514 → 519 | 660 → 664 |
| single | 1.000 → 1.000 | 149 → 149 | 231 → 231 |
| scale | 1.591 → 1.591 | 1187 → 1187 | 2058 → 2058 |
| fastScale | 1.535 → 1.542 | 324 → 645 | 1931 → 2183 |
| rate | 2.065 → 2.073 | 279 → 276 | 360 → 365 |
| latency | 2.505 → 2.505 | 612 → 612 | 875 → 875 |
| heterogeneous | 2.392 → 2.392 | 296 → 296 | 368 → 368 |
| burst | 2.413 → 2.413 | 263 → 263 | 341 → 341 |
| saturated | 3.730 → 3.730 | 84536 → 84536 | 88064 → 88064 |
| hot | 2.222 → 2.222 | 144 → 144 | 156 → 156 |
| resume | 2.135 → 2.135 | 278 → 278 | 370 → 370 |

## Live Firebase comparisons

Arrivals are scheduled independently of task completion. Every worker on a path uses the same
controller; fixed and normalized modes retain the same process identities. Separate runs reverse
mode order. These are small controlled trials, not stable production p99 estimates. Reported
elapsed time includes scheduling the arrivals, processing, and final settlement.

Each trial includes 4→40→4 arrivals/s, 20-task bursts, scale-down to the highest-window worker,
and a two-slot/500-ms-service saturation scenario. Every phase asserts one acquisition
per task, first-acquisition flags, and no failed lease transactions. Each UUID fixture is removed
and checked absent after worker shutdown. Gain diagnostics are local to the harness; cumulative
gain update counts and minimum gain help distinguish actual normalization from network noise.
No extra Sentry metrics, production writes, security-rule changes, or database upgrade were needed.

### gain-live-20

[Unmodified report](gain-live-20.json). 20 independent workers; 200 tasks in full-sized phases.

| Mode/scenario/phase | Acquired | Tx/acquisition | Tries/acquisition | Pickup p50/p95/p99 ms | Elapsed s |
| --- | ---: | ---: | ---: | --- | ---: |
| fixed-rate/0 | 100 | 2.540 | 4.080 | 93/179/808 | 25.17 |
| fixed-rate/1 | 200 | 1.870 | 2.740 | 97/178/228 | 5.39 |
| fixed-rate/2 | 100 | 2.170 | 3.340 | 95/194/807 | 25.17 |
| fixed-burst/0 | 200 | 2.260 | 3.520 | 83/177/238 | 18.65 |
| fixed-scale-down/0 | 200 | 2.230 | 3.460 | 81/167/193 | 10.41 |
| fixed-scale-down/1 | 200 | 1.000 | 1.000 | 183/897/1084 | 25.25 |
| fixed-saturated/0 | 200 | 3.415 | 5.855 | 164/1969/2026 | 7.75 |
| normalized-rate/0 | 100 | 2.500 | 4.000 | 90/164/434 | 25.17 |
| normalized-rate/1 | 200 | 1.935 | 2.870 | 97/173/198 | 5.43 |
| normalized-rate/2 | 100 | 2.020 | 3.040 | 97/189/256 | 25.16 |
| normalized-burst/0 | 200 | 2.135 | 3.270 | 90/154/191 | 18.63 |
| normalized-scale-down/0 | 200 | 2.215 | 3.430 | 90/160/217 | 10.40 |
| normalized-scale-down/1 | 200 | 1.000 | 1.000 | 222/974/1159 | 25.25 |
| normalized-saturated/0 | 200 | 3.390 | 5.790 | 381/1632/1676 | 7.40 |

### gain-live-20-reverse

[Unmodified report](gain-live-20-reverse.json). 20 independent workers; 200 tasks in full-sized phases.

| Mode/scenario/phase | Acquired | Tx/acquisition | Tries/acquisition | Pickup p50/p95/p99 ms | Elapsed s |
| --- | ---: | ---: | ---: | --- | ---: |
| normalized-rate/0 | 100 | 2.520 | 4.040 | 99/180/654 | 25.16 |
| normalized-rate/1 | 200 | 1.910 | 2.820 | 102/185/203 | 5.38 |
| normalized-rate/2 | 100 | 1.930 | 2.860 | 101/178/221 | 25.16 |
| normalized-burst/0 | 200 | 2.180 | 3.360 | 103/173/205 | 18.60 |
| normalized-scale-down/0 | 200 | 2.265 | 3.530 | 105/191/575 | 10.37 |
| normalized-scale-down/1 | 200 | 1.000 | 1.000 | 413/1179/1425 | 25.26 |
| normalized-saturated/0 | 200 | 3.365 | 5.750 | 231/1641/1749 | 7.40 |
| fixed-rate/0 | 100 | 2.430 | 3.860 | 94/159/183 | 25.17 |
| fixed-rate/1 | 200 | 1.975 | 2.950 | 96/161/176 | 5.38 |
| fixed-rate/2 | 100 | 1.900 | 2.800 | 99/162/217 | 25.17 |
| fixed-burst/0 | 200 | 2.215 | 3.430 | 95/176/203 | 18.64 |
| fixed-scale-down/0 | 200 | 2.180 | 3.360 | 96/159/191 | 10.32 |
| fixed-scale-down/1 | 200 | 1.000 | 1.000 | 195/860/1267 | 25.25 |
| fixed-saturated/0 | 200 | 3.820 | 6.665 | 323/2479/2574 | 8.26 |


Across these paired trials: 4800 acquisitions;
0 failed transactions.
Do not infer a universal win from individual noisy tail samples, especially saturation phases
where the reported minimum gain remains at 2 and both controllers use identical updates.

## Live lifecycle regression

[Unmodified report](gain-lifecycle-8.json): 8 workers, 120 tasks per sequential comparison. Transactions/acquisition 7.833 → 1.583; pickup median 71 → 125 ms; p95 76 → 260 ms.

The harness also asserted affinity, busy/disconnected fallback, retry/first-acquisition flags, extension, backlog, limited arrivals, and ping behavior.

Limited-query deletion and eviction yielded identical old-value snapshots, while the evicted task still existed. Those ambiguous removals deliberately cancel without training; no per-loser confirmation read was added. Observed fixture values:

```json
{
  "deletion": {
    "_lease": {
      "expiry": 100
    },
    "payload": "same-old-value"
  },
  "eviction": {
    "_lease": {
      "expiry": 100
    },
    "payload": "same-old-value"
  },
  "evictedTaskStillExists": true
}
```
