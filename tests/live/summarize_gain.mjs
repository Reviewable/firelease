// Render tables directly from unmodified experiment reports; no hand-entered measurements.
import {readFile, writeFile} from 'node:fs/promises';
import {parseArgs} from 'node:util';
import {basename} from 'node:path';
import assert from 'node:assert/strict';
const {values} = parseArgs({options: {
  simulation: {type: 'string'}, live: {type: 'string', multiple: true},
  lifecycle: {type: 'string'}, output: {type: 'string'}
}});
const simulation = JSON.parse(await readFile(values.simulation, 'utf8'));
const selected = simulation.results.default, fixed = simulation.results.fixed;
const rows = Object.keys(selected).map(name => {
  const a = fixed[name].aggregate, b = selected[name].aggregate;
  return `| ${name} | ${a.amplification.toFixed(3)} → ${b.amplification.toFixed(3)} | ` +
    `${Math.round(a.pickupP95)} → ${Math.round(b.pickupP95)} | ` +
    `${Math.round(a.pickupP99)} → ${Math.round(b.pickupP99)} |`;
});
const variants = Object.entries(simulation.results).map(([name, scenarios]) => {
  return `| ${name} | ${scenarios.fast.aggregate.amplification.toFixed(3)} | ` +
    `${Math.round(scenarios.fast.aggregate.pickupP95)} | ` +
    `${Math.round(scenarios.scale.aggregate.pickupP99)} |`;
});
const totals = {acquired: 0, failed: 0};
let liveTables = '';
for (const path of values.live ?? []) {
  const report = JSON.parse(await readFile(path, 'utf8'));
  assert(report.passed && report.cleanedUp);
  assert.deepEqual(report.gain, simulation.defaultGain);
  liveTables += `\n### ${basename(path, '.json')}\n\n` +
    `[Unmodified report](${basename(path)}). ${report.workers} independent workers; ` +
    `${report.tasksPerMode} tasks in full-sized phases.\n\n` +
    '| Mode/scenario/phase | Acquired | Tx/acquisition | Tries/acquisition | ' +
    'Pickup p50/p95/p99 ms | Elapsed s |\n' +
    '| --- | ---: | ---: | ---: | --- | ---: |\n';
  for (const [name, phases] of Object.entries(report.scenarios)) {
    for (const [index, phase] of phases.entries()) {
      totals.acquired += phase.acquired;
      totals.failed += phase.failed;
      liveTables += `| ${name}/${index} | ${phase.acquired} | ` +
        `${phase.amplification.toFixed(3)} | ` +
        `${(phase.tries / phase.acquired).toFixed(3)} | ` +
        `${phase.pickupMedianMs}/${phase.pickupP95Ms}/${phase.pickupP99Ms} | ` +
        `${(phase.elapsedMs / 1000).toFixed(2)} |\n`;
    }
  }
}
let lifecycleText = '';
if (values.lifecycle) {
  const report = JSON.parse(await readFile(values.lifecycle, 'utf8'));
  assert(report.passed && report.cleanedUp);
  assert.deepEqual(report.gain, simulation.defaultGain);
  const base = report.scenarios.baseline, adaptive = report.scenarios.adaptive;
  lifecycleText = `\n## Live lifecycle regression\n\n` +
    `[Unmodified report](${basename(values.lifecycle)}): ${report.workers} workers, ` +
    `${base.acquired} tasks per sequential comparison. Transactions/acquisition ` +
    `${base.amplification.toFixed(3)} → ${adaptive.amplification.toFixed(3)}; ` +
    `pickup median ${base.pickupMedianMs} → ${adaptive.pickupMedianMs} ms; ` +
    `p95 ${base.pickupP95Ms} → ${adaptive.pickupP95Ms} ms.\n\n` +
    'The harness also asserted affinity, busy/disconnected fallback, ' +
    'retry/first-acquisition flags, ' +
    'extension, backlog, limited arrivals, and ping behavior.\n\n' +
    'Limited-query deletion and eviction yielded identical old-value snapshots, ' +
    'while the evicted task still existed. Those ambiguous removals deliberately cancel ' +
    'without training; no per-loser ' +
    'confirmation read was added. Observed fixture values:\n\n```json\n' +
    JSON.stringify(report.limitedRemovalSnapshots, null, 2) + '\n```\n';
}
const markdown = `# Bounded rate-normalized leasing gain

The selected controller uses a maximum gain of ${simulation.defaultGain.maximum}, a gain budget of
${simulation.defaultGain.budget}/second, and a ${simulation.defaultGain.smoothingMs / 1000}-second
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

Raw data: [held-out model reports](${basename(values.simulation)}). Reproduce with

    yarn test:simulation --seeds 16 --offset 100 \\
      --candidates fixed,default,cap2-budget20-smooth5,cap3-budget20-smooth5 \\
      --output gain-holdout.json

| Candidate | Fast tx/acquisition | Fast pickup p95 ms | Scale-change pickup p99 ms |
| --- | ---: | ---: | ---: |
${variants.join('\n')}

## Model coverage: fixed gain 2 → selected normalization

Scenarios include sparse/fast arrivals, 1/2/8/20 workers, rate steps, 1→20→1 workers (keeping the
highest-window survivor), latency steps, heterogeneous links, bursts, saturation, hot affinity,
and resumption after inactivity. All generated tasks must complete. Saturation intentionally
overloads the model; its large delays are retained rather than discarded.

| Scenario | Tx/acquisition | Pickup p95 ms | Pickup p99 ms |
| --- | ---: | ---: | ---: |
${rows.join('\n')}

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
${liveTables}

Across these paired trials: ${totals.acquired} acquisitions;
${totals.failed} failed transactions.
Do not infer a universal win from individual noisy tail samples, especially saturation phases
where the reported minimum gain remains at 2 and both controllers use identical updates.
${lifecycleText}`;
await writeFile(values.output, markdown);
