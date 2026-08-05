import assert from 'node:assert';
import {test} from 'node:test';

import {FireleaseStats, QueueSourceStats, QueueStats, TESTABLES} from '../src';

test('stats are derived through the hierarchy on demand', () => {
  TESTABLES.resetBetweenTests();
  let stuckTasks = 0;
  const sourceStats = new QueueSourceStats('https://stats.example.test/queues/jobs');
  const secondSourceStats = new QueueSourceStats('https://second.example.test/queues/jobs');
  const queueStats = new QueueStats(
    'https://stats.example.test/queues/jobs', 'jobs', [sourceStats, secondSourceStats]);
  const hierarchyStats = new FireleaseStats(() => stuckTasks);
  hierarchyStats.queues.push(queueStats);

  assert.strictEqual(queueStats.healthy, false);
  assert.strictEqual(hierarchyStats.healthy, false);
  assert.deepStrictEqual(hierarchyStats.sickQueues, ['jobs']);
  assert.deepStrictEqual(hierarchyStats.sickSources, [sourceStats.ref, secondSourceStats.ref]);
  assert.strictEqual(queueStats.mode, 'safe');
  assert.strictEqual(queueStats.size, null);
  assert.strictEqual(queueStats.sizeDelta, undefined);
  assert.strictEqual(queueStats.sizeTimestamp, undefined);

  sourceStats.connected = true;
  sourceStats.size = 4;
  sourceStats.sizeDelta = 1;
  sourceStats.sizeTimestamp = 200;
  sourceStats.latency = 12;
  sourceStats.leaseTransactions.acquired = 2;
  sourceStats.leaseTransactions.contended = 3;
  sourceStats.leaseTransactions.tries = 8;
  sourceStats.leaseTransactions.duration = 50;
  secondSourceStats.connected = true;
  secondSourceStats.size = 6;
  secondSourceStats.sizeDelta = 2;
  secondSourceStats.sizeTimestamp = 100;
  secondSourceStats.latency = 8;
  secondSourceStats.leaseTransactions.acquired = 1;
  secondSourceStats.leaseTransactions.contended = 2;
  secondSourceStats.leaseTransactions.tries = 5;
  secondSourceStats.leaseTransactions.duration = 30;
  queueStats.tasksAcquired = 3;
  stuckTasks = 2;
  assert.strictEqual(hierarchyStats.healthy, true);
  assert.deepStrictEqual(hierarchyStats.sickQueues, []);
  assert.deepStrictEqual(hierarchyStats.sickSources, []);
  assert.strictEqual(queueStats.maxLatency, 12);
  assert.strictEqual(hierarchyStats.maxLatency, 12);
  assert.deepStrictEqual(queueStats.leaseTransactions, {
    acquired: 3, contended: 5, tries: 13, duration: 80
  });
  assert.deepStrictEqual(hierarchyStats.leaseTransactions, queueStats.leaseTransactions);
  assert.strictEqual(hierarchyStats.tasksAcquired, 3);
  assert.strictEqual(hierarchyStats.stuckTasks, 2);
  assert.strictEqual(queueStats.size, 10);
  assert.strictEqual(queueStats.sizeDelta, 3);
  assert.strictEqual(queueStats.sizeTimestamp, 100);
  secondSourceStats.mode = 'full';
  assert.strictEqual(queueStats.mode, 'mixed');
  secondSourceStats.mode = 'safe';
  delete secondSourceStats.sizeDelta;
  assert.strictEqual(queueStats.sizeDelta, undefined);
  secondSourceStats.sizeDelta = 2;
  assert.deepStrictEqual(
    Object.keys(hierarchyStats),
    [
      'queues', 'healthy', 'sickQueues', 'sickSources', 'stuckTasks', 'maxLatency',
      'leaseTransactions', 'tasksAcquired'
    ],
  );
  assert.deepStrictEqual(JSON.parse(JSON.stringify(hierarchyStats)), {
    queues: [{
      tasksAcquired: 3,
      ref: 'https://stats.example.test/queues/jobs',
      key: 'jobs',
      sources: [{
        connected: true,
        mode: 'safe',
        size: 4,
        healthy: true,
        latency: 12,
        leaseTransactions: {acquired: 2, contended: 3, tries: 8, duration: 50},
        ref: 'https://stats.example.test/queues/jobs',
        sizeDelta: 1,
        sizeTimestamp: 200
      }, {
        connected: true,
        mode: 'safe',
        size: 6,
        healthy: true,
        latency: 8,
        leaseTransactions: {acquired: 1, contended: 2, tries: 5, duration: 30},
        ref: 'https://second.example.test/queues/jobs',
        sizeTimestamp: 100,
        sizeDelta: 2
      }],
      mode: 'safe',
      size: 10,
      sizeDelta: 3,
      sizeTimestamp: 100,
      healthy: true,
      maxLatency: 12,
      leaseTransactions: {acquired: 3, contended: 5, tries: 13, duration: 80}
    }],
    healthy: true,
    sickQueues: [],
    sickSources: [],
    stuckTasks: 2,
    maxLatency: 12,
    leaseTransactions: {acquired: 3, contended: 5, tries: 13, duration: 80},
    tasksAcquired: 3
  });

  assert.deepStrictEqual(queueStats.resetLeaseTransactions(), {
    acquired: 3, contended: 5, tries: 13, duration: 80
  });
  assert.deepStrictEqual(queueStats.leaseTransactions, {
    acquired: 0, contended: 0, tries: 0, duration: 0
  });
  assert.deepStrictEqual(hierarchyStats.leaseTransactions, queueStats.leaseTransactions);
  assert.strictEqual(hierarchyStats.tasksAcquired, 3, 'legacy count remains cumulative');

  sourceStats.leaseTransactions.acquired = 1;
  sourceStats.leaseTransactions.tries = 2;
  sourceStats.leaseTransactions.duration = 7;
  assert.deepStrictEqual(hierarchyStats.resetLeaseTransactions(), {
    acquired: 1, contended: 0, tries: 2, duration: 7
  });
  assert.deepStrictEqual(sourceStats.leaseTransactions, {
    acquired: 0, contended: 0, tries: 0, duration: 0
  });
  TESTABLES.resetBetweenTests();
});
