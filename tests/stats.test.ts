import assert from 'node:assert';
import {test} from 'node:test';

import {FireleaseStats, QueueSourceStats, QueueStats, TESTABLES} from '../src';

test('stats are derived through the hierarchy on demand', () => {
  TESTABLES.resetBetweenTests();
  const expectedDuration = 450 / 11;
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
  sourceStats.leaseTransactions.failed = 1;
  sourceStats.leaseTransactions.tries = 8;
  sourceStats.leaseTransactions.duration = 50;
  secondSourceStats.connected = true;
  secondSourceStats.size = 6;
  secondSourceStats.sizeDelta = 2;
  secondSourceStats.sizeTimestamp = 100;
  secondSourceStats.latency = 8;
  secondSourceStats.leaseTransactions.acquired = 1;
  secondSourceStats.leaseTransactions.contended = 2;
  secondSourceStats.leaseTransactions.failed = 2;
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
    acquired: 3, contended: 5, failed: 3, tries: 13, duration: expectedDuration
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
        leaseTransactions: {acquired: 2, contended: 3, failed: 1, tries: 8, duration: 50},
        ref: 'https://stats.example.test/queues/jobs',
        sizeDelta: 1,
        sizeTimestamp: 200
      }, {
        connected: true,
        mode: 'safe',
        size: 6,
        healthy: true,
        latency: 8,
        leaseTransactions: {acquired: 1, contended: 2, failed: 2, tries: 5, duration: 30},
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
      leaseTransactions: {
        acquired: 3, contended: 5, failed: 3, tries: 13, duration: expectedDuration
      }
    }],
    healthy: true,
    sickQueues: [],
    sickSources: [],
    stuckTasks: 2,
    maxLatency: 12,
    leaseTransactions: {
      acquired: 3, contended: 5, failed: 3, tries: 13, duration: expectedDuration
    },
    tasksAcquired: 3
  });
  TESTABLES.resetBetweenTests();
});

test('duration rollups are weighted by total lease attempts at each level', () => {
  const firstSource = new QueueSourceStats('https://first.example.test/queues/jobs');
  firstSource.leaseTransactions.acquired = 1;
  firstSource.leaseTransactions.contended = 2;
  firstSource.leaseTransactions.duration = 20;
  const secondSource = new QueueSourceStats('https://second.example.test/queues/jobs');
  secondSource.leaseTransactions.failed = 1;
  secondSource.leaseTransactions.duration = 80;
  const firstQueue = new QueueStats('https://first.example.test/queues/jobs', 'jobs', [
    firstSource, secondSource
  ]);

  const thirdSource = new QueueSourceStats('https://third.example.test/queues/other');
  thirdSource.leaseTransactions.acquired = 2;
  thirdSource.leaseTransactions.contended = 1;
  thirdSource.leaseTransactions.failed = 1;
  thirdSource.leaseTransactions.duration = 50;
  const secondQueue = new QueueStats(
    'https://third.example.test/queues/other', 'other', [thirdSource]);

  const hierarchyStats = new FireleaseStats(() => 0);
  hierarchyStats.queues.push(firstQueue, secondQueue);

  assert.strictEqual(firstQueue.leaseTransactions.duration, 35);
  assert.strictEqual(secondQueue.leaseTransactions.duration, 50);
  assert.strictEqual(hierarchyStats.leaseTransactions.duration, 42.5);
});
