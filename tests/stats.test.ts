import assert from 'node:assert';
import {test} from 'node:test';

import {FireleaseStats, QueueSourceStats, QueueStats, TESTABLES} from '../src';

test('stats are derived through the hierarchy on demand', () => {
  TESTABLES.reset();
  let stuckTasks = 0;
  const sourceStats = new QueueSourceStats('https://stats.example.test/queues/jobs');
  const queueStats = new QueueStats(
    'https://stats.example.test/queues/jobs', 'jobs', [sourceStats]);
  const hierarchyStats = new FireleaseStats(() => stuckTasks);
  hierarchyStats.queues.push(queueStats);

  assert.strictEqual(queueStats.healthy, false);
  assert.strictEqual(hierarchyStats.healthy, false);
  assert.deepStrictEqual(hierarchyStats.sickQueues, ['jobs']);
  assert.deepStrictEqual(hierarchyStats.sickSources, [sourceStats.ref]);

  sourceStats.connected = true;
  sourceStats.latency = 12;
  queueStats.tasksAcquired = 3;
  stuckTasks = 2;
  assert.strictEqual(hierarchyStats.healthy, true);
  assert.deepStrictEqual(hierarchyStats.sickQueues, []);
  assert.deepStrictEqual(hierarchyStats.sickSources, []);
  assert.strictEqual(queueStats.maxLatency, 12);
  assert.strictEqual(hierarchyStats.maxLatency, 12);
  assert.strictEqual(hierarchyStats.tasksAcquired, 3);
  assert.strictEqual(hierarchyStats.stuckTasks, 2);
  assert.deepStrictEqual(
    Object.keys(hierarchyStats),
    ['queues', 'healthy', 'sickQueues', 'sickSources', 'stuckTasks', 'maxLatency', 'tasksAcquired'],
  );
  assert.deepStrictEqual(JSON.parse(JSON.stringify(hierarchyStats)), {
    queues: [{
      tasksAcquired: 3,
      ref: 'https://stats.example.test/queues/jobs',
      key: 'jobs',
      sources: [{
        connected: true,
        mode: 'safe',
        size: null,
        healthy: true,
        latency: 12,
        ref: 'https://stats.example.test/queues/jobs'
      }],
      healthy: true,
      maxLatency: 12
    }],
    healthy: true,
    sickQueues: [],
    sickSources: [],
    stuckTasks: 2,
    maxLatency: 12,
    tasksAcquired: 3
  });
  TESTABLES.reset();
});
