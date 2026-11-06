import { describe, it } from 'node:test';
import assert from 'node:assert';
import { WorkerPool } from '../../src/core/engine/worker-pool.js';

describe('WorkerPool', () => {
  it('should enforce concurrency limit', async () => {
    const pool = new WorkerPool(2);
    let peakConcurrency = 0;
    let currentConcurrent = 0;

    const task = async () => {
      currentConcurrent++;
      peakConcurrency = Math.max(peakConcurrency, currentConcurrent);
      await new Promise((r) => setTimeout(r, 50));
      currentConcurrent--;
      return 'done';
    };

    await Promise.all([
      pool.enqueue(task),
      pool.enqueue(task),
      pool.enqueue(task),
      pool.enqueue(task),
    ]);

    assert.strictEqual(peakConcurrency, 2);
    const metrics = pool.getMetrics();
    assert.strictEqual(metrics.completedTasks, 4);
    assert.strictEqual(metrics.activeWorkers, 0);
  });

  it('should respect task priority ordering when queue is backed up', async () => {
    const pool = new WorkerPool(1);
    const executionOrder: string[] = [];

    // Block the 1 worker
    const blocker = pool.enqueue(async () => {
      await new Promise((r) => setTimeout(r, 60));
      executionOrder.push('blocker');
    });

    // Enqueue low and high priority while blocked
    const low = pool.enqueue(async () => {
      executionOrder.push('low');
    }, 1);

    const high = pool.enqueue(async () => {
      executionOrder.push('high');
    }, 10);

    await Promise.all([blocker, low, high]);

    // High priority must execute before low priority
    assert.deepStrictEqual(executionOrder, ['blocker', 'high', 'low']);
  });
});
