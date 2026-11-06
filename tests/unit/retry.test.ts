import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  calculateBackoffDelay,
  executeWithRetry,
  StepTimeoutError,
} from '../../src/core/engine/retry-policy.js';

describe('Retry & Timeout Engine', () => {
  it('should calculate exponential backoff without exceeding maxDelay', () => {
    const policy = {
      maxRetries: 5,
      initialDelayMs: 100,
      maxDelayMs: 1000,
      backoffFactor: 2,
      jitter: false,
    };

    assert.strictEqual(calculateBackoffDelay(1, policy), 100);
    assert.strictEqual(calculateBackoffDelay(2, policy), 200);
    assert.strictEqual(calculateBackoffDelay(3, policy), 400);
    assert.strictEqual(calculateBackoffDelay(4, policy), 800);
    assert.strictEqual(calculateBackoffDelay(5, policy), 1000); // capped at maxDelayMs
  });

  it('should retry a failed function and return result upon eventual success', async () => {
    let callCount = 0;
    const result = await executeWithRetry({
      stepId: 'flaky-step',
      policy: { maxRetries: 3, initialDelayMs: 10, backoffFactor: 1.5, jitter: false },
      fn: async (_signal, attempt) => {
        callCount++;
        if (attempt < 3) {
          throw new Error('Temporary failure');
        }
        return 'recovered-data';
      },
    });

    assert.strictEqual(result, 'recovered-data');
    assert.strictEqual(callCount, 3);
  });

  it('should fail when maxRetries is exhausted', async () => {
    let callCount = 0;
    await assert.rejects(
      async () => {
        await executeWithRetry({
          stepId: 'failing-step',
          policy: { maxRetries: 2, initialDelayMs: 10, backoffFactor: 1.5, jitter: false },
          fn: async () => {
            callCount++;
            throw new Error('Permanent database failure');
          },
        });
      },
      /Permanent database failure/
    );

    assert.strictEqual(callCount, 2);
  });

  it('should abort and throw StepTimeoutError if execution exceeds timeout', async () => {
    await assert.rejects(
      async () => {
        await executeWithRetry({
          stepId: 'slow-step',
          timeoutMs: 50,
          policy: { maxRetries: 1, initialDelayMs: 10, backoffFactor: 1, jitter: false },
          fn: async (signal) => {
            await new Promise((resolve, reject) => {
              const timer = setTimeout(resolve, 200);
              signal.addEventListener('abort', () => {
                clearTimeout(timer);
                reject(new StepTimeoutError('slow-step', 50));
              });
            });
          },
        });
      },
      (err: any) => {
        assert(err instanceof StepTimeoutError || err.name === 'StepTimeoutError');
        return true;
      }
    );
  });
});
