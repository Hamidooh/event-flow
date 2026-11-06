/**
 * EventFlow - Resilient Retry and Timeout Policy
 * Exponential backoff with Full Jitter and AbortSignal cancellation.
 */

import { RetryPolicy } from '../types.js';

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxRetries: 3,
  initialDelayMs: 300,
  maxDelayMs: 8000,
  backoffFactor: 2,
  jitter: true,
};

export class StepTimeoutError extends Error {
  constructor(stepId: string, timeoutMs: number) {
    super(`Step "${stepId}" timed out after ${timeoutMs}ms.`);
    this.name = 'StepTimeoutError';
  }
}

export function calculateBackoffDelay(attempt: number, policy: RetryPolicy): number {
  const exponential = policy.initialDelayMs * Math.pow(policy.backoffFactor, attempt - 1);
  const capped = Math.min(exponential, policy.maxDelayMs);

  if (!policy.jitter) {
    return Math.floor(capped);
  }

  // Full Jitter: random between 0.5x and 1.5x of capped backoff
  const jitterFactor = 0.5 + Math.random();
  return Math.floor(capped * jitterFactor);
}

export async function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      return reject(new Error('Operation aborted during sleep delay'));
    }

    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timer);
      reject(new Error('Operation aborted during sleep delay'));
    };

    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

export interface RetryExecutionOptions<T> {
  stepId: string;
  policy?: Partial<RetryPolicy>;
  timeoutMs?: number;
  parentSignal?: AbortSignal;
  onRetry?: (attempt: number, error: Error, delayMs: number) => void;
  fn: (signal: AbortSignal, attempt: number) => Promise<T>;
}

export async function executeWithRetry<T>(options: RetryExecutionOptions<T>): Promise<T> {
  const policy: RetryPolicy = { ...DEFAULT_RETRY_POLICY, ...options.policy };
  let attempt = 1;

  while (true) {
    const controller = new AbortController();
    let timeoutHandle: NodeJS.Timeout | undefined;

    // Link parent signal if present
    const onParentAbort = () => controller.abort();
    if (options.parentSignal) {
      if (options.parentSignal.aborted) {
        controller.abort();
      } else {
        options.parentSignal.addEventListener('abort', onParentAbort, { once: true });
      }
    }

    if (options.timeoutMs && options.timeoutMs > 0) {
      timeoutHandle = setTimeout(() => {
        controller.abort(new StepTimeoutError(options.stepId, options.timeoutMs!));
      }, options.timeoutMs);
    }

    try {
      const result = await options.fn(controller.signal, attempt);
      if (timeoutHandle) clearTimeout(timeoutHandle);
      if (options.parentSignal) {
        options.parentSignal.removeEventListener('abort', onParentAbort);
      }
      if (options.parentSignal?.aborted || controller.signal.aborted) {
        throw new Error('Operation was cancelled');
      }
      return result;
    } catch (err: any) {
      if (timeoutHandle) clearTimeout(timeoutHandle);
      if (options.parentSignal) {
        options.parentSignal.removeEventListener('abort', onParentAbort);
      }

      // If aborted by user / parent cancel, do not retry
      if (options.parentSignal?.aborted) {
        throw err;
      }

      const isTimeout =
        err instanceof StepTimeoutError ||
        controller.signal.reason instanceof StepTimeoutError;

      const finalError = isTimeout
        ? (controller.signal.reason as Error) || err
        : err;

      if (attempt >= policy.maxRetries) {
        throw finalError;
      }

      const delayMs = calculateBackoffDelay(attempt, policy);
      if (options.onRetry) {
        options.onRetry(attempt, finalError, delayMs);
      }

      await sleep(delayMs, options.parentSignal);
      attempt++;
    }
  }
}
