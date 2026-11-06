/**
 * EventFlow - Resilient Worker Pool & Task Dispatcher
 * Manages concurrent asynchronous execution, priority queues, and runtime metrics.
 */

import { WorkerPoolMetrics } from '../types.js';

export interface Task<T = any> {
  id: string;
  priority?: number; // Higher number = higher priority
  execute: () => Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: any) => void;
  queuedAt: number;
}

export class WorkerPool {
  private readonly maxConcurrency: number;
  private activeWorkers = 0;
  private readonly taskQueue: Task[] = [];
  private totalCompleted = 0;
  private totalFailed = 0;
  private totalRetried = 0;
  private totalExecutionTimeMs = 0;
  private isDraining = false;

  constructor(maxConcurrency = 8) {
    if (maxConcurrency < 1) {
      throw new Error('WorkerPool concurrency must be at least 1');
    }
    this.maxConcurrency = maxConcurrency;
  }

  public enqueue<T>(execute: () => Promise<T>, priority = 0, taskId?: string): Promise<T> {
    if (this.isDraining) {
      return Promise.reject(new Error('WorkerPool is draining and not accepting new tasks'));
    }

    return new Promise<T>((resolve, reject) => {
      const task: Task<T> = {
        id: taskId ?? `task-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        priority,
        execute,
        resolve,
        reject,
        queuedAt: Date.now(),
      };

      // Priority insertion (descending order)
      let inserted = false;
      for (let i = 0; i < this.taskQueue.length; i++) {
        if ((task.priority ?? 0) > (this.taskQueue[i]!.priority ?? 0)) {
          this.taskQueue.splice(i, 0, task);
          inserted = true;
          break;
        }
      }
      if (!inserted) {
        this.taskQueue.push(task);
      }

      this.dispatch();
    });
  }

  public recordRetry(): void {
    this.totalRetried++;
  }

  private dispatch(): void {
    while (this.activeWorkers < this.maxConcurrency && this.taskQueue.length > 0) {
      const task = this.taskQueue.shift();
      if (!task) break;

      this.activeWorkers++;
      const startTime = Date.now();

      (async () => {
        try {
          const result = await task.execute();
          const duration = Date.now() - startTime;
          this.totalExecutionTimeMs += duration;
          this.totalCompleted++;
          this.activeWorkers--;
          task.resolve(result);
        } catch (error) {
          const duration = Date.now() - startTime;
          this.totalExecutionTimeMs += duration;
          this.totalFailed++;
          this.activeWorkers--;
          task.reject(error);
        } finally {
          this.dispatch();
        }
      })();
    }
  }

  public getMetrics(): WorkerPoolMetrics {
    const totalFinished = this.totalCompleted + this.totalFailed;
    const avgDuration = totalFinished > 0 ? this.totalExecutionTimeMs / totalFinished : 0;

    return {
      totalCapacity: this.maxConcurrency,
      activeWorkers: this.activeWorkers,
      idleWorkers: Math.max(0, this.maxConcurrency - this.activeWorkers),
      queuedTasks: this.taskQueue.length,
      completedTasks: this.totalCompleted,
      failedTasks: this.totalFailed,
      retriedTasks: this.totalRetried,
      averageExecutionTimeMs: Math.round(avgDuration * 10) / 10,
    };
  }

  public async drain(): Promise<void> {
    this.isDraining = true;
    while (this.activeWorkers > 0 || this.taskQueue.length > 0) {
      await new Promise((r) => setTimeout(r, 50));
    }
  }
}
