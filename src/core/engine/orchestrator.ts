/**
 * EventFlow - Core Workflow Orchestrator
 * High-performance DAG scheduler, Saga compensation engine, and state coordinator.
 */

import { randomUUID } from 'node:crypto';
import {
  EngineMetrics,
  StepContext,
  StepDefinition,
  StepExecutionRecord,
  WorkflowDefinition,
  WorkflowEvent,
  WorkflowInstance,
} from '../types.js';
import { DirectedAcyclicGraph } from '../dag/graph.js';
import { EventStore } from '../state/event-store.js';
import { WALStorage } from '../state/wal-storage.js';
import { WorkerPool } from './worker-pool.js';
import { executeWithRetry } from './retry-policy.js';

export interface OrchestratorOptions {
  concurrency?: number;
  walStorage?: WALStorage;
  eventStore?: EventStore;
}

export class Orchestrator {
  private readonly workflows: Map<string, WorkflowDefinition> = new Map();
  private readonly dags: Map<string, DirectedAcyclicGraph> = new Map();
  private readonly instances: Map<string, WorkflowInstance> = new Map();
  private readonly activeAbortControllers: Map<string, AbortController> = new Map();
  private readonly pausedWorkflowIds: Set<string> = new Set();
  private readonly workerPool: WorkerPool;
  private readonly eventStore: EventStore;
  private readonly walStorage?: WALStorage;
  private readonly startTime = Date.now();

  private stats = {
    totalWorkflowsExecuted: 0,
    successfulWorkflows: 0,
    failedWorkflows: 0,
    cancelledWorkflows: 0,
  };

  constructor(options: OrchestratorOptions = {}) {
    this.workerPool = new WorkerPool(options.concurrency ?? 10);
    this.eventStore = options.eventStore ?? new EventStore();
    this.walStorage = options.walStorage;

    // Listen to all events and log to WAL if enabled
    this.eventStore.onEvent((event) => {
      if (this.walStorage) {
        this.walStorage.appendEvent(event).catch((err) => {
          console.error('[Orchestrator] WAL append error:', err);
        });
      }
    });
  }

  public registerWorkflow(workflow: WorkflowDefinition): void {
    const dag = new DirectedAcyclicGraph(workflow.steps);
    // Validate on registration - throws DAGValidationError if cycles or invalid deps
    dag.validate();

    this.workflows.set(workflow.id, workflow);
    this.dags.set(workflow.id, dag);
  }

  public getWorkflow(id: string): WorkflowDefinition | undefined {
    return this.workflows.get(id);
  }

  public getAllWorkflows(): WorkflowDefinition[] {
    return Array.from(this.workflows.values());
  }

  public getInstance(instanceId: string): WorkflowInstance | undefined {
    return this.instances.get(instanceId);
  }

  public getAllInstances(): WorkflowInstance[] {
    return Array.from(this.instances.values());
  }

  public getEventStore(): EventStore {
    return this.eventStore;
  }

  public getMetrics(): EngineMetrics {
    const active = Array.from(this.instances.values()).filter(
      (i) => i.status === 'RUNNING' || i.status === 'PAUSED'
    ).length;

    return {
      totalWorkflowsExecuted: this.stats.totalWorkflowsExecuted,
      activeWorkflows: active,
      successfulWorkflows: this.stats.successfulWorkflows,
      failedWorkflows: this.stats.failedWorkflows,
      cancelledWorkflows: this.stats.cancelledWorkflows,
      workerPool: this.workerPool.getMetrics(),
      uptimeSeconds: Math.floor((Date.now() - this.startTime) / 1000),
    };
  }

  /**
   * Initializes instance and executes workflow DAG.
   */
  public async runWorkflow(
    workflowId: string,
    input: Record<string, any> = {}
  ): Promise<WorkflowInstance> {
    const workflow = this.workflows.get(workflowId);
    const dag = this.dags.get(workflowId);

    if (!workflow || !dag) {
      throw new Error(`Workflow with ID "${workflowId}" is not registered.`);
    }

    const instanceId = randomUUID();
    const abortController = new AbortController();
    this.activeAbortControllers.set(instanceId, abortController);

    const stepStates: Record<string, StepExecutionRecord> = {};
    for (const step of workflow.steps) {
      stepStates[step.id] = {
        stepId: step.id,
        name: step.name,
        status: 'PENDING',
        attempt: 0,
        dependencies: step.dependencies ?? [],
        logs: [],
      };
    }

    const instance: WorkflowInstance = {
      id: instanceId,
      workflowId: workflow.id,
      workflowName: workflow.name,
      status: 'RUNNING',
      startTime: Date.now(),
      initialInput: input,
      stepStates,
      stepOutputs: {},
    };

    this.instances.set(instanceId, instance);
    this.stats.totalWorkflowsExecuted++;

    this.emitEvent(instanceId, 'WORKFLOW_CREATED', {
      workflowId: workflow.id,
      workflowName: workflow.name,
      input,
      steps: workflow.steps.map((s) => ({ id: s.id, name: s.name, dependencies: s.dependencies })),
    });

    this.emitEvent(instanceId, 'WORKFLOW_STARTED', { timestamp: instance.startTime });

    try {
      await this.executeDAG(instance, workflow, dag, abortController.signal);
      instance.status = 'COMPLETED';
      instance.endTime = Date.now();
      instance.durationMs = instance.endTime - instance.startTime;
      this.stats.successfulWorkflows++;
      this.emitEvent(instanceId, 'WORKFLOW_COMPLETED', {
        durationMs: instance.durationMs,
        outputs: instance.stepOutputs,
      });
    } catch (err: any) {
      if (abortController.signal.aborted) {
        instance.status = 'CANCELLED';
        instance.endTime = Date.now();
        instance.durationMs = instance.endTime - instance.startTime;
        this.stats.cancelledWorkflows++;
        this.emitEvent(instanceId, 'WORKFLOW_CANCELLED', { error: err.message });
      } else {
        instance.status = 'FAILED';
        instance.endTime = Date.now();
        instance.durationMs = instance.endTime - instance.startTime;
        instance.error = err.message ?? String(err);
        this.stats.failedWorkflows++;
        this.emitEvent(instanceId, 'WORKFLOW_FAILED', { error: instance.error });

        // Run Saga compensations for completed steps
        await this.runSagaCompensations(instance, workflow);
      }
    } finally {
      this.activeAbortControllers.delete(instanceId);
      this.pausedWorkflowIds.delete(instanceId);
      if (this.walStorage) {
        this.walStorage.saveSnapshot(Object.fromEntries(this.instances)).catch(() => {});
      }
    }

    return instance;
  }

  private async executeDAG(
    instance: WorkflowInstance,
    workflow: WorkflowDefinition,
    dag: DirectedAcyclicGraph,
    signal: AbortSignal
  ): Promise<void> {
    const completedStepIds = new Set<string>();
    const activePromises = new Map<string, Promise<any>>();

    const abortPromise = new Promise<never>((_, reject) => {
      if (signal.aborted) {
        reject(new Error('Workflow was cancelled'));
      } else {
        signal.addEventListener('abort', () => reject(new Error('Workflow was cancelled')), {
          once: true,
        });
      }
    });

    // Check for paused state helper
    const checkPaused = async () => {
      while (this.pausedWorkflowIds.has(instance.id) && !signal.aborted) {
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
    };

    while (completedStepIds.size < workflow.steps.length) {
      if (signal.aborted) {
        throw new Error('Workflow was cancelled');
      }

      await checkPaused();

      // Find all steps ready to run:
      // 1. Not already started or completed
      // 2. All dependencies are in completedStepIds
      const readySteps = workflow.steps.filter((step) => {
        return (
          !completedStepIds.has(step.id) &&
          !activePromises.has(step.id) &&
          dag.areDependenciesSatisfied(step.id, completedStepIds)
        );
      });

      for (const step of readySteps) {
        const stepRecord = instance.stepStates[step.id]!;
        stepRecord.status = 'READY';
        this.emitEvent(instance.id, 'STEP_QUEUED', {}, step.id);

        const promise = this.workerPool.enqueue(async () => {
          await checkPaused();
          if (signal.aborted) {
            throw new Error(`Step "${step.id}" cancelled before execution`);
          }
          return this.executeStep(instance, step, signal);
        }, 0, `${instance.id}-${step.id}`);

        activePromises.set(step.id, promise);
      }

      if (activePromises.size === 0 && completedStepIds.size < workflow.steps.length) {
        // Deadlock / unmet dependencies
        throw new Error('Workflow execution reached deadlock: unmet dependencies remain.');
      }

      // Wait for any active step to finish or workflow abort
      const finishedStepId = await Promise.race([
        abortPromise,
        ...Array.from(activePromises.entries()).map(async ([sId, p]) => {
          await p;
          return sId;
        }),
      ]);

      activePromises.delete(finishedStepId);
      completedStepIds.add(finishedStepId);
    }
  }

  private async executeStep(
    instance: WorkflowInstance,
    step: StepDefinition,
    parentSignal: AbortSignal
  ): Promise<any> {
    const record = instance.stepStates[step.id]!;
    record.status = 'RUNNING';
    record.startTime = Date.now();
    this.emitEvent(instance.id, 'STEP_STARTED', { attempt: 1 }, step.id);

    // Prepare context
    const depsOutput: Record<string, any> = {};
    for (const depId of step.dependencies ?? []) {
      depsOutput[depId] = instance.stepOutputs[depId];
    }

    try {
      const output = await executeWithRetry({
        stepId: step.id,
        policy: step.retryPolicy,
        timeoutMs: step.timeoutMs,
        parentSignal,
        onRetry: (attempt, error, delayMs) => {
          this.workerPool.recordRetry();
          record.status = 'RETRYING';
          record.attempt = attempt;
          this.emitEvent(
            instance.id,
            'STEP_RETRYING',
            { attempt, error: error.message, retryDelayMs: delayMs },
            step.id
          );
        },
        fn: async (signal, attempt) => {
          record.attempt = attempt;
          const context: StepContext = {
            workflowInstanceId: instance.id,
            stepId: step.id,
            attempt,
            input: instance.initialInput,
            dependenciesOutput: depsOutput,
            signal,
            log: (message, data) => {
              record.logs.push({ timestamp: Date.now(), message, data });
              this.emitEvent(instance.id, 'LOG_EMITTED', { message, data }, step.id);
            },
            getStepOutput: (sId: string) => instance.stepOutputs[sId],
          };

          return await step.run(context);
        },
      });

      record.status = 'COMPLETED';
      record.endTime = Date.now();
      record.durationMs = record.endTime - (record.startTime ?? record.endTime);
      record.output = output;
      instance.stepOutputs[step.id] = output;

      this.emitEvent(
        instance.id,
        'STEP_COMPLETED',
        { durationMs: record.durationMs, output },
        step.id
      );

      return output;
    } catch (err: any) {
      record.status = 'FAILED';
      record.endTime = Date.now();
      record.durationMs = record.endTime - (record.startTime ?? record.endTime);
      record.error = err.message ?? String(err);

      this.emitEvent(
        instance.id,
        'STEP_FAILED',
        { durationMs: record.durationMs, error: record.error },
        step.id
      );

      throw err;
    }
  }

  /**
   * Saga Compensation: runs compensations for completed steps in reverse order (LIFO).
   */
  private async runSagaCompensations(
    instance: WorkflowInstance,
    workflow: WorkflowDefinition
  ): Promise<void> {
    const completedStepsWithCompensation = workflow.steps
      .filter((s) => instance.stepStates[s.id]?.status === 'COMPLETED' && s.compensation)
      .reverse();

    for (const step of completedStepsWithCompensation) {
      this.emitEvent(instance.id, 'STEP_COMPENSATION_STARTED', {}, step.id);
      try {
        const context: StepContext = {
          workflowInstanceId: instance.id,
          stepId: step.id,
          attempt: 1,
          input: instance.initialInput,
          dependenciesOutput: {},
          signal: new AbortController().signal,
          log: (msg, data) => {
            this.emitEvent(instance.id, 'LOG_EMITTED', { message: `[Compensation] ${msg}`, data }, step.id);
          },
          getStepOutput: (sId: string) => instance.stepOutputs[sId],
        };

        await step.compensation!(context);
        this.emitEvent(instance.id, 'STEP_COMPENSATION_COMPLETED', {}, step.id);
      } catch (compErr: any) {
        this.emitEvent(
          instance.id,
          'STEP_COMPENSATION_FAILED',
          { error: compErr.message },
          step.id
        );
      }
    }
  }

  public pauseWorkflow(instanceId: string): boolean {
    const instance = this.instances.get(instanceId);
    if (!instance || instance.status !== 'RUNNING') return false;

    this.pausedWorkflowIds.add(instanceId);
    instance.status = 'PAUSED';
    this.emitEvent(instanceId, 'WORKFLOW_PAUSED');
    return true;
  }

  public resumeWorkflow(instanceId: string): boolean {
    const instance = this.instances.get(instanceId);
    if (!instance || instance.status !== 'PAUSED') return false;

    this.pausedWorkflowIds.delete(instanceId);
    instance.status = 'RUNNING';
    this.emitEvent(instanceId, 'WORKFLOW_RESUMED');
    return true;
  }

  public cancelWorkflow(instanceId: string): boolean {
    const controller = this.activeAbortControllers.get(instanceId);
    if (!controller) return false;

    controller.abort();
    return true;
  }

  private emitEvent(
    workflowInstanceId: string,
    type: WorkflowEvent['type'],
    payload?: any,
    stepId?: string
  ): void {
    this.eventStore.append(workflowInstanceId, type, payload, stepId);
  }
}
