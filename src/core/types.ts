/**
 * EventFlow - Core Type Definitions
 * Distributed, resilient DAG workflow & event orchestration engine.
 */

export type WorkflowStatus =
  | 'IDLE'
  | 'RUNNING'
  | 'PAUSED'
  | 'COMPLETED'
  | 'FAILED'
  | 'CANCELLED';

export type StepStatus =
  | 'PENDING'
  | 'WAITING_DEPS'
  | 'READY'
  | 'RUNNING'
  | 'COMPLETED'
  | 'FAILED'
  | 'RETRYING'
  | 'SKIPPED'
  | 'CANCELLED';

export interface RetryPolicy {
  maxRetries: number;
  initialDelayMs: number;
  maxDelayMs: number;
  backoffFactor: number;
  jitter: boolean;
}

export interface StepContext<TInput = Record<string, any>> {
  workflowInstanceId: string;
  stepId: string;
  attempt: number;
  input: TInput;
  dependenciesOutput: Record<string, any>;
  signal: AbortSignal;
  log: (message: string, data?: any) => void;
  getStepOutput: <T = any>(stepId: string) => T | undefined;
}

export interface StepDefinition<TInput = any, TOutput = any> {
  id: string;
  name: string;
  dependencies?: string[];
  timeoutMs?: number;
  retryPolicy?: Partial<RetryPolicy>;
  run: (context: StepContext<TInput>) => Promise<TOutput>;
  compensation?: (context: StepContext<TInput>) => Promise<void>;
}

export interface WorkflowDefinition {
  id: string;
  name: string;
  version: string;
  description?: string;
  concurrency?: number;
  steps: StepDefinition[];
}

export interface StepExecutionRecord {
  stepId: string;
  name: string;
  status: StepStatus;
  attempt: number;
  startTime?: number;
  endTime?: number;
  durationMs?: number;
  output?: any;
  error?: string;
  dependencies: string[];
  logs: Array<{ timestamp: number; message: string; data?: any }>;
}

export interface WorkflowInstance {
  id: string;
  workflowId: string;
  workflowName: string;
  status: WorkflowStatus;
  startTime: number;
  endTime?: number;
  durationMs?: number;
  initialInput: Record<string, any>;
  stepStates: Record<string, StepExecutionRecord>;
  stepOutputs: Record<string, any>;
  error?: string;
}

export type EventType =
  | 'WORKFLOW_CREATED'
  | 'WORKFLOW_STARTED'
  | 'WORKFLOW_PAUSED'
  | 'WORKFLOW_RESUMED'
  | 'WORKFLOW_COMPLETED'
  | 'WORKFLOW_FAILED'
  | 'WORKFLOW_CANCELLED'
  | 'STEP_QUEUED'
  | 'STEP_STARTED'
  | 'STEP_COMPLETED'
  | 'STEP_FAILED'
  | 'STEP_RETRYING'
  | 'STEP_SKIPPED'
  | 'STEP_COMPENSATION_STARTED'
  | 'STEP_COMPENSATION_COMPLETED'
  | 'STEP_COMPENSATION_FAILED'
  | 'LOG_EMITTED';

export interface WorkflowEvent {
  id: string;
  sequence: number;
  timestamp: number;
  workflowInstanceId: string;
  type: EventType;
  stepId?: string;
  payload?: any;
}

export interface WorkerPoolMetrics {
  totalCapacity: number;
  activeWorkers: number;
  idleWorkers: number;
  queuedTasks: number;
  completedTasks: number;
  failedTasks: number;
  retriedTasks: number;
  averageExecutionTimeMs: number;
}

export interface EngineMetrics {
  totalWorkflowsExecuted: number;
  activeWorkflows: number;
  successfulWorkflows: number;
  failedWorkflows: number;
  cancelledWorkflows: number;
  workerPool: WorkerPoolMetrics;
  uptimeSeconds: number;
}
