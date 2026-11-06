/**
 * EventFlow - Event Store & Sourcing Engine
 * Implements append-only event sourcing, event pub/sub, and state reconstruction.
 */

import { EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import { EventType, WorkflowEvent, WorkflowInstance } from '../types.js';

export class EventStore {
  private events: WorkflowEvent[] = [];
  private sequence = 0;
  private readonly emitter = new EventEmitter();

  constructor() {
    this.emitter.setMaxListeners(100);
  }

  public append(
    workflowInstanceId: string,
    type: EventType,
    payload?: any,
    stepId?: string
  ): WorkflowEvent {
    this.sequence++;
    const event: WorkflowEvent = {
      id: randomUUID(),
      sequence: this.sequence,
      timestamp: Date.now(),
      workflowInstanceId,
      type,
      stepId,
      payload,
    };

    this.events.push(event);
    this.emitter.emit('event', event);
    this.emitter.emit(`workflow:${workflowInstanceId}`, event);

    return event;
  }

  public getEventsForWorkflow(workflowInstanceId: string): WorkflowEvent[] {
    return this.events.filter((e) => e.workflowInstanceId === workflowInstanceId);
  }

  public getAllEvents(): readonly WorkflowEvent[] {
    return this.events;
  }

  public getEventsSince(sequence: number): WorkflowEvent[] {
    return this.events.filter((e) => e.sequence > sequence);
  }

  public onEvent(listener: (event: WorkflowEvent) => void): () => void {
    this.emitter.on('event', listener);
    return () => this.emitter.off('event', listener);
  }

  public onWorkflowEvent(
    workflowInstanceId: string,
    listener: (event: WorkflowEvent) => void
  ): () => void {
    const channel = `workflow:${workflowInstanceId}`;
    this.emitter.on(channel, listener);
    return () => this.emitter.off(channel, listener);
  }

  /**
   * Replays a stream of events to deterministically reconstruct a WorkflowInstance state.
   */
  public static replayWorkflow(events: WorkflowEvent[]): WorkflowInstance | null {
    if (events.length === 0) return null;

    let instance: WorkflowInstance | null = null;

    for (const evt of events) {
      switch (evt.type) {
        case 'WORKFLOW_CREATED':
          instance = {
            id: evt.workflowInstanceId,
            workflowId: evt.payload.workflowId,
            workflowName: evt.payload.workflowName,
            status: 'IDLE',
            startTime: evt.timestamp,
            initialInput: evt.payload.input ?? {},
            stepStates: {},
            stepOutputs: {},
          };
          for (const step of evt.payload.steps || []) {
            instance.stepStates[step.id] = {
              stepId: step.id,
              name: step.name,
              status: 'PENDING',
              attempt: 0,
              dependencies: step.dependencies ?? [],
              logs: [],
            };
          }
          break;

        case 'WORKFLOW_STARTED':
          if (instance) {
            instance.status = 'RUNNING';
            instance.startTime = evt.timestamp;
          }
          break;

        case 'WORKFLOW_PAUSED':
          if (instance) instance.status = 'PAUSED';
          break;

        case 'WORKFLOW_RESUMED':
          if (instance) instance.status = 'RUNNING';
          break;

        case 'WORKFLOW_COMPLETED':
          if (instance) {
            instance.status = 'COMPLETED';
            instance.endTime = evt.timestamp;
            instance.durationMs = instance.endTime - instance.startTime;
          }
          break;

        case 'WORKFLOW_FAILED':
          if (instance) {
            instance.status = 'FAILED';
            instance.endTime = evt.timestamp;
            instance.durationMs = instance.endTime - instance.startTime;
            instance.error = evt.payload?.error;
          }
          break;

        case 'WORKFLOW_CANCELLED':
          if (instance) {
            instance.status = 'CANCELLED';
            instance.endTime = evt.timestamp;
            instance.durationMs = instance.endTime - instance.startTime;
          }
          break;

        case 'STEP_QUEUED':
          if (instance && evt.stepId && instance.stepStates[evt.stepId]) {
            instance.stepStates[evt.stepId]!.status = 'READY';
          }
          break;

        case 'STEP_STARTED':
          if (instance && evt.stepId && instance.stepStates[evt.stepId]) {
            const step = instance.stepStates[evt.stepId]!;
            step.status = 'RUNNING';
            step.startTime = evt.timestamp;
            step.attempt = evt.payload?.attempt ?? step.attempt + 1;
          }
          break;

        case 'STEP_COMPLETED':
          if (instance && evt.stepId && instance.stepStates[evt.stepId]) {
            const step = instance.stepStates[evt.stepId]!;
            step.status = 'COMPLETED';
            step.endTime = evt.timestamp;
            step.durationMs = step.startTime ? step.endTime - step.startTime : 0;
            step.output = evt.payload?.output;
            instance.stepOutputs[evt.stepId] = evt.payload?.output;
          }
          break;

        case 'STEP_FAILED':
          if (instance && evt.stepId && instance.stepStates[evt.stepId]) {
            const step = instance.stepStates[evt.stepId]!;
            step.status = 'FAILED';
            step.endTime = evt.timestamp;
            step.durationMs = step.startTime ? step.endTime - step.startTime : 0;
            step.error = evt.payload?.error;
          }
          break;

        case 'STEP_RETRYING':
          if (instance && evt.stepId && instance.stepStates[evt.stepId]) {
            const step = instance.stepStates[evt.stepId]!;
            step.status = 'RETRYING';
            step.attempt = evt.payload?.attempt ?? step.attempt;
          }
          break;

        case 'STEP_SKIPPED':
          if (instance && evt.stepId && instance.stepStates[evt.stepId]) {
            instance.stepStates[evt.stepId]!.status = 'SKIPPED';
          }
          break;

        case 'LOG_EMITTED':
          if (instance && evt.stepId && instance.stepStates[evt.stepId]) {
            instance.stepStates[evt.stepId]!.logs.push({
              timestamp: evt.timestamp,
              message: evt.payload?.message ?? '',
              data: evt.payload?.data,
            });
          }
          break;
      }
    }

    return instance;
  }
}
