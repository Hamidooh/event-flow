import { describe, it } from 'node:test';
import assert from 'node:assert';
import { Orchestrator } from '../../src/core/engine/orchestrator.js';
import { WorkflowDefinition } from '../../src/core/types.js';

describe('Orchestrator Integration', () => {
  it('should execute multi-step DAG and route outputs to downstream dependencies', async () => {
    const workflow: WorkflowDefinition = {
      id: 'pipeline-test',
      name: 'Pipeline Test',
      version: '1.0.0',
      steps: [
        {
          id: 'step-a',
          name: 'Step A',
          run: async () => ({ value: 10 }),
        },
        {
          id: 'step-b',
          name: 'Step B',
          dependencies: ['step-a'],
          run: async (ctx) => {
            const outA = ctx.getStepOutput('step-a');
            return { multiplied: outA.value * 2 };
          },
        },
      ],
    };

    const orchestrator = new Orchestrator();
    orchestrator.registerWorkflow(workflow);

    const instance = await orchestrator.runWorkflow('pipeline-test');

    assert.strictEqual(instance.status, 'COMPLETED');
    assert.strictEqual(instance.stepStates['step-a']?.status, 'COMPLETED');
    assert.strictEqual(instance.stepStates['step-b']?.status, 'COMPLETED');
    assert.strictEqual(instance.stepOutputs['step-b']?.multiplied, 20);
  });

  it('should trigger Saga backward compensations when step fails', async () => {
    const compensationsRun: string[] = [];

    const sagaWorkflow: WorkflowDefinition = {
      id: 'saga-failure-test',
      name: 'Saga Failure Test',
      version: '1.0.0',
      steps: [
        {
          id: 'step-first',
          name: 'First Step',
          run: async () => 'first-ok',
          compensation: async () => {
            compensationsRun.push('first-compensated');
          },
        },
        {
          id: 'step-second',
          name: 'Second Step',
          dependencies: ['step-first'],
          run: async () => 'second-ok',
          compensation: async () => {
            compensationsRun.push('second-compensated');
          },
        },
        {
          id: 'step-third-failing',
          name: 'Third Step Failing',
          dependencies: ['step-second'],
          retryPolicy: { maxRetries: 1, initialDelayMs: 10 },
          run: async () => {
            throw new Error('Fatal service exception');
          },
        },
      ],
    };

    const orchestrator = new Orchestrator();
    orchestrator.registerWorkflow(sagaWorkflow);

    const instance = await orchestrator.runWorkflow('saga-failure-test');

    assert.strictEqual(instance.status, 'FAILED');
    assert(instance.error?.includes('Fatal service exception'));

    // Compensations must run in reverse order (LIFO: second first, then first)
    assert.deepStrictEqual(compensationsRun, ['second-compensated', 'first-compensated']);
  });

  it('should cancel running workflow when cancelWorkflow is invoked', async () => {
    const workflow: WorkflowDefinition = {
      id: 'cancel-test',
      name: 'Cancel Test',
      version: '1.0.0',
      steps: [
        {
          id: 'slow-step',
          name: 'Slow Step',
          run: async () => {
            await new Promise((r) => setTimeout(r, 400));
            return 'done';
          },
        },
      ],
    };

    const orchestrator = new Orchestrator();
    orchestrator.registerWorkflow(workflow);

    const runPromise = orchestrator.runWorkflow('cancel-test');

    // Wait slightly then cancel
    await new Promise((r) => setTimeout(r, 50));
    const instance = orchestrator.getAllInstances()[0]!;
    orchestrator.cancelWorkflow(instance.id);

    const completedInstance = await runPromise;
    assert.strictEqual(completedInstance.status, 'CANCELLED');
  });
});
