import { describe, it } from 'node:test';
import assert from 'node:assert';
import { EventStore } from '../../src/core/state/event-store.js';

describe('EventStore & Replay', () => {
  it('should append events with monotonic sequences and emit to subscribers', () => {
    const store = new EventStore();
    const captured: any[] = [];
    const unsubscribe = store.onEvent((e) => captured.push(e));

    const e1 = store.append('inst-1', 'WORKFLOW_CREATED', { workflowId: 'wf-1' });
    const e2 = store.append('inst-1', 'WORKFLOW_STARTED');

    assert.strictEqual(e1.sequence, 1);
    assert.strictEqual(e2.sequence, 2);
    assert.strictEqual(captured.length, 2);

    unsubscribe();
    store.append('inst-1', 'WORKFLOW_COMPLETED');
    assert.strictEqual(captured.length, 2); // Unsubscribed
  });

  it('should deterministically reconstruct workflow state from event stream', () => {
    const store = new EventStore();
    const instId = 'w-test-123';

    store.append(instId, 'WORKFLOW_CREATED', {
      workflowId: 'test-wf',
      workflowName: 'Test Workflow',
      input: { foo: 'bar' },
      steps: [
        { id: 's1', name: 'Step 1' },
        { id: 's2', name: 'Step 2' },
      ],
    });
    store.append(instId, 'WORKFLOW_STARTED');
    store.append(instId, 'STEP_STARTED', { attempt: 1 }, 's1');
    store.append(instId, 'STEP_COMPLETED', { output: { val: 42 } }, 's1');
    store.append(instId, 'WORKFLOW_COMPLETED');

    const events = store.getEventsForWorkflow(instId);
    const instance = EventStore.replayWorkflow(events);

    assert(instance !== null);
    assert.strictEqual(instance.id, instId);
    assert.strictEqual(instance.status, 'COMPLETED');
    assert.strictEqual(instance.stepStates['s1']?.status, 'COMPLETED');
    assert.strictEqual(instance.stepOutputs['s1'].val, 42);
    assert.strictEqual(instance.stepStates['s2']?.status, 'PENDING');
  });
});
