import { describe, it } from 'node:test';
import assert from 'node:assert';
import { DirectedAcyclicGraph, DAGValidationError } from '../../src/core/dag/graph.js';
import { StepDefinition } from '../../src/core/types.js';

describe('DirectedAcyclicGraph', () => {
  it('should compute valid topological execution layers for linear pipeline', () => {
    const steps: StepDefinition[] = [
      { id: 'step-1', name: 'Step 1', run: async () => 'ok' },
      { id: 'step-2', name: 'Step 2', dependencies: ['step-1'], run: async () => 'ok' },
      { id: 'step-3', name: 'Step 3', dependencies: ['step-2'], run: async () => 'ok' },
    ];

    const dag = new DirectedAcyclicGraph(steps);
    const layers = dag.getExecutionLayers();

    assert.strictEqual(layers.length, 3);
    assert.strictEqual(layers[0]![0]!.id, 'step-1');
    assert.strictEqual(layers[1]![0]!.id, 'step-2');
    assert.strictEqual(layers[2]![0]!.id, 'step-3');
  });

  it('should correctly partition parallel diamond dependencies into layers', () => {
    //   A
    //  / \
    // B   C
    //  \ /
    //   D
    const steps: StepDefinition[] = [
      { id: 'A', name: 'A', run: async () => 1 },
      { id: 'B', name: 'B', dependencies: ['A'], run: async () => 2 },
      { id: 'C', name: 'C', dependencies: ['A'], run: async () => 3 },
      { id: 'D', name: 'D', dependencies: ['B', 'C'], run: async () => 4 },
    ];

    const dag = new DirectedAcyclicGraph(steps);
    const layers = dag.getExecutionLayers();

    assert.strictEqual(layers.length, 3);
    assert.strictEqual(layers[0]!.length, 1);
    assert.strictEqual(layers[0]![0]!.id, 'A');

    // Layer 1 has both B and C
    assert.strictEqual(layers[1]!.length, 2);
    const layer1Ids = layers[1]!.map((s) => s.id).sort();
    assert.deepStrictEqual(layer1Ids, ['B', 'C']);

    // Layer 2 has D
    assert.strictEqual(layers[2]!.length, 1);
    assert.strictEqual(layers[2]![0]!.id, 'D');
  });

  it('should throw DAGValidationError when a cycle is detected and pinpoint the path', () => {
    // A -> B -> C -> A
    const steps: StepDefinition[] = [
      { id: 'A', name: 'A', dependencies: ['C'], run: async () => 1 },
      { id: 'B', name: 'B', dependencies: ['A'], run: async () => 2 },
      { id: 'C', name: 'C', dependencies: ['B'], run: async () => 3 },
    ];

    const dag = new DirectedAcyclicGraph(steps);

    assert.throws(
      () => {
        dag.validate();
      },
      (err: any) => {
        assert(err instanceof DAGValidationError);
        assert(err.message.includes('Cycle detected'));
        return true;
      }
    );
  });

  it('should throw when a step depends on a non-existent step', () => {
    const steps: StepDefinition[] = [
      { id: 'A', name: 'A', dependencies: ['ghost-step'], run: async () => 1 },
    ];

    const dag = new DirectedAcyclicGraph(steps);

    assert.throws(
      () => {
        dag.validate();
      },
      /depends on non-existent step "ghost-step"/
    );
  });
});
