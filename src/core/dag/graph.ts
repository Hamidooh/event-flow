/**
 * EventFlow - Directed Acyclic Graph (DAG) Engine
 * Cycle detection (Tarjan/DFS), Kahn's topological sorting, and dependency resolution.
 */

import { StepDefinition } from '../types.js';

export class DAGValidationError extends Error {
  constructor(message: string, public readonly cyclePath?: string[]) {
    super(message);
    this.name = 'DAGValidationError';
  }
}

export class DirectedAcyclicGraph {
  private readonly nodes: Map<string, StepDefinition> = new Map();
  private readonly adjList: Map<string, Set<string>> = new Map(); // parent -> children
  private readonly revAdjList: Map<string, Set<string>> = new Map(); // child -> parents (dependencies)

  constructor(steps: StepDefinition[] = []) {
    for (const step of steps) {
      this.addStep(step);
    }
  }

  public addStep(step: StepDefinition): void {
    if (this.nodes.has(step.id)) {
      throw new DAGValidationError(`Duplicate step id detected: "${step.id}"`);
    }

    this.nodes.set(step.id, step);
    if (!this.adjList.has(step.id)) {
      this.adjList.set(step.id, new Set());
    }
    if (!this.revAdjList.has(step.id)) {
      this.revAdjList.set(step.id, new Set());
    }

    const dependencies = step.dependencies ?? [];
    for (const depId of dependencies) {
      if (!this.adjList.has(depId)) {
        this.adjList.set(depId, new Set());
      }
      this.adjList.get(depId)!.add(step.id);
      this.revAdjList.get(step.id)!.add(depId);
    }
  }

  public getStep(id: string): StepDefinition | undefined {
    return this.nodes.get(id);
  }

  public getAllSteps(): StepDefinition[] {
    return Array.from(this.nodes.values());
  }

  public getDependencies(stepId: string): string[] {
    return Array.from(this.revAdjList.get(stepId) ?? []);
  }

  public getDependents(stepId: string): string[] {
    return Array.from(this.adjList.get(stepId) ?? []);
  }

  public getRootSteps(): StepDefinition[] {
    const roots: StepDefinition[] = [];
    for (const [id, step] of this.nodes.entries()) {
      const deps = this.revAdjList.get(id);
      if (!deps || deps.size === 0) {
        roots.push(step);
      }
    }
    return roots;
  }

  public getLeafSteps(): StepDefinition[] {
    const leaves: StepDefinition[] = [];
    for (const [id, step] of this.nodes.entries()) {
      const children = this.adjList.get(id);
      if (!children || children.size === 0) {
        leaves.push(step);
      }
    }
    return leaves;
  }

  /**
   * Validates the graph integrity:
   * 1. Checks that all declared dependencies exist as registered steps.
   * 2. Checks for cycles using 3-color DFS to provide human-readable cycle paths.
   */
  public validate(): void {
    // 1. Check missing dependencies
    for (const [stepId, deps] of this.revAdjList.entries()) {
      for (const depId of deps) {
        if (!this.nodes.has(depId)) {
          throw new DAGValidationError(
            `Step "${stepId}" depends on non-existent step "${depId}".`
          );
        }
      }
    }

    // 2. Cycle detection via 3-color DFS (0 = unvisited, 1 = visiting, 2 = visited)
    const visited = new Map<string, number>();
    const parentPath: string[] = [];

    const dfs = (nodeId: string): string[] | null => {
      visited.set(nodeId, 1); // visiting
      parentPath.push(nodeId);

      const children = this.adjList.get(nodeId) ?? new Set();
      for (const childId of children) {
        const state = visited.get(childId) ?? 0;
        if (state === 1) {
          // Found cycle! Extract path from first appearance of childId
          const cycleStartIndex = parentPath.indexOf(childId);
          const cycle = parentPath.slice(cycleStartIndex).concat(childId);
          return cycle;
        }
        if (state === 0) {
          const cycle = dfs(childId);
          if (cycle) return cycle;
        }
      }

      parentPath.pop();
      visited.set(nodeId, 2); // fully visited
      return null;
    };

    for (const nodeId of this.nodes.keys()) {
      if ((visited.get(nodeId) ?? 0) === 0) {
        const cycle = dfs(nodeId);
        if (cycle) {
          throw new DAGValidationError(
            `Cycle detected in workflow: ${cycle.join(' -> ')}`,
            cycle
          );
        }
      }
    }
  }

  /**
   * Performs Kahn's algorithm to compute topological order.
   * Returns steps partitioned into parallel execution stages (layers).
   */
  public getExecutionLayers(): StepDefinition[][] {
    this.validate();

    const inDegree = new Map<string, number>();
    for (const [id] of this.nodes) {
      inDegree.set(id, this.revAdjList.get(id)?.size ?? 0);
    }

    const layers: StepDefinition[][] = [];
    let currentLayer: string[] = [];

    for (const [id, deg] of inDegree.entries()) {
      if (deg === 0) {
        currentLayer.push(id);
      }
    }

    const processedCount = { value: 0 };

    while (currentLayer.length > 0) {
      const stepLayer: StepDefinition[] = [];
      const nextLayer: string[] = [];

      for (const stepId of currentLayer) {
        stepLayer.push(this.nodes.get(stepId)!);
        processedCount.value++;

        const children = this.adjList.get(stepId) ?? new Set();
        for (const childId of children) {
          const newDeg = (inDegree.get(childId) ?? 1) - 1;
          inDegree.set(childId, newDeg);
          if (newDeg === 0) {
            nextLayer.push(childId);
          }
        }
      }

      layers.push(stepLayer);
      currentLayer = nextLayer;
    }

    if (processedCount.value !== this.nodes.size) {
      throw new DAGValidationError('Failed to topologically order DAG. Graph contains cycles.');
    }

    return layers;
  }

  /**
   * Checks whether all dependencies of a target step have successfully completed.
   */
  public areDependenciesSatisfied(stepId: string, completedStepIds: Set<string>): boolean {
    const deps = this.revAdjList.get(stepId) ?? new Set();
    for (const dep of deps) {
      if (!completedStepIds.has(dep)) {
        return false;
      }
    }
    return true;
  }
}
