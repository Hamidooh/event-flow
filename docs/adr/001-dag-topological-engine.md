# ADR 001: DAG Topological Engine & Cycle Detection

## Status
Accepted

## Context
Orchestrating multi-step workflows requires deterministic execution order while maximizing concurrent execution of independent branches. Furthermore, cyclic dependencies between tasks lead to permanent deadlocks and infinite loops. We need an algorithm to validate workflow definitions upfront and schedule steps as their prerequisites resolve.

## Decision
We implemented a hybrid graph traversal approach:
1. **Upfront Validation with 3-Color Depth-First Search (DFS)**:
   - Nodes are colored WHITE (unvisited), GRAY (currently visiting in call stack), and BLACK (fully visited).
   - Any edge pointing to a GRAY node immediately triggers a cycle detection error.
   - The traversal stack is retained to pinpoint the exact cyclic path (e.g. `StepA -> StepB -> StepC -> StepA`), providing actionable diagnostics to developers.
2. **Kahn's Topological Sorting for Stage Partitioning**:
   - In-degree count is computed for every node.
   - Roots (in-degree 0) form Stage 0.
   - As nodes finish, in-degrees of children are decremented; when they reach 0, they are dynamically dispatched to the worker pool.

## Consequences
### Positive
- Graph validation is $O(V + E)$ time complexity, ensuring millisecond-level initialization even for complex graphs.
- Clear, descriptive cycle error messages prevent cryptic runtime deadlocks.
- Enables dynamic parallel execution across multiple branches without manual synchronization primitives.

### Negative
- Requires workflows to be fully declared before execution starts (dynamic runtime step generation requires sub-workflow orchestration).
