# EventFlow Architecture & Engineering Specification

EventFlow is a lightweight, distributed DAG workflow and event orchestration engine designed for high throughput, predictable latency, and resilient state recovery in TypeScript.

---

## 1. System Overview

```
                      ┌──────────────────────────────────────────────┐
                      │              Clients & Consumers             │
                      │   (Web Dashboard • CLI • REST Client • WS)   │
                      └───────────────────────┬──────────────────────┘
                                              │ HTTP / WebSocket
                                              ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │                                 EventFlow Server                                  │
 │                                                                                   │
 │  ┌───────────────────────┐                          ┌──────────────────────────┐  │
 │  │    HTTP REST Server   │                          │  Live Telemetry WS Hub   │  │
 │  │  (/api/workflows,     │                          │  (/ws event broadcasts,  │  │
 │  │   /api/instances)     │                          │   state sync)            │  │
 │  └───────────┬───────────┘                          └────────────▲─────────────┘  │
 │              │                                                   │                │
 │              ▼                                                   │                │
 │  ┌───────────────────────────────────────────────────────────────┴─────────────┐  │
 │  │                           Workflow Orchestrator                            │  │
 │  │                                                                             │  │
 │  │  ┌───────────────────────┐  ┌───────────────────────┐  ┌─────────────────┐  │  │
 │  │  │  DAG Engine & Kahn    │  │  Worker Pool & Queue  │  │ Saga Compenser  │  │  │
 │  │  │  (Cycle check, layers)│  │  (Priority, slots)    │  │ (LIFO rollback) │  │  │
 │  │  └───────────────────────┘  └───────────────────────┘  └─────────────────┘  │  │
 │  └───────────────────────────┬─────────────────────────────────────────────────┘  │
 │                              │                                                    │
 │                              ▼                                                    │
 │  ┌─────────────────────────────────────────────────────────────────────────────┐  │
 │  │                     Event Sourcing & Persistence Layer                      │  │
 │  │                                                                             │  │
 │  │  ┌───────────────────────────────┐     ┌─────────────────────────────────┐  │  │
 │  │  │   In-Memory Event Store       │────▶│    Disk Write-Ahead Log (WAL)   │  │  │
 │  │  │   (Pub/Sub, sequence streams) │     │    (.eventflow-data/events.wal) │  │  │
 │  │  └───────────────────────────────┘     └─────────────────────────────────┘  │  │
 │  └─────────────────────────────────────────────────────────────────────────────┘  │
 └───────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core Subsystems

### 2.1 Directed Acyclic Graph (DAG) Engine (`src/core/dag/graph.ts`)

The DAG engine models dependencies between tasks using an adjacency list. Workflows must be strictly acyclic.

#### Cycle Detection (Tarjan / 3-Color DFS)
Before any execution begins, `DirectedAcyclicGraph.prototype.validate()` traverses the graph using a 3-color DFS marking:
- **WHITE (0)**: Unvisited node.
- **GRAY (1)**: Currently visiting in the current DFS call stack.
- **BLACK (2)**: Node and all descendants have been visited.

If an edge $(u, v)$ points to a GRAY node, a cycle is present. The algorithm extracts the call stack slice to report the exact human-readable cycle:
$$\text{Cycle detected: StepA} \to \text{StepB} \to \text{StepC} \to \text{StepA}$$

#### Topological Sorting (Kahn's Algorithm)
Execution stages are computed by tracking in-degree $d_{\text{in}}(v)$. Nodes with $d_{\text{in}} = 0$ are scheduled into the initial layer. As tasks complete, dependent nodes with satisfied prerequisites are immediately enqueued into the worker pool.

---

### 2.2 Event Sourcing & State Machine (`src/core/state/`)

EventFlow does not maintain mutable workflow records. Instead, state is derived from an append-only log of immutable events:

```
[WORKFLOW_CREATED] ──▶ [WORKFLOW_STARTED] ──▶ [STEP_QUEUED] ──▶ [STEP_STARTED]
                                                                        │
        ┌───────────────────────────────────────────────────────────────┴─────────┐
        ▼                                                                         ▼
 [STEP_COMPLETED]                                                           [STEP_RETRYING]
        │                                                                         │
        ▼                                                                         ▼
 [WORKFLOW_COMPLETED]                                                       [STEP_FAILED]
                                                                                  │
                                                                                  ▼
                                                                     [STEP_COMPENSATION_STARTED]
                                                                                  │
                                                                                  ▼
                                                                        [WORKFLOW_FAILED]
```

#### Deterministic Replay
The engine reconstructs any workflow instance at any historical point by folding events:
$$\text{State}_N = \sum_{i=1}^N \Delta(e_i)$$

---

### 2.3 Resilient Worker Pool & Scheduling (`src/core/engine/worker-pool.ts`)

The worker pool limits concurrency to avoid thread pool and file descriptor starvation:
- **Priority Queue**: Tasks are inserted into a priority-ordered list. High-priority compensation or critical tasks bypass standard pipeline steps.
- **Heartbeat & Metrics**: Collects active workers, queue wait time, execution duration, and p95 latency.

---

### 2.4 Distributed Saga Pattern & Backward Compensation

When a distributed workflow fails (e.g., payment declined after inventory was allocated), simply stopping leaves the system in an inconsistent state.

EventFlow incorporates the **Saga Pattern**:
1. When step $S_k$ fails after all retries are exhausted, the orchestrator halts remaining steps.
2. It queries all successfully executed steps in $\{S_1, \dots, S_{k-1}\}$ that define a `compensation` handler.
3. Compensations are executed in reverse order (**LIFO**):
   $$C_{k-1} \to C_{k-2} \to \dots \to C_1$$
4. Compensation lifecycle events (`STEP_COMPENSATION_STARTED`, `STEP_COMPENSATION_COMPLETED`, `STEP_COMPENSATION_FAILED`) are recorded in the event store.

---

### 2.5 Exponential Backoff with Full Jitter (`src/core/engine/retry-policy.ts`)

To avoid the Thundering Herd phenomenon during downstream service degradation, retries follow the Full Jitter formula:

$$\text{backoff}(a) = \min(M, I \cdot B^{a-1})$$
$$\text{delay}(a) = \text{backoff}(a) \times \text{Uniform}(0.5, 1.5)$$

Where:
- $I$: Initial delay ($300\text{ms}$)
- $B$: Backoff multiplier ($2.0$)
- $M$: Max delay limit ($8000\text{ms}$)
- $a$: Current attempt count

---

### 2.6 Real-Time Telemetry & Web Dashboard (`src/server/` & `src/ui/`)

- **WebSocket Protocol**: Connected frontends receive live streams of `WorkflowEvent` objects alongside aggregate engine telemetry.
- **Zero-Dependency Serving**: Static assets and API endpoints are served directly by the built-in HTTP server without external framework dependencies.
- **Canvas / SVG DAG Renderer**: Automatically computes topological levels and renders connecting bezier curves with directional arrows, animated state indicators, and interactive JSON inspectors.
