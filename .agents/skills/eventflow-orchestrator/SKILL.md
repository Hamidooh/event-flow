---
name: eventflow-orchestrator
description: >-
  Expert guide, reference runbook, and architectural cheatsheet for designing,
  building, executing, and debugging distributed DAG workflows, Saga compensations,
  resilient retry policies, and real-time event streaming with EventFlow Engine.
  Use when creating new workflows, orchestrating async steps, recovering state via WAL,
  or interacting with the EventFlow REST & WebSocket APIs.
---

# EventFlow Engine — Agent Skill & Orchestration Guide

EventFlow is a lightweight, zero-dependency (Node.js native + `ws`) distributed DAG workflow and resilient event orchestration engine. It combines compile-time & runtime cycle-detecting topological graph schedulers, an append-only Write-Ahead Log (WAL) event store, bounded concurrency worker pools, and backward Saga rollback compensations.

---

## 1. Engine Core Concepts

### Directed Acyclic Graph (DAG) Scheduler
* **Cycle Detection:** Tarjan / 3-color Depth First Search (`WHITE`, `GRAY`, `BLACK`) prevents infinite dependency deadlocks before any task runs.
* **Topological Execution Layers:** Kahn's Algorithm partitions DAG steps into ordered parallel execution tiers. Steps in the same layer run concurrently up to the worker pool capacity.
* **Diamond Topologies:** Automatically resolves split-and-merge pipelines (e.g. Ingest $\rightarrow$ [Enrich Geo + Score Risk] $\rightarrow$ Load Warehouse).

### Resilient Worker Pool
* **Bounded Concurrency:** Strict semaphore-guarded queue preventing Node.js event-loop starvation and socket exhaustion.
* **Priority Queue:** High-priority recovery and compensation tasks preempt standard workflow steps.
* **Full Jitter Exponential Backoff:** $t_{sleep} = \text{random}(0, \min(M, B \times F^{\text{attempt}}))$ prevents thundering herds against external downstream APIs.

### Event Sourcing & Disk Write-Ahead Log (WAL)
* **Append-Only Disk Log:** Synchronously flushes state mutations (`WORKFLOW_CREATED`, `STEP_STARTED`, `STEP_COMPLETED`, etc.) to `.eventflow-data/events.wal`.
* **Deterministic Replay:** Reconstructs in-flight and terminal workflow states from the raw event stream on cold restart.

### Distributed Saga Compensation
* **LIFO Rollbacks:** When a step fails and exhausts all retries, all previously successful upstream steps that registered a `compensation` handler are executed in reverse order.

---

## 2. Defining Workflows

### Minimal Workflow Schema (`WorkflowDefinition`)

```typescript
import { WorkflowDefinition, StepContext } from 'eventflow-engine';

export const MyDataWorkflow: WorkflowDefinition = {
  id: 'customer-data-enrichment',
  name: 'Customer Data Enrichment Pipeline',
  version: '1.0.0',
  description: 'Validates raw incoming payloads, fetches credit data, and syncs to CRM.',
  concurrency: 5, // optional step-level concurrency limit
  steps: [
    {
      id: 'validate-input',
      name: 'Validate Input Schema',
      run: async (ctx: StepContext) => {
        ctx.log('Checking payload validity...');
        if (!ctx.input.customerId) {
          throw new Error('Missing customerId in initial input');
        }
        return { valid: true, customerId: ctx.input.customerId };
      },
    },
    {
      id: 'fetch-credit-score',
      name: 'Query Equifax Credit API',
      dependencies: ['validate-input'],
      timeoutMs: 3000,
      retryPolicy: {
        maxRetries: 3,
        initialDelayMs: 200,
        maxDelayMs: 2000,
        backoffFactor: 2,
        jitter: true,
      },
      run: async (ctx: StepContext) => {
        const upstream = ctx.getStepOutput('validate-input');
        ctx.log(`Fetching credit score for ${upstream.customerId}`);
        return { creditScore: 740, tier: 'PRIME' };
      },
    },
    {
      id: 'reserve-account-balance',
      name: 'Reserve Customer Balance',
      dependencies: ['fetch-credit-score'],
      run: async (ctx: StepContext) => {
        ctx.log('Holding reservation funds...');
        return { reservationId: 'RES-10492' };
      },
      // Backward rollback if downstream steps fail
      compensation: async (ctx: StepContext) => {
        ctx.log('COMPENSATION: Releasing held reservation funds!');
      },
    },
    {
      id: 'sync-crm',
      name: 'Sync Record to Salesforce',
      dependencies: ['reserve-account-balance'],
      run: async (ctx: StepContext) => {
        ctx.log('Syncing record downstream...');
        return { synced: true };
      },
    },
  ],
};
```

---

## 3. Programmatic Orchestration Runbook

### Step 1: Initialize Orchestrator with Persistent WAL
```typescript
import { Orchestrator, WALStorage } from 'eventflow-engine';

const wal = new WALStorage('.eventflow-data/events.wal');
await wal.init();

const orchestrator = new Orchestrator({
  concurrency: 10,
  walStorage: wal,
});

// Register workflows
orchestrator.registerWorkflow(MyDataWorkflow);
```

### Step 2: Trigger Execution
```typescript
// Asynchronous trigger (returns completed instance when DAG settles)
const instance = await orchestrator.runWorkflow('customer-data-enrichment', {
  customerId: 'CUST-8831',
  amount: 250.00,
});

console.log(`Execution ${instance.id} settled with status: ${instance.status}`);
console.log('Final Step Outputs:', instance.stepOutputs);
```

### Step 3: Lifecycle Controls (Pause, Resume, Cancel)
```typescript
// Pause execution between step boundaries
orchestrator.pauseWorkflow(instance.id);

// Resume paused execution
orchestrator.resumeWorkflow(instance.id);

// Abort in-flight steps immediately via AbortSignal
orchestrator.cancelWorkflow(instance.id);
```

---

## 4. REST API & WebSocket Reference

Server defaults to port `3030` (`http://localhost:3030`).

### REST Endpoints

| Method | Endpoint | Description | Query / Body |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/health` | Service uptime and memory usage | None |
| `GET` | `/api/metrics` | System throughput, queue depths, worker utilization | None |
| `GET` | `/api/workflows` | List all registered workflow templates | None |
| `POST` | `/api/workflows/:id/run` | Trigger workflow run | Body: JSON payload<br>`?sync=true` (optional) |
| `GET` | `/api/instances` | List all workflow execution instances | None |
| `GET` | `/api/instances/:id` | Get details, step states, and outputs for an instance | None |
| `POST` | `/api/instances/:id/pause` | Pause an active execution | None |
| `POST` | `/api/instances/:id/resume` | Resume a paused execution | None |
| `POST` | `/api/instances/:id/cancel` | Cancel an active execution | None |
| `GET` | `/api/instances/:id/events` | Get all WAL events for a specific execution | None |
| `GET` | `/api/events` | Stream/fetch all system events | `?since=<timestamp>` |

### WebSocket Protocol (`ws://localhost:3030/ws`)

1. **Initial Connection Payload (`INIT_STATE`):**
   ```json
   {
     "type": "INIT_STATE",
     "timestamp": 1793980000000,
     "metrics": { ... },
     "workflows": [
       { "id": "etl-data-pipeline", "name": "...", "steps": [ ... ] }
     ],
     "instances": [ ... ]
   }
   ```
2. **Real-time Event Broadcast (`WORKFLOW_EVENT`):**
   Broadcasted whenever any step starts, emits a log, retries, completes, or compensates.
   ```json
   {
     "type": "WORKFLOW_EVENT",
     "event": {
       "sequence": 42,
       "type": "STEP_COMPLETED",
       "workflowInstanceId": "...",
       "stepId": "reserve-inventory",
       "payload": { "durationMs": 450 }
     },
     "metrics": { ... }
   }
   ```

---

## 5. Command Line Interface (CLI)

```bash
# Start server with Web Visualizer on custom port
npx tsx src/cli/cli.ts start --port 3030 --concurrency 16

# Run High-Throughput Load Benchmark
npx tsx src/cli/cli.ts bench --workflows 50 --concurrency 10

# CLI Help
npx tsx src/cli/cli.ts --help
```

---

## 6. Verification & Test Suite

Always verify changes by executing the test suite:
```bash
# Typecheck TypeScript definitions
npm run typecheck

# Execute all unit and integration tests (15/15 tests)
npm test

# Run real-world showcase scripts
npm run example:etl
npm run example:saga
npm run example:enrich
```
