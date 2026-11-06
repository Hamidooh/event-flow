# ADR 002: Event Sourcing & Write-Ahead Log (WAL) Durability

## Status
Accepted

## Context
Distributed orchestrators must maintain state durability across crashes, network partitions, and host restarts. Traditional relational updates (`UPDATE workflows SET status = ...`) overwrite previous state, losing execution history, causing concurrency lock contention, and complicating distributed tracing.

## Decision
We adopted an **Append-Only Event Sourcing** architecture coupled with a disk-backed **Write-Ahead Log (WAL)**:
1. Every state mutation is represented as an immutable, sequentially-indexed `WorkflowEvent` (`WORKFLOW_CREATED`, `STEP_STARTED`, `STEP_COMPLETED`, etc.).
2. State is computed as a pure fold over the event stream:
   $$S_{t} = f(S_{t-1}, e_t)$$
3. The `WALStorage` engine synchronously appends each event to `.eventflow-data/events.wal` in JSON Lines format before in-memory notification.
4. Periodic snapshots are recorded in `snapshots.json` to compact log files and allow rapid recovery upon service reboot.

## Consequences
### Positive
- **Complete Auditability & Time-Travel Debugging**: Workflows can be replayed from any arbitrary sequence number to inspect state transitions.
- **High Throughput Append**: Sequential disk writes bypass database index updates and row-locking bottlenecks.
- **Natural Real-Time Pub/Sub**: WebSocket clients tap directly into the append-only event stream for reactive telemetry.

### Negative
- Replaying extremely long event histories can incur computational cost without snapshots. Addressed by automated snapshotting.
