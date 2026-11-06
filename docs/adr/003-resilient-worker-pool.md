# ADR 003: Resilient Worker Pool, Backpressure & Saga Compensation

## Status
Accepted

## Context
When executing hundreds of concurrent steps, naive unbounded `Promise.all` spikes memory and overwhelms external services, while transient network failures can abort multi-stage transactions without rolling back state changes.

## Decision
1. **Bounded Priority Worker Pool**:
   - Implemented an internal priority queue with configurable concurrency (default: 10 worker slots).
   - Higher priority tasks preempt low-priority queue items when slots free up.
   - Live metrics (active workers, queued tasks, throughput, p95 durations) are tracked and broadcast.
2. **Exponential Backoff with Full Jitter**:
   - Retries apply the full jitter algorithm:
     $$\text{delay} = \min(M, I \cdot B^{\text{attempt}-1}) \times \text{Uniform}(0.5, 1.5)$$
   - Prevents the "Thundering Herd" problem on external dependencies.
3. **Saga Pattern Backward Compensation (LIFO)**:
   - When a step permanently fails after exhausting retries, the orchestrator halts remaining branches and triggers compensation functions for all previously completed steps in reverse order (Last-In, First-Out).

## Consequences
### Positive
- Prevents resource exhaustion and thread pool starvation under burst traffic.
- Ensures data consistency in distributed transactions across microservices.
- Full jitter smooths out retry traffic spikes across network boundaries.

### Negative
- Step authors must provide idempotent compensation routines for transactional guarantees.
