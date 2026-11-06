/**
 * Example 1: ETL Pipeline
 * Demonstrates:
 * - DAG topological dependency ordering
 * - Parallel branching (geolocation & fraud detection)
 * - Step output propagation to downstream steps
 */

import { Orchestrator, DEMO_ETL_WORKFLOW } from '../src/index.js';

async function run() {
  console.log('\n======================================================');
  console.log('🚀 Running Example 01: Real-Time ETL Data Pipeline');
  console.log('======================================================\n');

  const orchestrator = new Orchestrator({ concurrency: 4 });
  orchestrator.registerWorkflow(DEMO_ETL_WORKFLOW);

  // Subscribe to real-time events in terminal
  orchestrator.getEventStore().onEvent((event) => {
    const time = new Date(event.timestamp).toLocaleTimeString();
    console.log(`[${time}] [${event.type}] ${event.stepId ? `(${event.stepId})` : ''}`);
  });

  const instance = await orchestrator.runWorkflow('etl-data-pipeline', {
    batchId: 'BATCH-2026-X',
    recordsCount: 250,
  });

  console.log('\n✔ Workflow Execution Finished!');
  console.log('Instance ID:', instance.id);
  console.log('Status:', instance.status);
  console.log('Duration:', `${instance.durationMs}ms`);
  console.log('\nOutputs produced by steps:');
  console.log(JSON.stringify(instance.stepOutputs, null, 2));
}

run().catch(console.error);
