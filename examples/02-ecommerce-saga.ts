/**
 * Example 2: E-Commerce Order Fulfillment (Saga Pattern)
 * Demonstrates:
 * - Distributed Saga transactions
 * - Failure handling & automatic backward compensation (rollback)
 */

import { Orchestrator, DEMO_SAGA_WORKFLOW } from '../src/index.js';

async function run() {
  console.log('\n======================================================');
  console.log('🛍 Running Example 02: E-Commerce Saga with Failure Rollback');
  console.log('======================================================\n');

  const orchestrator = new Orchestrator({ concurrency: 4 });
  orchestrator.registerWorkflow(DEMO_SAGA_WORKFLOW);

  orchestrator.getEventStore().onEvent((event) => {
    const time = new Date(event.timestamp).toLocaleTimeString();
    if (event.type.includes('COMPENSATION')) {
      console.log(`\x1b[35m[${time}] [${event.type}] (${event.stepId})\x1b[0m`);
    } else if (event.type.includes('FAILED')) {
      console.log(`\x1b[31m[${time}] [${event.type}] (${event.stepId}) ${JSON.stringify(event.payload)}\x1b[0m`);
    } else {
      console.log(`[${time}] [${event.type}] (${event.stepId || 'workflow'})`);
    }
  });

  console.log('Triggering workflow with payment failure simulation...\n');
  const instance = await orchestrator.runWorkflow('ecommerce-checkout-saga', {
    customerId: 'CUST-ALPHA-49',
    simulatePaymentFailure: true,
  });

  console.log('\nWorkflow Completed With Status:', instance.status);
  console.log('Error Captured:', instance.error);
  console.log('Inventory was automatically rolled back via Saga compensation!');
}

run().catch(console.error);
