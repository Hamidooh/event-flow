/**
 * EventFlow - Demo Workflows
 * Built-in workflows for demonstrations, testing, and out-of-the-box showcase.
 */

import { WorkflowDefinition } from './types.js';

export const DEMO_ETL_WORKFLOW: WorkflowDefinition = {
  id: 'etl-data-pipeline',
  name: 'Real-Time ETL Data Pipeline',
  version: '1.2.0',
  description: 'Extracts raw customer transactions, parses schema, enriches with geolocation & risk scores in parallel, and loads to analytics store.',
  steps: [
    {
      id: 'extract-source',
      name: 'Extract Source Data',
      run: async (ctx) => {
        ctx.log('Connecting to upstream data source...');
        await new Promise((r) => setTimeout(r, 400));
        const count = ctx.input.recordsCount ?? 50;
        ctx.log(`Extracted ${count} raw transaction records.`);
        return { recordsCount: count, batchId: ctx.input.batchId ?? 'BATCH-001' };
      },
    },
    {
      id: 'validate-schema',
      name: 'Validate & Cleanse Schema',
      dependencies: ['extract-source'],
      run: async (ctx) => {
        const sourceData = ctx.getStepOutput('extract-source');
        ctx.log(`Cleansing ${sourceData.recordsCount} records against OpenAPI schema...`);
        await new Promise((r) => setTimeout(r, 300));
        return { cleansedRecords: sourceData.recordsCount, valid: true };
      },
    },
    {
      id: 'enrich-geolocation',
      name: 'Enrich IP Geolocation',
      dependencies: ['validate-schema'],
      run: async (ctx) => {
        ctx.log('Querying MaxMind GeoIP distributed database...');
        await new Promise((r) => setTimeout(r, 600));
        return { enrichedWithCountry: true, regions: ['US', 'EU', 'APAC'] };
      },
    },
    {
      id: 'score-fraud-risk',
      name: 'Calculate Fraud Risk (ML)',
      dependencies: ['validate-schema'],
      run: async (ctx) => {
        ctx.log('Invoking XGBoost risk inference scoring model...');
        await new Promise((r) => setTimeout(r, 550));
        return { averageRiskScore: 0.12, flaggedCount: 0 };
      },
    },
    {
      id: 'aggregate-and-load',
      name: 'Aggregate & Load Data Warehouse',
      dependencies: ['enrich-geolocation', 'score-fraud-risk'],
      run: async (ctx) => {
        const geo = ctx.getStepOutput('enrich-geolocation');
        const fraud = ctx.getStepOutput('score-fraud-risk');
        ctx.log('Writing partitioned Parquet files to Object Storage...');
        await new Promise((r) => setTimeout(r, 500));
        return {
          status: 'SUCCESS',
          loadedPartitions: geo.regions.length,
          fraudRisk: fraud.averageRiskScore,
        };
      },
    },
  ],
};

export const DEMO_SAGA_WORKFLOW: WorkflowDefinition = {
  id: 'ecommerce-checkout-saga',
  name: 'E-Commerce Order Fulfillment (Saga)',
  version: '2.0.0',
  description: 'Multi-service Saga with automatic rollback compensations (reserve inventory, charge card, generate shipping label).',
  steps: [
    {
      id: 'verify-customer',
      name: 'Verify Customer Identity',
      run: async (ctx) => {
        ctx.log('Checking user account standing and token...');
        await new Promise((r) => setTimeout(r, 300));
        return { verified: true, customerId: ctx.input.customerId || 'CUST-77' };
      },
    },
    {
      id: 'reserve-inventory',
      name: 'Reserve Stock in Warehouse',
      dependencies: ['verify-customer'],
      run: async (ctx) => {
        ctx.log('Acquiring distributed lock on inventory items...');
        await new Promise((r) => setTimeout(r, 450));
        return { reservationId: 'RES-9901', itemsCount: 3 };
      },
      compensation: async (ctx) => {
        ctx.log('COMPENSATION: Releasing held inventory stock reservation!');
        await new Promise((r) => setTimeout(r, 200));
      },
    },
    {
      id: 'process-payment',
      name: 'Charge Payment Gateway',
      dependencies: ['reserve-inventory'],
      retryPolicy: { maxRetries: 2, initialDelayMs: 200 },
      run: async (ctx) => {
        ctx.log('Submitting card authorization to Stripe...');
        await new Promise((r) => setTimeout(r, 500));
        if (ctx.input.simulatePaymentFailure) {
          throw new Error('Payment declined: Insufficient funds in account');
        }
        return { transactionId: 'TXN-554433', amount: 149.99 };
      },
      compensation: async (ctx) => {
        ctx.log('COMPENSATION: Issuing full payment refund to customer card!');
        await new Promise((r) => setTimeout(r, 300));
      },
    },
    {
      id: 'dispatch-shipping',
      name: 'Generate Shipping Waybill',
      dependencies: ['process-payment'],
      run: async (ctx) => {
        ctx.log('Scheduling courier pickup with FedEx API...');
        await new Promise((r) => setTimeout(r, 400));
        return { trackingNumber: 'TRK-981273910' };
      },
    },
  ],
};
