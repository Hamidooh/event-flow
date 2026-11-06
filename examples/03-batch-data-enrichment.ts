/**
 * Example 3: Batch Data Enrichment with Retries & Exponential Backoff
 * Demonstrates:
 * - Dynamic retries with jitter
 * - Diamond DAG topology (A -> B, C -> D)
 * - Worker pool scheduling
 */

import { Orchestrator, WorkflowDefinition } from '../src/index.js';

let flakeCounter = 0;

const batchWorkflow: WorkflowDefinition = {
  id: 'batch-enrichment-pipeline',
  name: 'Multi-API Batch Enrichment',
  version: '1.0.0',
  steps: [
    {
      id: 'fetch-documents',
      name: 'Fetch Document Corpus',
      run: async (ctx) => {
        ctx.log('Fetching documents from S3 bucket...');
        await new Promise((r) => setTimeout(r, 200));
        return { docIds: ['doc-1', 'doc-2', 'doc-3', 'doc-4'] };
      },
    },
    {
      id: 'sentiment-analysis',
      name: 'Run Sentiment NLP',
      dependencies: ['fetch-documents'],
      retryPolicy: { maxRetries: 3, initialDelayMs: 150 },
      run: async (ctx) => {
        flakeCounter++;
        // Simulate flaky 3rd-party API that fails once before succeeding
        if (flakeCounter === 1) {
          ctx.log('Simulating NLP API rate limit 429...');
          throw new Error('Rate limit exceeded: 429 Too Many Requests');
        }
        ctx.log('NLP analysis successfully processed.');
        return { averageSentiment: 0.85 };
      },
    },
    {
      id: 'entity-extraction',
      name: 'Extract Named Entities',
      dependencies: ['fetch-documents'],
      run: async (ctx) => {
        ctx.log('Named Entity Recognition (NER) inference complete.');
        await new Promise((r) => setTimeout(r, 250));
        return { entitiesFound: ['Antigravity', 'Node.js', 'TypeScript'] };
      },
    },
    {
      id: 'index-search',
      name: 'Index to Search Engine',
      dependencies: ['sentiment-analysis', 'entity-extraction'],
      run: async (ctx) => {
        const sentiment = ctx.getStepOutput('sentiment-analysis');
        const entities = ctx.getStepOutput('entity-extraction');
        ctx.log('Pushing final enriched documents to OpenSearch cluster...');
        await new Promise((r) => setTimeout(r, 200));
        return {
          indexed: true,
          sentiment: sentiment.averageSentiment,
          entitiesCount: entities.entitiesFound.length,
        };
      },
    },
  ],
};

async function run() {
  console.log('\n======================================================');
  console.log('⚡ Running Example 03: Batch Data Enrichment & Retries');
  console.log('======================================================\n');

  const orchestrator = new Orchestrator({ concurrency: 4 });
  orchestrator.registerWorkflow(batchWorkflow);

  orchestrator.getEventStore().onEvent((evt) => {
    if (evt.type === 'STEP_RETRYING') {
      console.log(`\x1b[33m[RETRY] Step "${evt.stepId}" failed (Attempt #${evt.payload.attempt}). Retrying with backoff...\x1b[0m`);
    } else if (evt.type === 'STEP_COMPLETED') {
      console.log(`\x1b[32m[DONE] Step "${evt.stepId}" completed in ${evt.payload.durationMs}ms\x1b[0m`);
    }
  });

  const instance = await orchestrator.runWorkflow('batch-enrichment-pipeline');

  console.log('\nPipeline completed with status:', instance.status);
  console.log('Final Search Index Output:', instance.stepOutputs['index-search']);
}

run().catch(console.error);
