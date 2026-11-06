#!/usr/bin/env node
/**
 * EventFlow - Command Line Interface (CLI)
 * Complete CLI tool for starting server, executing workflows, benchmarks, and inspections.
 */

import { Orchestrator } from '../core/engine/orchestrator.js';
import { EventFlowServer } from '../server/http-server.js';
import { WALStorage } from '../core/state/wal-storage.js';
import { DEMO_ETL_WORKFLOW, DEMO_SAGA_WORKFLOW } from '../core/demo-workflows.js';

export { DEMO_ETL_WORKFLOW, DEMO_SAGA_WORKFLOW };

async function main() {
  const args = process.argv.slice(2);
  const command = args[0] || 'start';

  const getArg = (name: string, fallback: string) => {
    const idx = args.indexOf(name);
    return idx !== -1 && args[idx + 1] ? args[idx + 1]! : fallback;
  };

  if (command === '--help' || command === '-h' || command === 'help') {
    printHelp();
    process.exit(0);
  }

  if (command === 'start' || command === 'dev') {
    const port = parseInt(getArg('--port', '3000'), 10);
    const concurrency = parseInt(getArg('--concurrency', '10'), 10);

    const walStorage = new WALStorage();
    await walStorage.init();

    const orchestrator = new Orchestrator({ concurrency, walStorage });
    orchestrator.registerWorkflow(DEMO_ETL_WORKFLOW);
    orchestrator.registerWorkflow(DEMO_SAGA_WORKFLOW);

    const server = new EventFlowServer({ port, orchestrator });
    await server.start();

    printBanner(port, concurrency);

    // Keep alive
    process.on('SIGINT', async () => {
      console.log('\nGracefully shutting down EventFlow server...');
      await server.stop();
      await walStorage.close();
      process.exit(0);
    });
  } else if (command === 'bench') {
    await runBenchmark(args);
  } else {
    console.error(`Unknown command: "${command}". Run "eventflow --help" for usage.`);
    process.exit(1);
  }
}

async function runBenchmark(args: string[]) {
  console.log('\n🚀 Starting EventFlow High-Throughput Engine Benchmark...');
  const count = parseInt(args.includes('--workflows') ? args[args.indexOf('--workflows') + 1]! : '30', 10);
  const concurrency = parseInt(args.includes('--concurrency') ? args[args.indexOf('--concurrency') + 1]! : '10', 10);

  const orchestrator = new Orchestrator({ concurrency });
  orchestrator.registerWorkflow(DEMO_ETL_WORKFLOW);

  console.log(`Running ${count} concurrent workflow instances with ${concurrency} worker threads...`);
  const start = Date.now();

  const promises = [];
  for (let i = 0; i < count; i++) {
    promises.push(
      orchestrator.runWorkflow('etl-data-pipeline', { batchId: `bench-${i}`, recordsCount: 100 })
    );
  }

  await Promise.all(promises);
  const durationMs = Date.now() - start;
  const metrics = orchestrator.getMetrics();

  console.log('\n📊 Benchmark Results:');
  console.log(`-----------------------------------------------`);
  console.log(`Total Workflows Completed: ${metrics.totalWorkflowsExecuted}`);
  console.log(`Total Execution Time:      ${durationMs}ms`);
  console.log(`Throughput:                ${((count / durationMs) * 1000).toFixed(2)} workflows/sec`);
  console.log(`Step Level Tasks Executed: ${metrics.workerPool.completedTasks}`);
  console.log(`Average Step Execution:    ${metrics.workerPool.averageExecutionTimeMs}ms`);
  console.log(`Error Count:               ${metrics.failedWorkflows}`);
  console.log(`-----------------------------------------------\n`);
}

function printBanner(port: number, concurrency: number) {
  console.log(`
\x1b[36m  ███████╗██╗   ██╗███████╗███╗   ██╗████████╗███████╗██╗      ██████╗ ██╗    ██╗
  ██╔════╝██║   ██║██╔════╝████╗  ██║╚══██╔══╝██╔════╝██║     ██╔═══██╗██║    ██║
  █████╗  ██║   ██║█████╗  ██╔██╗ ██║   ██║   █████╗  ██║     ██║   ██║██║ █╗ ██║
  ██╔══╝  ╚██╗ ██╔╝██╔══╝  ██║╚██╗██║   ██║   ██╔══╝  ██║     ██║   ██║██║███╗██║
  ███████╗ ╚████╔╝ ███████╗██║ ╚████║   ██║   ██║     ███████╗╚██████╔╝╚███╔███╔╝
  ╚══════╝  ╚═══╝  ╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚═╝     ╚══════╝ ╚═════╝  ╚══╝╚══╝ \x1b[0m
  \x1b[90mDistributed DAG Workflow & Resilient Event Orchestration Engine\x1b[0m

  \x1b[32m✔ Server listening at:\x1b[0m   \x1b[1mhttp://localhost:${port}\x1b[0m
  \x1b[32m✔ Live Web Visualizer:\x1b[0m   \x1b[1mhttp://localhost:${port}/\x1b[0m
  \x1b[32m✔ WebSocket Telemetry:\x1b[0m   \x1b[1mws://localhost:${port}/ws\x1b[0m
  \x1b[32m✔ Worker Concurrency:\x1b[0m    \x1b[33m${concurrency} parallel workers\x1b[0m
  \x1b[32m✔ Write-Ahead Log:\x1b[0m       \x1b[33m.eventflow-data/events.wal\x1b[0m

  \x1b[90mPress Ctrl+C to terminate\x1b[0m
`);
}

function printHelp() {
  console.log(`
EventFlow CLI v1.0.0

Usage:
  eventflow <command> [options]

Commands:
  start               Start the orchestration HTTP server, WebSocket telemetry, and Web UI
  bench               Run synthetic load benchmark across parallel workflows
  help, --help        Show this help message

Options:
  --port <number>         Port to bind server to (default: 3000)
  --concurrency <number>  Maximum parallel worker threads (default: 10)
  --workflows <number>    Number of workflows for benchmark (default: 30)

Examples:
  eventflow start --port 3000
  eventflow bench --workflows 50 --concurrency 16
`);
}

// Check if running as script entry point
const isMainScript =
  process.argv[1]?.endsWith('cli.ts') ||
  process.argv[1]?.endsWith('cli.js') ||
  process.argv[1]?.endsWith('eventflow');

if (isMainScript) {
  main().catch((err) => {
    console.error('Fatal CLI Error:', err);
    process.exit(1);
  });
}
