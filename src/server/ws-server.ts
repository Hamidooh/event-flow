/**
 * EventFlow - WebSocket Live Telemetry Server
 * Broadcasts execution events, metrics, and workflow updates in real-time.
 */

import { WebSocketServer, WebSocket } from 'ws';
import { Server as HttpServer } from 'node:http';
import { Orchestrator } from '../core/engine/orchestrator.js';
import { WorkflowEvent } from '../core/types.js';

export class LiveTelemetryServer {
  private wss: WebSocketServer;
  private readonly orchestrator: Orchestrator;
  private unsubscribeEvents?: () => void;

  constructor(server: HttpServer, orchestrator: Orchestrator) {
    this.orchestrator = orchestrator;
    this.wss = new WebSocketServer({ server, path: '/ws' });
    this.init();
  }

  private init(): void {
    this.wss.on('connection', (ws: WebSocket) => {
      // Send initial welcome & metrics payload
      const initialPayload = {
        type: 'INIT_STATE',
        timestamp: Date.now(),
        metrics: this.orchestrator.getMetrics(),
        workflows: this.orchestrator.getAllWorkflows().map((w) => ({
          id: w.id,
          name: w.name,
          version: w.version,
          description: w.description,
          stepsCount: w.steps.length,
          // Full step definitions so the UI board can render template cards
          steps: w.steps.map((s) => ({
            id: s.id,
            name: s.name,
            dependencies: s.dependencies ?? [],
          })),
        })),
        instances: this.orchestrator.getAllInstances(),
      };

      ws.send(JSON.stringify(initialPayload));

      ws.on('message', (message: string) => {
        try {
          const parsed = JSON.parse(message.toString());
          if (parsed.action === 'PING') {
            ws.send(JSON.stringify({ type: 'PONG', timestamp: Date.now() }));
          }
        } catch {
          // Ignore invalid messages
        }
      });
    });

    // Subscribe to orchestrator event store
    this.unsubscribeEvents = this.orchestrator.getEventStore().onEvent((event: WorkflowEvent) => {
      this.broadcast({
        type: 'WORKFLOW_EVENT',
        event,
        metrics: this.orchestrator.getMetrics(),
      });
    });
  }

  public broadcast(data: any): void {
    const payload = JSON.stringify(data);
    for (const client of this.wss.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    }
  }

  public close(): Promise<void> {
    if (this.unsubscribeEvents) {
      this.unsubscribeEvents();
    }
    return new Promise((resolve) => {
      this.wss.close(() => resolve());
    });
  }
}
