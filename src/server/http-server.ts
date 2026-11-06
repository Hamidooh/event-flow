/**
 * EventFlow - HTTP REST API & Static Dashboard Server
 * Zero-dependency robust HTTP server with full REST endpoints for orchestration.
 */

import * as http from 'node:http';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Orchestrator } from '../core/engine/orchestrator.js';
import { LiveTelemetryServer } from './ws-server.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export interface ServerOptions {
  port?: number;
  host?: string;
  orchestrator: Orchestrator;
}

export class EventFlowServer {
  private server: http.Server;
  private wsServer?: LiveTelemetryServer;
  private readonly orchestrator: Orchestrator;
  private readonly port: number;
  private readonly host: string;

  constructor(options: ServerOptions) {
    this.orchestrator = options.orchestrator;
    this.port = options.port ?? 3000;
    this.host = options.host ?? '0.0.0.0';
    this.server = http.createServer(this.handleRequest.bind(this));
    this.wsServer = new LiveTelemetryServer(this.server, this.orchestrator);
  }

  private async handleRequest(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const parsedUrl = new URL(req.url ?? '/', `http://${req.headers.host || 'localhost'}`);
    const pathname = parsedUrl.pathname;

    try {
      // 1. Health
      if (req.method === 'GET' && pathname === '/api/health') {
        return this.sendJson(res, 200, {
          status: 'UP',
          version: '1.0.0',
          uptime: process.uptime(),
          memoryUsage: process.memoryUsage(),
        });
      }

      // 2. Metrics
      if (req.method === 'GET' && pathname === '/api/metrics') {
        return this.sendJson(res, 200, this.orchestrator.getMetrics());
      }

      // 3. List Workflows
      if (req.method === 'GET' && pathname === '/api/workflows') {
        const workflows = this.orchestrator.getAllWorkflows().map((w) => ({
          id: w.id,
          name: w.name,
          version: w.version,
          description: w.description,
          steps: w.steps.map((s) => ({
            id: s.id,
            name: s.name,
            dependencies: s.dependencies ?? [],
            timeoutMs: s.timeoutMs,
            hasCompensation: typeof s.compensation === 'function',
          })),
        }));
        return this.sendJson(res, 200, { workflows });
      }

      // 4. Trigger Workflow Run: POST /api/workflows/:id/run
      const runMatch = pathname.match(/^\/api\/workflows\/([^/]+)\/run$/);
      if (req.method === 'POST' && runMatch) {
        const workflowId = decodeURIComponent(runMatch[1]!);
        const body = await this.readJsonBody(req);
        
        // Run in background or wait for completion based on query param ?sync=true
        const isSync = parsedUrl.searchParams.get('sync') === 'true';

        if (isSync) {
          const instance = await this.orchestrator.runWorkflow(workflowId, body);
          return this.sendJson(res, 200, instance);
        } else {
          // Fire and return instance promise / initial state
          const runPromise = this.orchestrator.runWorkflow(workflowId, body);
          // Wait slightly for instance to be registered
          await new Promise((r) => setTimeout(r, 20));
          const instances = this.orchestrator.getAllInstances();
          const latestInstance = instances[instances.length - 1];

          runPromise.catch((err) => {
            console.error(`[Server] Background workflow error [${workflowId}]:`, err.message);
          });

          return this.sendJson(res, 202, {
            message: 'Workflow execution accepted',
            instanceId: latestInstance?.id,
            workflowId,
          });
        }
      }

      // 5. List Instances: GET /api/instances
      if (req.method === 'GET' && pathname === '/api/instances') {
        return this.sendJson(res, 200, { instances: this.orchestrator.getAllInstances() });
      }

      // 6. Get Specific Instance: GET /api/instances/:id
      const instanceMatch = pathname.match(/^\/api\/instances\/([^/]+)$/);
      if (req.method === 'GET' && instanceMatch) {
        const instanceId = decodeURIComponent(instanceMatch[1]!);
        const instance = this.orchestrator.getInstance(instanceId);
        if (!instance) {
          return this.sendJson(res, 404, { error: `Workflow instance "${instanceId}" not found` });
        }
        return this.sendJson(res, 200, instance);
      }

      // 7. Pause Instance: POST /api/instances/:id/pause
      const pauseMatch = pathname.match(/^\/api\/instances\/([^/]+)\/pause$/);
      if (req.method === 'POST' && pauseMatch) {
        const instanceId = decodeURIComponent(pauseMatch[1]!);
        const success = this.orchestrator.pauseWorkflow(instanceId);
        return this.sendJson(res, success ? 200 : 400, { success, instanceId });
      }

      // 8. Resume Instance: POST /api/instances/:id/resume
      const resumeMatch = pathname.match(/^\/api\/instances\/([^/]+)\/resume$/);
      if (req.method === 'POST' && resumeMatch) {
        const instanceId = decodeURIComponent(resumeMatch[1]!);
        const success = this.orchestrator.resumeWorkflow(instanceId);
        return this.sendJson(res, success ? 200 : 400, { success, instanceId });
      }

      // 9. Cancel Instance: POST /api/instances/:id/cancel
      const cancelMatch = pathname.match(/^\/api\/instances\/([^/]+)\/cancel$/);
      if (req.method === 'POST' && cancelMatch) {
        const instanceId = decodeURIComponent(cancelMatch[1]!);
        const success = this.orchestrator.cancelWorkflow(instanceId);
        return this.sendJson(res, success ? 200 : 400, { success, instanceId });
      }

      // 10. Instance Events: GET /api/instances/:id/events
      const eventsMatch = pathname.match(/^\/api\/instances\/([^/]+)\/events$/);
      if (req.method === 'GET' && eventsMatch) {
        const instanceId = decodeURIComponent(eventsMatch[1]!);
        const events = this.orchestrator.getEventStore().getEventsForWorkflow(instanceId);
        return this.sendJson(res, 200, { events });
      }

      // 11. All Events: GET /api/events
      if (req.method === 'GET' && pathname === '/api/events') {
        const since = Number(parsedUrl.searchParams.get('since') ?? 0);
        const events = since > 0
          ? this.orchestrator.getEventStore().getEventsSince(since)
          : this.orchestrator.getEventStore().getAllEvents();
        return this.sendJson(res, 200, { events });
      }

      // 12. Serve Static UI Dashboard (index.html, css, js)
      if (req.method === 'GET') {
        const served = await this.serveStaticUI(pathname, res);
        if (served) return;
      }

      // 404 Fallback
      return this.sendJson(res, 404, { error: 'Not Found' });
    } catch (err: any) {
      console.error('[EventFlowServer] Request Error:', err);
      return this.sendJson(res, 500, { error: err.message ?? 'Internal Server Error' });
    }
  }

  private async serveStaticUI(pathname: string, res: http.ServerResponse): Promise<boolean> {
    let cleanPath = pathname === '/' ? '/index.html' : pathname;
    if (cleanPath.startsWith('/ui/')) {
      cleanPath = cleanPath.slice(3);
    }

    const possibleDirs = [
      path.join(process.cwd(), 'src', 'ui'),
      path.join(process.cwd(), 'dist', 'ui'),
      path.join(__dirname, '..', 'ui'),
    ];

    let filePath: string | null = null;
    for (const dir of possibleDirs) {
      const candidate = path.join(dir, cleanPath);
      if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
        filePath = candidate;
        break;
      }
    }

    if (!filePath) {
      return false;
    }

    const ext = path.extname(filePath).toLowerCase();
    const mimeTypes: Record<string, string> = {
      '.html': 'text/html; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.js': 'application/javascript; charset=utf-8',
      '.json': 'application/json; charset=utf-8',
      '.svg': 'image/svg+xml',
      '.png': 'image/png',
    };

    const contentType = mimeTypes[ext] || 'text/plain';
    const content = await fs.promises.readFile(filePath);
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(content);
    return true;
  }

  private async readJsonBody(req: http.IncomingMessage): Promise<any> {
    return new Promise((resolve, reject) => {
      let data = '';
      req.on('data', (chunk) => {
        data += chunk;
        if (data.length > 5 * 1024 * 1024) {
          reject(new Error('Request payload too large'));
        }
      });
      req.on('end', () => {
        if (!data.trim()) return resolve({});
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error('Invalid JSON payload'));
        }
      });
      req.on('error', reject);
    });
  }

  private sendJson(res: http.ServerResponse, statusCode: number, data: any): void {
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data, null, 2));
  }

  public async start(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(this.port, this.host, () => {
        resolve();
      });
    });
  }

  public async stop(): Promise<void> {
    if (this.wsServer) {
      await this.wsServer.close();
    }
    return new Promise((resolve) => {
      this.server.close(() => resolve());
    });
  }

  public getAddress(): { port: number; host: string } {
    return { port: this.port, host: this.host };
  }
}
