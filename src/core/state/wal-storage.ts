/**
 * EventFlow - Write-Ahead Log (WAL) Storage Engine
 * High-durability append-only disk logging with periodic snapshots for crash recovery.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { WorkflowEvent, WorkflowInstance } from '../types.js';
import { EventStore } from './event-store.js';

export interface WALStorageOptions {
  storageDir?: string;
  walFileName?: string;
  snapshotFileName?: string;
  autoSync?: boolean;
}

export class WALStorage {
  private readonly storageDir: string;
  private readonly walPath: string;
  private readonly snapshotPath: string;
  private walWriteStream?: fs.WriteStream;
  private isInitialized = false;

  constructor(options: WALStorageOptions = {}) {
    this.storageDir = path.resolve(process.cwd(), options.storageDir ?? '.eventflow-data');
    this.walPath = path.join(this.storageDir, options.walFileName ?? 'events.wal');
    this.snapshotPath = path.join(this.storageDir, options.snapshotFileName ?? 'snapshots.json');
  }

  public async init(): Promise<void> {
    if (this.isInitialized) return;

    if (!fs.existsSync(this.storageDir)) {
      fs.mkdirSync(this.storageDir, { recursive: true });
    }

    this.walWriteStream = fs.createWriteStream(this.walPath, {
      flags: 'a',
      encoding: 'utf8',
    });

    this.isInitialized = true;
  }

  public async appendEvent(event: WorkflowEvent): Promise<void> {
    if (!this.isInitialized) {
      await this.init();
    }

    const line = JSON.stringify(event) + '\n';
    return new Promise((resolve, reject) => {
      this.walWriteStream?.write(line, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  public async saveSnapshot(instances: Record<string, WorkflowInstance>): Promise<void> {
    if (!this.isInitialized) {
      await this.init();
    }

    const tmpSnapshotPath = `${this.snapshotPath}.tmp`;
    const data = JSON.stringify(instances, null, 2);
    await fs.promises.writeFile(tmpSnapshotPath, data, 'utf8');
    await fs.promises.rename(tmpSnapshotPath, this.snapshotPath);
  }

  public async recover(eventStore: EventStore): Promise<{
    recoveredInstances: Record<string, WorkflowInstance>;
    totalEventsRecovered: number;
  }> {
    await this.init();

    const instances: Record<string, WorkflowInstance> = {};
    let totalEventsRecovered = 0;

    // 1. Load snapshot if exists
    if (fs.existsSync(this.snapshotPath)) {
      try {
        const raw = await fs.promises.readFile(this.snapshotPath, 'utf8');
        const parsed = JSON.parse(raw);
        Object.assign(instances, parsed);
      } catch (err) {
        console.warn('[WALStorage] Could not parse snapshot file, falling back to full WAL replay:', err);
      }
    }

    // 2. Replay events from WAL
    if (fs.existsSync(this.walPath)) {
      const content = await fs.promises.readFile(this.walPath, 'utf8');
      const lines = content.split('\n').filter((l) => l.trim().length > 0);

      const eventsByWorkflow = new Map<string, WorkflowEvent[]>();

      for (const line of lines) {
        try {
          const event: WorkflowEvent = JSON.parse(line);
          totalEventsRecovered++;

          // Feed into active event store
          eventStore.append(
            event.workflowInstanceId,
            event.type,
            event.payload,
            event.stepId
          );

          if (!eventsByWorkflow.has(event.workflowInstanceId)) {
            eventsByWorkflow.set(event.workflowInstanceId, []);
          }
          eventsByWorkflow.get(event.workflowInstanceId)!.push(event);
        } catch (parseErr) {
          console.warn('[WALStorage] Skipping corrupted line in WAL:', parseErr);
        }
      }

      // Reconstruct state for all workflows in WAL
      for (const [wId, events] of eventsByWorkflow.entries()) {
        const replayed = EventStore.replayWorkflow(events);
        if (replayed) {
          // If a workflow was left in RUNNING when process died, mark it FAILED/RECOVERED
          if (replayed.status === 'RUNNING') {
            replayed.status = 'FAILED';
            replayed.error = 'Workflow interrupted by unexpected process termination';
            replayed.endTime = Date.now();
            replayed.durationMs = replayed.endTime - replayed.startTime;
          }
          instances[wId] = replayed;
        }
      }
    }

    return {
      recoveredInstances: instances,
      totalEventsRecovered,
    };
  }

  public async close(): Promise<void> {
    if (this.walWriteStream) {
      await new Promise<void>((resolve) => {
        this.walWriteStream?.end(() => resolve());
      });
      this.isInitialized = false;
    }
  }
}
