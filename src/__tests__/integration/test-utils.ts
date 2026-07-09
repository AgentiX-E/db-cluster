/**
 * Shared utilities for multi-process Raft cluster integration tests.
 *
 * Each "Raft node" runs as a child_process that:
 * 1. Creates a TLS-secured RaftTcpTransport
 * 2. Creates a SqliteStorage for Raft log persistence
 * 3. Creates @maboke123/raft-core RaftNode(s)
 * 4. Listens on a random TCP port
 * 5. Communicates test results back via IPC message channel
 */

import { spawn, type ChildProcess } from 'node:child_process';
import path from 'node:path';
import net from 'node:net';
import fs from 'node:fs';
import os from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';

import type { CertBundle } from './test-certs';

// ──────────────────────────────────────────
//  Port allocation
// ──────────────────────────────────────────

export async function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      if (addr && typeof addr === 'object') {
        const port = addr.port;
        server.close(() => resolve(port));
      } else {
        server.close(() => reject(new Error('Failed to get port')));
      }
    });
    server.on('error', reject);
  });
}

export async function getFreePorts(count: number): Promise<number[]> {
  return Promise.all(Array.from({ length: count }, () => getFreePort()));
}

// ──────────────────────────────────────────
//  Temp directory management
// ──────────────────────────────────────────

export function createTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), `raft-test-${prefix}-`));
}

export function cleanupTempDir(dir: string): void {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {
    // ignore cleanup errors
  }
}

// ──────────────────────────────────────────
//  Child process management
// ──────────────────────────────────────────

export interface NodeConfig {
  id: number;
  host: string;
  port: number;
  dataDir: string;
  cert: CertBundle;
}

export interface ClusterConfig {
  nodes: NodeConfig[];
  electionTick: number;
  heartbeatTick: number;
}

export interface RaftNodeMessage {
  type: 'ready' | 'error' | 'leader-elected' | 'log-committed' | 'status' | 'exited';
  nodeId: number;
  data?: any;
  timestamp: number;
}

export class RaftTestNode {
  public readonly process: ChildProcess;
  public readonly config: NodeConfig;
  private _ready = false;
  private _messages: RaftNodeMessage[] = [];

  constructor(config: NodeConfig, scriptPath: string, env: Record<string, string>) {
    this.config = config;
    this.process = spawn('node', [scriptPath], {
      env: { ...process.env, ...env },
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    });

    this.process.on('message', (msg: any) => {
      if (msg && msg.type) {
        this._messages.push(msg as RaftNodeMessage);
        if (msg.type === 'ready') this._ready = true;
      }
    });

    this.process.stderr?.on('data', (d: Buffer) => {
      // Forward to parent stderr for debugging
      process.stderr.write(`[node-${config.id}] ${d}`);
    });
  }

  get ready(): boolean {
    return this._ready;
  }

  get messages(): RaftNodeMessage[] {
    return this._messages.slice();
  }

  clearMessages(): void {
    this._messages = [];
  }

  waitForMessage(type: string, timeoutMs: number = 30000): Promise<RaftNodeMessage> {
    return new Promise((resolve, reject) => {
      const existing = this._messages.find((m) => m.type === type);
      if (existing) return resolve(existing);

      const timer = setTimeout(() => {
        this.process.removeListener('message', handler);
        reject(new Error(`Timeout waiting for message type "${type}" from node ${this.config.id}`));
      }, timeoutMs);

      const handler = (msg: any) => {
        if (msg && msg.type === type) {
          clearTimeout(timer);
          resolve(msg as RaftNodeMessage);
        }
      };

      this.process.on('message', handler);
    });
  }

  send(msg: any): void {
    this.process.send?.(msg);
  }

  kill(signal: 'SIGTERM' | 'SIGKILL' = 'SIGTERM'): void {
    this.process.kill(signal);
  }

  async gracefulShutdown(timeoutMs: number = 5000): Promise<void> {
    this.send({ command: 'shutdown' });
    const timer = setTimeout(() => {
      this.kill('SIGKILL');
    }, timeoutMs);
    await new Promise<void>((resolve) => {
      this.process.on('exit', () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }
}

// ──────────────────────────────────────────
//  Cluster orchestration
// ──────────────────────────────────────────

export class TestRaftCluster {
  public nodes: RaftTestNode[] = [];
  private nodeConfigs: NodeConfig[] = [];
  private scriptPath: string;

  constructor(nodeCount: number) {
    this.scriptPath = path.resolve(__dirname, 'raft-node-worker.js');
  }

  async start(nodeCount: number, certs: CertBundle[]): Promise<void> {
    const ports = await getFreePorts(nodeCount);
    this.nodeConfigs = [];

    for (let i = 0; i < nodeCount; i++) {
      const dataDir = createTempDir(`node-${i + 1}`);
      this.nodeConfigs.push({
        id: i + 1,
        host: '127.0.0.1',
        port: ports[i],
        dataDir,
        cert: certs[i],
      });
    }

    // Serialize cluster config as JSON env var
    const clusterConfig: ClusterConfig = {
      nodes: this.nodeConfigs,
      electionTick: 10,
      heartbeatTick: 3,
    };

    // Start all nodes
    const startPromises = this.nodeConfigs.map((config) => {
      const node = new RaftTestNode(config, this.scriptPath, {
        RAFT_NODE_CONFIG: JSON.stringify(config),
        RAFT_CLUSTER_CONFIG: JSON.stringify(clusterConfig),
      });
      this.nodes.push(node);
      return node.waitForMessage('ready', 15000);
    });

    await Promise.all(startPromises);
  }

  async stop(): Promise<void> {
    await Promise.all(this.nodes.map((n) => n.gracefulShutdown()));
    this.nodeConfigs.forEach((c) => cleanupTempDir(c.dataDir));
  }

  async waitForLeader(timeoutMs: number = 30000): Promise<number> {
    const promises = this.nodes.map((n) => n.waitForMessage('leader-elected', timeoutMs));
    const result = await Promise.race(promises);
    return result.nodeId;
  }

  async propose(leaderId: number, data: string): Promise<void> {
    const leader = this.nodes.find((n) => n.config.id === leaderId);
    if (!leader) throw new Error(`Node ${leaderId} not found`);
    leader.send({ command: 'propose', data });
  }

  getAllLogs(): Promise<any[]> {
    return Promise.all(
      this.nodes.map((n) => {
        return new Promise((resolve) => {
          const handler = (msg: any) => {
            if (msg && msg.type === 'status') {
              n.process.removeListener('message', handler);
              resolve(msg.data);
            }
          };
          n.process.on('message', handler);
          n.send({ command: 'get-status' });
        });
      }),
    );
  }
}

// ──────────────────────────────────────────
//  Assertions
// ──────────────────────────────────────────

import { expect } from 'vitest';

export function assertLogsConsistent(logs: any[]): void {
  const firstLogs = logs[0];
  for (let i = 1; i < logs.length; i++) {
    expect(logs[i]).toEqual(firstLogs);
  }
}

export function assertExactlyOneLeader(statuses: any[]): void {
  const leaders = new Set(statuses.map((s: any) => s.leader).filter(Boolean));
  expect(leaders.size).toBe(1);
}
