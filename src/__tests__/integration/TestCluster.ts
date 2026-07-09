/**
 * TestCluster — In-process 3-node mTLS Raft cluster for integration & performance tests.
 *
 * Eliminates duplicated cluster setup in raft-stress, raft-chaos, raft-throughput-bench.
 *
 * Usage:
 *   const cluster = new TestCluster(nodeCount);
 *   await cluster.start();
 *   const leader = await cluster.findLeader();
 *   await cluster.stop();
 */

import { RaftAdapter } from '../../consensus/core/RaftAdapter';
import { generateClusterCerts, type TestClusterCerts } from './test-certs';
import { getFreePorts, createTempDir, cleanupTempDir } from './test-utils';
import { join } from 'node:path';
import * as fs from 'node:fs';

// ── Config ────────────────────────────────────────────────────────

export interface TestClusterOptions {
  nodeCount?: number; // default: 3
  electionTick?: number; // default: 15
  heartbeatTick?: number; // default: 3
  tickIntervalMs?: number; // default: 50
  leaderTimeoutMs?: number; // default: 30_000
  pollIntervalMs?: number; // default: 200
  snapshotInterval?: number; // default: 100_000
  preVote?: boolean; // default: true
  checkQuorum?: boolean; // default: true
  enableInstallSnapshot?: boolean; // default: false
}

// ── Public types ──────────────────────────────────────────────────

export interface TestNode {
  adapter: RaftAdapter;
  port: number;
  id: number;
  dir: string;
}

// ── Defaults ──────────────────────────────────────────────────────

const DEFAULTS: Required<TestClusterOptions> = {
  nodeCount: 3,
  electionTick: 15,
  heartbeatTick: 3,
  tickIntervalMs: 50,
  leaderTimeoutMs: 30_000,
  pollIntervalMs: 200,
  snapshotInterval: 100_000,
  preVote: true,
  checkQuorum: true,
  enableInstallSnapshot: false,
};

// ── Cluster class ─────────────────────────────────────────────────

export class TestCluster {
  private opts: Required<TestClusterOptions>;
  private certs!: TestClusterCerts;
  private ports: number[] = [];
  private baseDir: string = '';
  private dataDirs: string[] = [];

  /** Currently active nodes. Empty until start() resolves. */
  public nodes: TestNode[] = [];
  /** Peer configurations (all nodes, for init()). */
  public peerConfigs: { id: number; host: string; port: number }[] = [];

  private _started = false;

  constructor(opts: TestClusterOptions = {}) {
    this.opts = { ...DEFAULTS, ...opts };
  }

  // ── Lifecycle ──────────────────────────────────────────────

  /**
   * Start the cluster: allocate ports, generate certs, create + init
   * all RaftAdapters. Resolves once all nodes are initialized.
   */
  async start(): Promise<void> {
    await this.ensureClean();

    this.certs = generateClusterCerts(this.opts.nodeCount);
    this.ports = await getFreePorts(this.opts.nodeCount);
    this.baseDir = createTempDir('test-cluster');
    this.dataDirs = Array.from({ length: this.opts.nodeCount }, (_, i) => {
      const d = join(this.baseDir, `node-${i + 1}`);
      if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
      return d;
    });
    this.peerConfigs = Array.from({ length: this.opts.nodeCount }, (_, i) => ({
      id: i + 1,
      host: '127.0.0.1',
      port: this.ports[i],
    }));

    const instances: TestNode[] = [];
    for (let i = 0; i < this.opts.nodeCount; i++) {
      const id = i + 1;
      const nodePeers = this.peerConfigs.filter((p) => p.id !== id);
      const adapter = new RaftAdapter({
        id,
        peers: nodePeers.map((p) => p.id),
        electionTick: this.opts.electionTick,
        heartbeatTick: this.opts.heartbeatTick,
        tickIntervalMs: this.opts.tickIntervalMs,
        storagePath: join(this.dataDirs[i], 'raft.db'),
        snapshotInterval: this.opts.snapshotInterval,
        environment: 'development',
        preVote: this.opts.preVote,
        checkQuorum: this.opts.checkQuorum,
        enableInstallSnapshot: this.opts.enableInstallSnapshot,
      });
      instances.push({ adapter, port: this.ports[i], id, dir: this.dataDirs[i] });
    }

    for (const inst of instances) {
      const nodePeers = this.peerConfigs.filter((p) => p.id !== inst.id);
      await inst.adapter.init(() => {}, {
        node: { id: inst.id, host: '127.0.0.1', port: inst.port },
        peers: nodePeers,
        tls: {
          cert: this.certs.nodes[inst.id - 1].cert,
          key: this.certs.nodes[inst.id - 1].key,
          ca: this.certs.caPem,
        },
        connectTimeoutMs: 5000,
        initialRetryMs: 100,
        maxRetryMs: 5000,
      });
    }

    this.nodes = instances;
    this._started = true;
  }

  /**
   * Stop all nodes and clean up temp dirs.
   */
  async stop(): Promise<void> {
    for (const n of this.nodes) {
      try {
        n.adapter.stop();
      } catch {
        /* ignore */
      }
    }
    this.nodes = [];
    for (const dir of this.dataDirs) {
      try {
        cleanupTempDir(dir);
      } catch {
        /* ignore */
      }
    }
    this.dataDirs = [];
    this.ports = [];
    this._started = false;
  }

  // ── Leader queries ─────────────────────────────────────────

  /**
   * Wait for a leader to be elected. Throws if timeout reached.
   */
  async findLeader(): Promise<TestNode> {
    this.requireStarted();
    const deadline = Date.now() + this.opts.leaderTimeoutMs;
    while (Date.now() < deadline) {
      for (const n of this.nodes) {
        if (await n.adapter.isLeader()) return n;
      }
      await this.sleep(this.opts.pollIntervalMs);
    }
    throw new Error(`No leader elected within ${this.opts.leaderTimeoutMs}ms`);
  }

  /**
   * Wait for a new leader to emerge, excluding the given node id.
   */
  async waitForNewLeader(excludeId: number): Promise<TestNode> {
    this.requireStarted();
    const deadline = Date.now() + this.opts.leaderTimeoutMs;
    while (Date.now() < deadline) {
      for (const n of this.nodes) {
        if (n.id === excludeId) continue;
        if (await n.adapter.isLeader()) return n;
      }
      await this.sleep(this.opts.pollIntervalMs);
    }
    throw new Error(`No new leader after excluding ${excludeId}`);
  }

  /**
   * Kill a specific node (stop adapter + remove from active list).
   */
  async killNode(id: number): Promise<void> {
    this.requireStarted();
    const node = this.nodes.find((n) => n.id === id);
    if (!node) throw new Error(`Node ${id} not found`);
    node.adapter.stop();
    this.nodes = this.nodes.filter((n) => n.id !== id);
    await this.sleep(200);
  }

  /**
   * Return remaining nodes (excluding a given id).
   */
  remainingNodes(excludeId: number): TestNode[] {
    return this.nodes.filter((n) => n.id !== excludeId);
  }

  // ── Helpers ────────────────────────────────────────────────

  private async ensureClean(): Promise<void> {
    await this.stop();
    await this.sleep(500);
  }

  private requireStarted(): void {
    if (!this._started) {
      throw new Error('TestCluster not started — call start() first');
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}
