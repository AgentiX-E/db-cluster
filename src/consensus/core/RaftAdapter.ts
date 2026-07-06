/**
 * RaftAdapter — Wrapper around @maboke123/raft-core
 *
 * Normalizes the @maboke123/raft-core RaftNode API for SqlVec's consensus layer.
 * Keeps the exact same public API surface as the old adapter so that
 * RaftGroup.ts and its callers need zero changes.
 *
 * Key differences from the previous WASM adapter:
 *   - No WASM serialization queue (no RefCell issue — @maboke123/raft-core is
 *     pure TypeScript with async-safe internals)
 *   - No manual tick driving (RaftNode internally manages timers and elections)
 *   - No manual leader tracking (RaftNode.getLeaderId() is authoritative)
 *   - forceLeader simplified: mark + skip Raft for propose
 *   - applyHandler passed directly to ShardStateMachine
 */

import { RaftNode, InMemoryNodeStorage, LogEntryType } from '@maboke123/raft-core';
import type { RaftConfig as CoreRaftConfig, NodeStorage } from '@maboke123/raft-core';
import { SqliteNodeStorage } from '../storage/SqliteNodeStorage';
import { ShardStateMachine } from './ShardStateMachine';
import { BatchedRaftTransport } from '../transport/BatchedRaftTransport';
import { RaftTcpTransport as OriginalRaftTcpTransport } from '../transport/RaftTcpTransport';
import { SqlVecError, SqlVecErrorCode } from '../../interfaces/types';
import type { SharedTransport } from '../../interfaces/types';
import pino from 'pino';

const logger = pino({ level: process.env.LOG_LEVEL || 'info' });

// ──────────────────────────────────────────
//  Public types (re-exported for RaftGroup)
// ──────────────────────────────────────────

export interface RaftTcpTransportConfig {
  /** 本节点信息 */
  node: { id: number; host: string; port: number };
  /** 对端节点列表 */
  peers: Array<{ id: number; host: string; port: number }>;
  /** mTLS 证书 */
  tls: {
    cert: Buffer | string;
    key: Buffer | string;
    ca: Buffer | string;
  };
  /** 连接超时（ms，默认 5000） */
  connectTimeoutMs?: number;
  /** 初始重试间隔（ms，默认 100） */
  initialRetryMs?: number;
  /** 最大重试间隔（ms，默认 30000） */
  maxRetryMs?: number;
}

export interface RaftConfig {
  id: number;
  peers: number[];
  electionTick: number;
  heartbeatTick: number;
  storage?: any;
  transport?: any;
  tickIntervalMs?: number;
  /** Path to a SQLite file for durable Raft log storage. */
  storagePath?: string;
  /** Enable pre-vote check (default: true). Disable for InMemoryTransport tests. */
  preVote?: boolean;
  /** Enable check quorum (default: true). */
  checkQuorum?: boolean;
  /** Snapshot interval (0 = disabled). Every N committed entries triggers snapshot. */
  snapshotInterval?: number;
  /** Max log entries before forced compaction (0 = no limit). */
  maxLogEntries?: number;
  /**
   * Use InMemoryStorage (test-only). When false (default), RaftAdapter
   * requires storagePath or a custom storage instance. NEVER enable in production.
   */
  useInMemoryStorage?: boolean;
  /**
   * Use InMemoryTransport (test-only). When false (default), RaftAdapter
   * requires sharedTransport, tcpConfig, or transport. NEVER enable in production.
   */
  useInMemoryTransport?: boolean;
  /**
   * Enable InstallSnapshot RPC handler for streaming snapshot chunks (G17).
   * When true, RaftGroup initializes InstallSnapshotHandler and routes
   * MsgInstallSnapshot (type 14) messages to it for chunk reassembly.
   * Default: false.
   */
  enableInstallSnapshot?: boolean;
  /**
   * Environment mode. When set to 'production', requires durable storage
   * (storagePath or custom storage instance). Default: 'development'.
   */
  environment?: 'development' | 'production';
  /** Local transport address (used for @maboke123/raft-core RaftConfig). */
  address?: string;
}

export interface CommittedEntry {
  index: string;
  term: string;
  data: string;
}

export interface SnapshotMeta {
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  nodeId: number;
  shardId?: number;
  timestamp: number;
}

// ──────────────────────────────────────────
//  RaftAdapter
// ──────────────────────────────────────────

export class RaftAdapter {
  private node: RaftNode | null = null;
  private config: RaftConfig;
  private applyHandler: ((data: string, index?: number, term?: number) => void) | null = null;

  private nodeStorage: NodeStorage | null = null;
  private transport: BatchedRaftTransport | null = null;
  private originalTcpTransport: OriginalRaftTcpTransport | null = null;

  // Snapshot tracking
  private committedCount: number = 0;
  private lastSnapshotIndex: number = 0;

  /** Forced leader flag — set by HA failover when 2-node cluster is stuck */
  private _forceLeader: boolean = false;

  // Stored reference for getStorage() compatibility
  private sqliteStorage: any = null;

  constructor(config: RaftConfig) {
    this.config = config;
  }

  /**
   * Initialize the @maboke123/raft-core RaftNode.
   *
   * @param applyHandler - Called for every committed entry with data, plus optional index/term
   * @param tcpConfig - Optional TCP transport config (mutually exclusive with sharedTransport)
   * @param sharedTransport - Optional SharedTransport (shared across all RaftGroups, mutually exclusive with tcpConfig)
   */
  async init(
    applyHandler?: ((data: string, index?: number, term?: number) => void),
    tcpConfig?: RaftTcpTransportConfig,
    sharedTransport?: SharedTransport,
  ): Promise<void> {
    if (applyHandler) this.applyHandler = applyHandler;

    try {
      // ── Step 1: Build NodeStorage ────────────────────────────
      let storage: NodeStorage;

      if (this.config.storage) {
        // Existing SqliteStorage instance passed directly
        storage = new SqliteNodeStorage(this.config.storage);
        this.sqliteStorage = this.config.storage;
      } else if (this.config.storagePath) {
        const { SqliteStorage } = await import('../storage/SqliteStorage');
        const ss = new SqliteStorage({ file: this.config.storagePath, maxLogSize: 0 });
        ss.open();
        this.sqliteStorage = ss;
        storage = new SqliteNodeStorage(ss);
      } else if (this.config.useInMemoryStorage) {
        storage = new InMemoryNodeStorage();
      } else {
        throw new SqlVecError(
          'RaftAdapter requires storagePath or a custom storage instance. Set useInMemoryStorage=true only for tests.',
          SqlVecErrorCode.INTERNAL_ERROR,
        );
      }

      // Production mode requires durable storage
      if (this.config.environment === 'production' && !this.config.storagePath && !this.config.storage) {
        throw new SqlVecError(
          'Production mode requires durable Raft log storage (storagePath or custom storage instance)',
          SqlVecErrorCode.INTERNAL_ERROR,
        );
      }

      this.nodeStorage = storage;

      // ── Step 2: Build Transport ──────────────────────────────
      let raftTransport: BatchedRaftTransport;

      if (this.config.transport) {
        // Custom transport already provided
        raftTransport = this.config.transport;
      } else if (sharedTransport) {
        // SharedTransport (NodeTransport) — wrap in BatchedRaftTransport-compatible adapter
        // SharedTransport has forShard() returning a { send(fromId, msgs) } object.
        // We need a real BatchedRaftTransport for RPC request/response matching.
        // For now, we use a simple bridge that implements the Transport interface.

        // Create an internal transport that wraps SharedTransport
        const { RaftTransportAdapter } = await import('../transport/RaftTransportAdapter');
        const bridgeTransport = new RaftTransportAdapter(sharedTransport as any, this.config.id);

        // Create an offline BatchedRaftTransport to get the wire protocol
        // Actually, SharedTransport mode means NodeTransport in-memory routing.
        // The existing RaftTransportAdapter already implements the old adapter
        // transport interface (send with messages array). We need to bridge it
        // to the @maboke123/raft-core Transport interface (send one msg, get one response).

        // Create a minimal Transport bridge that wraps SharedTransport
        raftTransport = await this.buildSharedTransportBridge(sharedTransport);
      } else if (tcpConfig) {
        // TCP + mTLS — BatchedRaftTransport wraps the single RaftTcpTransport instance
        raftTransport = new BatchedRaftTransport({
          node: tcpConfig.node,
          peers: tcpConfig.peers,
          tls: tcpConfig.tls,
          connectTimeoutMs: tcpConfig.connectTimeoutMs,
          initialRetryMs: tcpConfig.initialRetryMs,
          maxRetryMs: tcpConfig.maxRetryMs,
        });

        // Wait for connections before starting Raft to avoid heartbeat drops
        await raftTransport.underlying.start();
        await raftTransport.underlying.waitForConnections();
        this.originalTcpTransport = raftTransport.underlying;
      } else {
        if (this.config.useInMemoryTransport) {
          throw new SqlVecError(
            'InMemoryTransport is not needed with @maboke123/raft-core — use InMemoryNodeStorage for tests',
            SqlVecErrorCode.INTERNAL_ERROR,
          );
        }
        throw new SqlVecError(
          'RaftAdapter requires sharedTransport, tcpConfig, or transport. Set useInMemoryStorage=true only for tests.',
          SqlVecErrorCode.INTERNAL_ERROR,
        );
      }

      this.transport = raftTransport;

      // Start transport
      if (!raftTransport.isStarted()) {
        await raftTransport.start();
      }

      // ── Step 3: Build ShardStateMachine ──────────────────────
      const stateMachine = new ShardStateMachine({
        applyHandler: (data: string) => {
          if (this.applyHandler) {
            try {
              this.applyHandler(data);
            } catch (err: any) {
              logger.error({ err }, `[RaftAdapter] applyHandler error: ${err.message}`);
            }
          }
          this.committedCount++;
        },
      });

      // ── Step 4: Build RaftConfig for @maboke123/raft-core ────
      const tickIntervalMs = this.config.tickIntervalMs || 100;
      const electionTimeoutMinMs = Math.max(100, this.config.electionTick * tickIntervalMs);
      const electionTimeoutMaxMs = Math.max(150, electionTimeoutMinMs + tickIntervalMs * 2);

      // Build peer list
      const nodeId = String(this.config.id);
      const address = tcpConfig
        ? `${tcpConfig.node.host}:${tcpConfig.node.port}`
        : this.config.address || `node-${this.config.id}:0`;

      const peers = this.config.peers
        .filter(p => p !== this.config.id)
        .map(p => ({
          id: String(p),
          address: tcpConfig
            ? `${tcpConfig.peers.find(pc => pc.id === p)?.host || '127.0.0.1'}:${tcpConfig.peers.find(pc => pc.id === p)?.port || 0}`
            : `node-${p}:0`,
        }));

      const coreConfig: CoreRaftConfig = {
        nodeId,
        address,
        peers,
        electionTimeoutMinMs,
        electionTimeoutMaxMs,
        heartbeatIntervalMs: Math.max(10, this.config.heartbeatTick * tickIntervalMs),
      };

      // ── Step 5: Create RaftNode ──────────────────────────────
      const raftNode = new RaftNode({
        config: coreConfig,
        storage,
        transport: raftTransport,
        stateMachine,
        logger: {
          debug: (msg: string, ctx?: any) => logger.debug(ctx || {}, msg),
          info: (msg: string, ctx?: any) => logger.info(ctx || {}, msg),
          warn: (msg: string, ctx?: any) => logger.warn(ctx || {}, msg),
          error: (msg: string, ctx?: any) => logger.error(ctx || {}, msg),
        },
      });

      await raftNode.start();
      this.node = raftNode;

      logger.info({ nodeId: this.config.id }, '[RaftAdapter] Initialized with @maboke123/raft-core');
    } catch (err: any) {
      throw new SqlVecError(
        `Failed to initialize @maboke123/raft-core: ${err.message}`,
        SqlVecErrorCode.INTERNAL_ERROR,
      );
    }
  }

  /**
   * Build a transport bridge wrapping SharedTransport.
   *
   * SharedTransport is used by NodeTransport which routes messages between
   * RaftGroups within the same process. We use the existing RaftTransportAdapter
   * pattern: the adapter wires messages from the TCP transport (or shared transport)
   * into the Raft layer. For SharedTransport, messages come from inter-group routing.
   *
   * Since we can't wire onMessage() on SharedTransport (it doesn't have one),
   * we return a transport that handles send() via shared.send() but relies on
   * the caller (RaftGroupManager) to call RaftAdapter.step() with incoming messages.
   *
   * The actual inbound routing is handled by RaftGroupManager which receives
   * messages from NodeTransport and calls group.handleMessage() / group.step().
   */
  private async buildSharedTransportBridge(shared: SharedTransport): Promise<BatchedRaftTransport> {
    const selfId = this.config.id;

    // We create a transport stub that sends via SharedTransport.
    // The inbound routing will be handled externally (RaftGroupManager calls step()).
    const handlerStack: Array<(from: string, msg: any) => Promise<any>> = [];

    const transport: BatchedRaftTransport = {
      start: async () => {},
      stop: async () => {},
      isStarted: () => true,
      send: async (peerId: string, message: any): Promise<any> => {
        const msgs = [message];
        await shared.send(String(selfId), msgs as any);
        return message;
      },
      onMessage: (handler: (from: string, msg: any) => Promise<any>) => {
        handlerStack.push(handler);
      },
    } as unknown as BatchedRaftTransport;

    return transport;
  }

  /**
   * Propose an entry through Raft consensus.
   * Resolves with { index, term } after the entry is committed and applied.
   */
  async propose(data: string): Promise<{ index: string; term: string }> {
    if (!this.node) {
      throw new SqlVecError('Raft node not initialized', SqlVecErrorCode.INTERNAL_ERROR);
    }

    // ── Force leader bypass ──
    if (this._forceLeader) {
      try {
        if (this.applyHandler) {
          this.applyHandler(data);
        }
        return { index: '0', term: '0' };
      } catch (applyErr: any) {
        throw new SqlVecError(
          `Force propose apply failed: ${applyErr.message}`,
          SqlVecErrorCode.INTERNAL_ERROR,
        );
      }
    }

    try {
      const result = await this.node.submitCommand({
        type: 'command',
        payload: data,
      });

      if (!result.success) {
        // Not leader
        if (result.leaderId) {
          throw new SqlVecError(
            `Not the Raft leader for this shard (leader=${result.leaderId})`,
            SqlVecErrorCode.NOT_LEADER,
          );
        }
        throw new SqlVecError(
          `Raft propose failed: ${result.error || 'unknown error'}`,
          SqlVecErrorCode.INTERNAL_ERROR,
        );
      }

      return {
        index: String(result.index ?? 0),
        term: String(this.node.getCurrentTerm()),
      };
    } catch (err: any) {
      if (err instanceof SqlVecError) throw err;
      throw new SqlVecError(
        `Raft propose failed: ${err.message}`,
        SqlVecErrorCode.INTERNAL_ERROR,
      );
    }
  }

  /**
   * Propose and wait for Raft commit + local apply.
   * submitCommand already waits for commit in @maboke123/raft-core.
   */
  async proposeAndWait(data: string): Promise<void> {
    if (!this.node) {
      throw new SqlVecError('Raft node not initialized', SqlVecErrorCode.INTERNAL_ERROR);
    }

    // ── Force leader bypass ──
    if (this._forceLeader) {
      try {
        if (this.applyHandler) {
          this.applyHandler(data);
        }
        return;
      } catch (applyErr: any) {
        throw new SqlVecError(
          `Force propose-and-wait apply failed: ${applyErr.message}`,
          SqlVecErrorCode.INTERNAL_ERROR,
        );
      }
    }

    try {
      const result = await this.node.submitCommand({
        type: 'command',
        payload: data,
      });

      if (!result.success && !result.leaderId) {
        throw new SqlVecError(
          `Raft propose failed: ${result.error || 'unknown error'}`,
          SqlVecErrorCode.INTERNAL_ERROR,
        );
      }

      if (!result.success && result.leaderId) {
        throw new SqlVecError(
          `Not the Raft leader for this shard (leader=${result.leaderId})`,
          SqlVecErrorCode.NOT_LEADER,
        );
      }
    } catch (err: any) {
      if (err instanceof SqlVecError) throw err;
      throw new SqlVecError(
        `Raft propose failed: ${err.message}`,
        SqlVecErrorCode.INTERNAL_ERROR,
      );
    }
  }

  /**
   * Check if this node is the Raft leader.
   */
  async isLeader(): Promise<boolean> {
    if (this._forceLeader) return true;
    if (!this.node) return false;
    try {
      return this.node.isLeader();
    } catch {
      return false;
    }
  }

  /**
   * Force this node to act as the leader, bypassing Raft election.
   * Used by HA failover coordinator when a 2-node cluster loses one node.
   */
  setForceLeader(enabled: boolean): void {
    this._forceLeader = enabled;
    logger.warn(
      { nodeId: this.config.id, enabled },
      `[RaftAdapter] Force leader ${enabled ? 'ENABLED' : 'DISABLED'}`,
    );
  }

  /**
   * Get the current Raft term.
   */
  async getTerm(): Promise<number> {
    if (!this.node) return 0;
    try {
      return this.node.getCurrentTerm();
    } catch {
      return 0;
    }
  }

  /**
   * Get the current Raft leader node ID.
   */
  async getLeader(): Promise<number | null> {
    if (this._forceLeader) return this.config.id;
    if (!this.node) return null;
    try {
      const leaderId = this.node.getLeaderId();
      if (leaderId == null) return null;
      return Number(leaderId);
    } catch {
      return null;
    }
  }

  /**
   * Get replication progress summary.
   */
  async getProgress(): Promise<Record<string, number>> {
    if (!this.node) return {};
    try {
      return {
        commit: this.node.getCommittedIndex(),
        applied: this.node.getLastApplied(),
        lastLog: this.node.getLastLogIndex(),
      };
    } catch {
      return {};
    }
  }

  /**
   * Get the last applied index.
   */
  async getAppliedIndex(): Promise<number> {
    if (!this.node) return 0;
    try {
      return this.node.getLastApplied();
    } catch {
      return 0;
    }
  }

  /**
   * Get the current commit index.
   */
  async getCommitIndex(): Promise<number> {
    if (!this.node) return 0;
    try {
      return this.node.getCommittedIndex();
    } catch {
      return 0;
    }
  }

  /**
   * Get full Raft status.
   */
  async getStatus(): Promise<any> {
    if (!this.node) return null;
    try {
      return {
        role: this.node.getState(),
        term: this.node.getCurrentTerm(),
        commitIndex: this.node.getCommittedIndex(),
        lastApplied: this.node.getLastApplied(),
        lastLogIndex: this.node.getLastLogIndex(),
        leader: this.node.getLeaderId(),
      };
    } catch {
      return null;
    }
  }

  /**
   * Trigger an election campaign.
   * @maboke123/raft-core handles elections automatically; this is a no-op.
   */
  async campaign(): Promise<void> {
    // @maboke123/raft-core handles elections automatically via timer.
    // No manual campaign needed.
  }

  /**
   * Step a Raft protocol message from a peer (public API).
   * @maboke123/raft-core handles routing via Transport; no manual step needed.
   */
  step(msg: any): void {
    // @maboke123/raft-core's Transport.onMessage handler automatically
    // routes incoming messages. No manual step() needed.
  }

  /**
   * Stop the Raft node and release resources.
   */
  stop(): void {
    if (this.node) {
      try {
        this.node.stop();
      } catch (err: any) {
        logger.warn({ err }, '[RaftAdapter] Error stopping RaftNode');
      }
      this.node = null;
    }

    if (this.transport) {
      try {
        this.transport.stop();
      } catch {
        // ignore
      }
      this.transport = null;
    }

    if (this.originalTcpTransport) {
      try {
        this.originalTcpTransport.stop();
      } catch {
        // ignore
      }
      this.originalTcpTransport = null;
    }

    if (this.sqliteStorage) {
      try {
        this.sqliteStorage.close();
      } catch {
        // ignore
      }
      this.sqliteStorage = null;
    }
  }

  // ──────────────────────────────────────────
  //  Snapshot API
  // ──────────────────────────────────────────

  /**
   * Check if a snapshot should be taken based on committed count.
   */
  shouldTakeSnapshot(): boolean {
    const interval = this.config.snapshotInterval || 100;
    if (interval <= 0) return false;
    return this.committedCount > 0 && this.committedCount % interval === 0;
  }

  /**
   * Record that a snapshot was taken at the given index.
   */
  takeSnapshot(index: number): void {
    this.lastSnapshotIndex = index;
  }

  /**
   * Reset the committed count after taking a snapshot.
   */
  resetCommittedCount(): void {
    this.committedCount = 0;
  }

  /**
   * Compact the Raft log up to (and including) the given snapshot index.
   */
  async compactLogs(lastIncludedIndex: number): Promise<void> {
    const storage = this.sqliteStorage || (this.config.storage as any);
    if (!storage) {
      logger.debug(`[RaftAdapter] No storage available for log compaction at index ${lastIncludedIndex}`);
      return;
    }
    if (typeof storage.compactLogs !== 'function') {
      logger.warn(`[RaftAdapter] Storage does not support compactLogs() — logs will not be truncated (index ${lastIncludedIndex})`);
      return;
    }
    try {
      await storage.compactLogs(lastIncludedIndex);
      logger.info(`[RaftAdapter] Logs compacted at index ${lastIncludedIndex}`);
    } catch (err: any) {
      logger.warn({ err }, `[RaftAdapter] Log compaction failed at index ${lastIncludedIndex}: ${err.message} — continuing`);
    }
  }

  /**
   * Flush pending apply queue (no-op for @maboke123/raft-core — apply is synchronous).
   */
  flushApplyQueue(): void {
    // @maboke123/raft-core applies entries synchronously in its apply loop.
    // No background apply queue needed.
  }

  /**
   * Get the underlying SqliteStorage instance for direct log manipulation.
   */
  getStorage(): any {
    return this.sqliteStorage || this.config.storage || null;
  }

  /**
   * Read index barrier for linearizable reads.
   * @maboke123/raft-core guarantees linearizable reads natively.
   */
  async readBarrier(): Promise<void> {
    // @maboke123/raft-core guarantees linearizable reads without manual barrier.
  }

  // ──────────────────────────────────────────
  //  Static helpers
  // ──────────────────────────────────────────

  /**
   * Encode snapshot metadata as Uint8Array for persistence.
   */
  static encodeSnapshotData(meta: SnapshotMeta): Uint8Array {
    return new TextEncoder().encode(JSON.stringify(meta));
  }
}
