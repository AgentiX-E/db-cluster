/**
 * NoOpConsensusLayer — lightweight IConsensusLayer for single-node deployments.
 *
 * Provides the same IConsensusLayer contract as RaftGroupManager but with zero
 * consensus overhead. Every propose() commits immediately, isLeader() always
 * returns true, and no network I/O occurs.
 *
 * ## When to use
 *
 * - Single-node SqlVec deployments without replication
 * - Development / test environments where Raft overhead is unnecessary
 * - Benchmark baseline: "no consensus" vs "Raft consensus" comparison
 *
 * ## Usage
 *
 * ```typescript
 * const consensus: IConsensusLayer = clusterMode
 *   ? new RaftGroupManager(...)
 *   : new NoOpConsensusLayer();
 * ```
 *
 * @see IConsensusLayer — interface definition in interfaces.ts
 * @see RaftGroupManager — full Raft implementation
 */

import pino from 'pino';
const logger = pino({ level: process.env.LOG_LEVEL || 'info' });
import type { IConsensusLayer, TopologyChange } from '../consensus/interfaces/interfaces';

export class NoOpConsensusLayer implements IConsensusLayer {
  private readonly logger = pino({ level: process.env.LOG_LEVEL || 'info' });
  private readonly eventHandlers = new Map<string, Set<(...args: any[]) => void>>();
  private started = false;

  async start(): Promise<void> {
    this.started = true;
    this.logger.info('NoOpConsensusLayer started (single-node mode, no Raft)');
  }

  async stop(): Promise<void> {
    this.started = false;
    this.eventHandlers.clear();
    this.logger.info('NoOpConsensusLayer stopped');
  }

  /**
   * Propose a value — immediately returns index 0 (no consensus needed).
   */
  async propose(_shardId: string, _value: string): Promise<number> {
    return 0;
  }

  /**
   * Local node is always the leader in single-node mode.
   */
  async isLeader(_shardId: string): Promise<boolean> {
    return true;
  }

  /**
   * Local node is the only node.
   */
  async getLeader(_shardId: string): Promise<string | null> {
    return 'self';
  }

  /**
   * No committed index tracking — return 0.
   */
  async getCommitIndex(_shardId: string): Promise<number> {
    return 0;
  }

  registerShard(_shardId: string, _config?: any): void {
    // No-op: all shards are local
  }

  async removeShard(_shardId: string): Promise<void> {
    // No-op: nothing to remove
  }

  async notifyTopologyChange(_change: TopologyChange): Promise<void> {
    // No-op: single node topology never changes
  }

  async getHealthStatus(): Promise<any[]> {
    return [{ node: 'self', status: 'healthy' }];
  }

  async campaign(_shardId: string): Promise<void> {
    // No-op: no election needed
  }

  on(event: string, handler: (...args: any[]) => void): void {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, new Set());
    }
    this.eventHandlers.get(event)!.add(handler);
  }

  off(event: string, handler: (...args: any[]) => void): void {
    this.eventHandlers.get(event)?.delete(handler);
  }

  setForceLeader(_enabled: boolean): void {
    // No-op: always leader
  }

  // ── RaftGroupManager compatibility methods (single node, no Raft) ─────────

  /** No-op: single node has no outbox */
  setOutboxWriter(_writer: any): void {}

  /** No-op: single node does not need shard-level replication registration */
  setSharedTransport(_transport: any): void {}

  /** No-op: single node does not need transport messaging */
  registerShardWithReplica(
    _shardId: number,
    _replica: any,
    _peers: any[],
    _transportId: number,
    _tlsConfig?: any,
    _raftConfig?: any,
  ): void {
    // Delegate to registerShard (IConsensusLayer)
    this.registerShard(String(_shardId));
  }

  /** No-op: single node has no Raft messages */
  handleRaftMessage(_shardId: number, _msg: any): void {}

  /** No-op: single node does not need PG port mapping */
  setPgPorts(
    _localPgPort: number,
    _pgPortMap: Record<number, number>,
    _host: string,
    _autoForward?: boolean,
  ): void {}

  /** Single node is always the leader — return immediately */
  async waitForRaftLeaders(_timeoutMs: number): Promise<boolean> {
    return true;
  }

  /** No-op: single node cluster topology */
  setClusterTopology(_topology: any): void {}

  /** No-op: single node does not need to consume from event bus */
  applyFromEventBus(_shardId: number, _payload: any): void {}

  /** Return empty stats */
  getStats(): any {
    return {
      totalProposals: 0,
      activeGroups: 0,
    };
  }
}
