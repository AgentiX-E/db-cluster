/**
 * ShardManager — distributed shard-to-node mapping manager.
 *
 * Manages shard assignment, rebalancing, and distribution tracking
 * across cluster nodes using pluggable IShardStrategy implementations.
 *
 * Inspired by HashQL's ShardManager and adapted from sqlvec-db's ShardManager
 * for cluster-level shard orchestration.
 */

import * as fs from 'fs';
import * as path from 'path';
import pino from 'pino';
import { IShardManager } from '../interfaces/shard/IShardManager';
import { IShardStrategy } from '../interfaces/shard/IShardStrategy';
import { OperationResult } from '../interfaces/types';

const logger = pino({ name: 'ShardManager' });

/**
 * Default replication factor for shards across nodes.
 */
const DEFAULT_REPLICATION_FACTOR = 3;

/**
 * Default number of shards in a new cluster.
 */
const DEFAULT_SHARD_COUNT = 256;

interface ShardAssignment {
  shardId: string;
  nodeIds: string[];
}

export class ShardManager implements IShardManager {
  private strategy: IShardStrategy | null = null;
  private assignments: Map<string, ShardAssignment> = new Map();
  private shardAssignedCallbacks: Array<(shardId: string, nodeIds: string[]) => void> = [];
  private shardRemovedCallbacks: Array<(shardId: string) => void> = [];
  private replicationFactor: number;
  private storageDir: string;

  constructor(storageDir?: string, replicationFactor?: number) {
    this.storageDir = storageDir || path.join(process.cwd(), '.shard-data');
    this.replicationFactor = replicationFactor ?? DEFAULT_REPLICATION_FACTOR;
    this.ensureDir(this.storageDir);
  }

  // ──────────────────────────────────────────────
  //  IShardManager implementation
  // ──────────────────────────────────────────────

  async initialize(strategy: IShardStrategy): Promise<OperationResult> {
    if (this.strategy) {
      return { success: false, error: 'ShardManager already initialized' };
    }
    this.strategy = strategy;

    // Load any persisted assignments from disk
    await this.loadAssignments();

    logger.info({ strategy: strategy.name, shardCount: this.assignments.size }, 'ShardManager initialized');
    return { success: true };
  }

  getStrategy(): IShardStrategy {
    if (!this.strategy) {
      throw new Error('ShardManager not yet initialized — call initialize(strategy) first');
    }
    return this.strategy;
  }

  async listShards(): Promise<OperationResult<string[]>> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }
    return { success: true, data: Array.from(this.assignments.keys()) };
  }

  async getShardNodes(shardId: string): Promise<OperationResult<string[]>> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }
    const assignment = this.assignments.get(shardId);
    if (!assignment) {
      return { success: false, error: `Shard ${shardId} not found` };
    }
    return { success: true, data: [...assignment.nodeIds] };
  }

  async getShardForKey(key: string): Promise<OperationResult<string>> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }
    const totalShards = this.assignments.size;
    if (totalShards === 0) {
      return { success: false, error: 'No shards configured' };
    }
    const result = await this.strategy.getShardForKey(key, totalShards);
    return result;
  }

  async assignShard(shardId: string, nodeIds: string[]): Promise<OperationResult> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }

    const existing = this.assignments.get(shardId);
    this.assignments.set(shardId, { shardId, nodeIds: [...new Set(nodeIds)] });

    // Persist to disk
    await this.persistAssignments();

    // Fire event
    this.fireShardAssigned(shardId, nodeIds);

    logger.info({ shardId, nodeIds, wasUpdate: !!existing }, 'Shard assigned');
    return { success: true };
  }

  async removeShard(shardId: string): Promise<OperationResult> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }

    const removed = this.assignments.delete(shardId);

    if (removed) {
      await this.persistAssignments();
      this.fireShardRemoved(shardId);
      logger.info({ shardId }, 'Shard removed');
    }

    return { success: true };
  }

  async rebalance(nodeIds: string[]): Promise<OperationResult> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }

    const shardIds = Array.from(this.assignments.keys());
    const currentDistribution = await this.getDistributionMap();

    const result = await this.strategy.computeAssignments(
      shardIds,
      nodeIds,
      currentDistribution,
      this.replicationFactor,
    );

    if (!result.success || !result.data) {
      return { success: false, error: result.error || 'Rebalance computation failed' };
    }

    // Apply the new distribution
    for (const [shardId, newNodeIds] of Object.entries(result.data)) {
      this.assignments.set(shardId, { shardId, nodeIds: newNodeIds });
      this.fireShardAssigned(shardId, newNodeIds);
    }

    await this.persistAssignments();

    logger.info({ shardCount: shardIds.length, nodeCount: nodeIds.length }, 'Shards rebalanced');
    return { success: true };
  }

  async addNode(nodeId: string): Promise<OperationResult> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }

    const currentDistribution = await this.getDistributionMap();
    const result = await this.strategy.onNodeAdded(
      nodeId,
      currentDistribution,
      this.replicationFactor,
    );

    if (!result.success || !result.data) {
      return { success: false, error: result.error || 'Node add rebalance computation failed' };
    }

    // Apply the new distribution
    for (const [shardId, newNodeIds] of Object.entries(result.data)) {
      const existing = this.assignments.get(shardId);
      if (existing) {
        existing.nodeIds = newNodeIds;
        this.fireShardAssigned(shardId, newNodeIds);
      } else {
        this.assignments.set(shardId, { shardId, nodeIds: newNodeIds });
        this.fireShardAssigned(shardId, newNodeIds);
      }
    }

    await this.persistAssignments();

    logger.info({ nodeId }, 'Node added and shards redistributed');
    return { success: true };
  }

  async removeNode(nodeId: string): Promise<OperationResult> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }

    const currentDistribution = await this.getDistributionMap();
    const result = await this.strategy.onNodeRemoved(
      nodeId,
      currentDistribution,
      this.replicationFactor,
    );

    if (!result.success || !result.data) {
      return { success: false, error: result.error || 'Node removal rebalance computation failed' };
    }

    // Remove the node from any existing assignments, then apply new distribution
    for (const [, assignment] of this.assignments) {
      assignment.nodeIds = assignment.nodeIds.filter(n => n !== nodeId);
    }

    for (const [shardId, newNodeIds] of Object.entries(result.data)) {
      const existing = this.assignments.get(shardId);
      if (existing) {
        existing.nodeIds = newNodeIds;
        this.fireShardAssigned(shardId, newNodeIds);
      } else {
        this.assignments.set(shardId, { shardId, nodeIds: newNodeIds });
        this.fireShardAssigned(shardId, newNodeIds);
      }
    }

    // Clean up any shards that have no nodes
    const emptyShards: string[] = [];
    for (const [shardId, assignment] of this.assignments) {
      if (assignment.nodeIds.length === 0) {
        emptyShards.push(shardId);
      }
    }
    for (const shardId of emptyShards) {
      this.assignments.delete(shardId);
      this.fireShardRemoved(shardId);
    }

    await this.persistAssignments();

    logger.info({ nodeId, emptyShardsRemoved: emptyShards.length }, 'Node removed and shards redistributed');
    return { success: true };
  }

  async getDistribution(): Promise<OperationResult<Record<string, string[]>>> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }

    const distribution: Record<string, string[]> = {};
    for (const [shardId, assignment] of this.assignments) {
      distribution[shardId] = [...assignment.nodeIds];
    }

    return { success: true, data: distribution };
  }

  onShardAssigned(callback: (shardId: string, nodeIds: string[]) => void): void {
    this.shardAssignedCallbacks.push(callback);
  }

  onShardRemoved(callback: (shardId: string) => void): void {
    this.shardRemovedCallbacks.push(callback);
  }

  // ──────────────────────────────────────────────
  //  Persistence helpers
  // ──────────────────────────────────────────────

  /**
   * Path to the assignments persistence file.
   */
  private getAssignmentsFilePath(): string {
    return path.join(this.storageDir, 'shard-assignments.json');
  }

  /**
   * Persist the current shard assignments to disk as JSON.
   */
  private async persistAssignments(): Promise<void> {
    const filePath = this.getAssignmentsFilePath();
    const data: Record<string, string[]> = {};
    for (const [shardId, assignment] of this.assignments) {
      data[shardId] = assignment.nodeIds;
    }
    try {
      const tmpPath = filePath + '.tmp';
      fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2), 'utf-8');
      fs.renameSync(tmpPath, filePath);
    } catch (err: any) {
      logger.error({ err: err.message }, 'Failed to persist shard assignments');
    }
  }

  /**
   * Load shard assignments from disk, if they exist.
   */
  private async loadAssignments(): Promise<void> {
    const filePath = this.getAssignmentsFilePath();
    if (!fs.existsSync(filePath)) {
      return;
    }
    try {
      const raw = fs.readFileSync(filePath, 'utf-8');
      const data = JSON.parse(raw) as Record<string, string[]>;
      for (const [shardId, nodeIds] of Object.entries(data)) {
        this.assignments.set(shardId, { shardId, nodeIds });
      }
      logger.info({ shardCount: Object.keys(data).length }, 'Loaded shard assignments from disk');
    } catch (err: any) {
      logger.warn({ err: err.message }, 'Failed to load shard assignments from disk');
    }
  }

  /**
   * Bootstrap a set of shards with an initial distribution across nodes.
   * Useful when creating a new cluster from scratch.
   */
  async bootstrap(shardCount: number, nodeIds: string[]): Promise<OperationResult> {
    if (!this.strategy) {
      return { success: false, error: 'ShardManager not initialized' };
    }

    const shardIds: string[] = [];
    for (let i = 0; i < shardCount; i++) {
      shardIds.push(`shard-${i}`);
    }

    const result = await this.strategy.computeAssignments(
      shardIds,
      nodeIds,
      {},
      this.replicationFactor,
    );

    if (!result.success || !result.data) {
      return { success: false, error: result.error || 'Bootstrap computation failed' };
    }

    for (const [shardId, newNodeIds] of Object.entries(result.data)) {
      this.assignments.set(shardId, { shardId, nodeIds: newNodeIds });
      this.fireShardAssigned(shardId, newNodeIds);
    }

    await this.persistAssignments();

    logger.info({ shardCount, nodeCount: nodeIds.length }, 'Shard bootstrap complete');
    return { success: true };
  }

  // ──────────────────────────────────────────────
  //  Helpers
  // ──────────────────────────────────────────────

  /**
   * Get the current distribution as a plain Record for strategy input.
   */
  private async getDistributionMap(): Promise<Record<string, string[]>> {
    const distribution: Record<string, string[]> = {};
    for (const [shardId, assignment] of this.assignments) {
      distribution[shardId] = [...assignment.nodeIds];
    }
    return distribution;
  }

  /**
   * Fire shard assigned event to all registered callbacks.
   */
  private fireShardAssigned(shardId: string, nodeIds: string[]): void {
    for (const cb of this.shardAssignedCallbacks) {
      try {
        cb(shardId, [...nodeIds]);
      } catch (err: any) {
        logger.warn({ err: err.message, shardId }, 'Shard assigned callback threw');
      }
    }
  }

  /**
   * Fire shard removed event to all registered callbacks.
   */
  private fireShardRemoved(shardId: string): void {
    for (const cb of this.shardRemovedCallbacks) {
      try {
        cb(shardId);
      } catch (err: any) {
        logger.warn({ err: err.message, shardId }, 'Shard removed callback threw');
      }
    }
  }

  private ensureDir(dir: string): void {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  }
}
