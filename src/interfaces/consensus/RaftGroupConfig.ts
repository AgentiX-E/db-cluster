import { ConsensusRole } from '../types';

/**
 * Configuration for a single Raft consensus group.
 */
export interface RaftGroupConfig {
  /** Unique identifier for this Raft group */
  groupId: string;

  /** Logical shard (or partition) this group covers */
  shardId: string;

  /** Node IDs that are members of this group */
  members: string[];

  /** Preferred leader node ID (may be empty for auto-election) */
  preferredLeader?: string;

  /** Election timeout range in milliseconds (min, max) */
  electionTimeoutMs: [number, number];

  /** Heartbeat interval in milliseconds */
  heartbeatIntervalMs: number;

  /** Maximum log entries per append-entries request */
  maxBatchSize: number;

  /** Snapshot threshold — entries to keep before compacting */
  snapshotThreshold: number;
}

/**
 * Runtime state of a Raft group node.
 */
export interface RaftGroupState {
  config: RaftGroupConfig;
  currentTerm: number;
  votedFor: string | null;
  role: ConsensusRole;
  commitIndex: number;
  lastApplied: number;
  leaderId: string | null;
}
