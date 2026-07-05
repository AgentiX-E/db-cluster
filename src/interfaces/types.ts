/**
 * Shared type definitions for @agentix-e/db-cluster
 */

/** Roles a node can assume in a Raft consensus group */
export enum ConsensusRole {
  Follower = 'follower',
  Candidate = 'candidate',
  Leader = 'leader',
}

/** Describes a change to the cluster membership */
export interface MembershipChange {
  type: 'join' | 'leave' | 'swap';
  nodeId: string;
  address?: string;
  raftGroupId?: string;
  timestamp: number;
}

/** A Raft log entry */
export interface LogEntry {
  index: number;
  term: number;
  command: string;
  data: unknown;
  timestamp: number;
}

/** Internal Raft protocol message */
export interface RaftMessage {
  type: 'request_vote' | 'request_vote_response' | 'append_entries' | 'append_entries_response' | 'install_snapshot';
  term: number;
  from: string;
  to: string;
  payload: unknown;
}

/** Snapshot metadata */
export interface SnapshotMeta {
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  groupId: string;
}

/** Node health status */
export interface NodeHealth {
  nodeId: string;
  isAlive: boolean;
  lastHeartbeat: number;
  term: number;
  role: ConsensusRole;
}

/** Generic result wrapper for operations */
export interface OperationResult<T = void> {
  success: boolean;
  error?: string;
  data?: T;
}
