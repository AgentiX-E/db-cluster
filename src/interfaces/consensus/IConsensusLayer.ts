import { RaftGroupConfig, RaftGroupState } from './RaftGroupConfig';
import { LogEntry, MembershipChange, OperationResult, NodeHealth } from '../types';

/**
 * The consensus layer provides distributed consensus via the Raft protocol.
 * Each group of nodes runs an independent Raft instance.
 */
export interface IConsensusLayer {
  /**
   * Initialize a new Raft group or join an existing one.
   */
  initialize(config: RaftGroupConfig): Promise<OperationResult>;

  /**
   * Propose a new log entry to the cluster. Only succeeds on the leader.
   */
  propose(command: string, data: unknown): Promise<OperationResult<LogEntry>>;

  /**
   * Read committed log entries. Returns entries from `startIndex` onward (up to a limit).
   */
  read(startIndex: number, limit?: number): Promise<OperationResult<LogEntry[]>>;

  /**
   * Get the current state of this Raft group node.
   */
  getState(): Promise<OperationResult<RaftGroupState>>;

  /**
   * Add a new node to the Raft group (membership change).
   */
  addMember(nodeId: string, address: string): Promise<OperationResult<MembershipChange>>;

  /**
   * Remove a node from the Raft group (membership change).
   */
  removeMember(nodeId: string): Promise<OperationResult<MembershipChange>>;

  /**
   * Get health status for all nodes in the group.
   */
  getHealth(): Promise<OperationResult<NodeHealth[]>>;

  /**
   * Trigger a leader election.
   */
  triggerElection(): Promise<OperationResult>;

  /**
   * Gracefully shut down the Raft group node.
   */
  shutdown(): Promise<void>;

  /**
   * Event: fires when the leader changes.
   */
  onLeaderChange(callback: (leaderId: string, term: number) => void): void;

  /**
   * Event: fires when a log entry is committed.
   */
  onCommit(callback: (entry: LogEntry) => void): void;

  /**
   * Event: fires when membership changes.
   */
  onMembershipChange(callback: (change: MembershipChange) => void): void;
}
