// Consensus
export { IConsensusLayer } from './interfaces/consensus/IConsensusLayer';
export { RaftGroupConfig, RaftGroupState } from './interfaces/consensus/RaftGroupConfig';

// Write-Ahead Log
export { IWalStorage } from './interfaces/wal/IWalStorage';

// Sharding
export { IShardManager } from './interfaces/shard/IShardManager';
export { IShardStrategy } from './interfaces/shard/IShardStrategy';

// High Availability
export { IFailoverDetector, FailoverDetectorConfig } from './interfaces/ha/IFailoverDetector';
export { IFailoverCoordinator, FailoverAction, FailoverState, FailoverCoordinatorConfig } from './interfaces/ha/IFailoverCoordinator';

// Transport
export { ITransportReceiver } from './interfaces/transport/ITransportReceiver';

// Shared Types
export {
  ConsensusRole,
  MembershipChange,
  LogEntry,
  RaftMessage,
  SnapshotMeta,
  NodeHealth,
  OperationResult,
} from './interfaces/types';
