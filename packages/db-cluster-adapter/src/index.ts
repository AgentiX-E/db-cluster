// ── Adapter Exports ──────────────────────────────────────────
//
// Each adapter wraps a db-cluster RaftGroup into a db-operations interface.
// Use these adapters to integrate Raft consensus monitoring into the
// unified operational observability framework.
//

export { DbClusterHealthAdapter } from './DbClusterHealthAdapter.js';
export { DbClusterMetricAdapter } from './DbClusterMetricAdapter.js';
export { DbClusterAuditAdapter } from './DbClusterAuditAdapter.js';
export { DbClusterDeadlockAdapter } from './DbClusterDeadlockAdapter.js';
export { DbClusterTracerAdapter } from './DbClusterTracerAdapter.js';
export type { RaftElectionDeadlockEvent } from './DbClusterDeadlockAdapter.js';

// ── Factory Utility ─────────────────────────────────────────

import { DbClusterHealthAdapter } from './DbClusterHealthAdapter.js';
import { DbClusterMetricAdapter } from './DbClusterMetricAdapter.js';
import { DbClusterAuditAdapter } from './DbClusterAuditAdapter.js';
import { DbClusterDeadlockAdapter } from './DbClusterDeadlockAdapter.js';
import { DbClusterTracerAdapter } from './DbClusterTracerAdapter.js';
import type { RaftGroup } from '@agentix-e/db-cluster';

/**
 * Create a complete suite of adapters for a RaftGroup.
 *
 * All adapters share the same RaftGroup instance, so their state
 * is consistent.
 *
 * @example
 * ```typescript
 * const group = new RaftGroup(shardId, raftConfig, replica, outboxManager);
 * await group.init();
 *
 * const adapters = createAdapters(group);
 * const health = adapters.health.liveness();
 * const metrics = adapters.metrics.snapshot();
 * const audit = await adapters.audit.recordCommittedEntry(logEntry);
 * ```
 */
export function createAdapters(group: RaftGroup): {
  health: DbClusterHealthAdapter;
  metrics: DbClusterMetricAdapter;
  audit: DbClusterAuditAdapter;
  deadlock: DbClusterDeadlockAdapter;
  tracer: DbClusterTracerAdapter;
} {
  return {
    health: new DbClusterHealthAdapter(group),
    metrics: new DbClusterMetricAdapter(group),
    audit: new DbClusterAuditAdapter(group),
    deadlock: new DbClusterDeadlockAdapter(group),
    tracer: new DbClusterTracerAdapter(group),
  };
}
