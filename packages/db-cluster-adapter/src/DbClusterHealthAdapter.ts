import type { IHealthProbe } from '@agentix-e/db-operations-health';
import type { ProbeResult, ComponentHealth } from '@agentix-e/db-operations-core/types';
import type { RaftGroup } from '@agentix-e/db-cluster';
import { ConsensusRole } from '@agentix-e/db-cluster';

const PROBE_ID = 'db-cluster.raft-group';
const COMPONENT_NAME = 'RaftGroup';

/**
 * DbClusterHealthAdapter — Adapts a RaftGroup's consensus state into
 * db-operations IHealthProbe interface.
 *
 * Maps Raft concepts:
 *   leader/follower  → healthy
 *   candidate        → degraded (liveness passes, readiness fails)
 *   no quorum        → unhealthy
 */
export class DbClusterHealthAdapter implements IHealthProbe {
  readonly probeId: string = PROBE_ID;
  readonly componentName: string = COMPONENT_NAME;

  private readonly group: RaftGroup;
  private readonly shardId: number;
  private startedAt: number;
  private lastError: string | null = null;

  constructor(group: RaftGroup) {
    this.group = group;
    this.shardId = group.shardId;
    this.startedAt = Date.now();
  }

  liveness(): ProbeResult {
    try {
      // Liveness is basic: if the RaftGroup object exists, we're alive.
      // The Raft node's internal timers handle heartbeats.
      return { ok: true, message: 'RaftGroup is alive' };
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this.lastError = msg;
      return { ok: false, message: msg };
    }
  }

  readiness(): ProbeResult {
    try {
      // Readiness checks whether the group has a leader or is electing one.
      // We access the underlying RaftAdapter state to determine leader presence.
      const raftAdapter = (this.group as any).raft;
      if (!raftAdapter) {
        return { ok: false, message: 'RaftAdapter not initialized' };
      }

      const node = (raftAdapter as any).node;
      if (!node) {
        return { ok: false, message: 'RaftNode not initialized' };
      }

      const state: string = node.getState?.() ?? 'unknown';
      const leaderId = node.getLeaderId?.() ?? null;

      // Leader with no leader is degraded; follower/candidate without leader is ok
      if (state === 'leader' && leaderId === null) {
        return { ok: false, message: 'Leader elected but no leader ID available' };
      }

      if (state === 'candidate') {
        return {
          ok: false,
          message: 'Raft group is in candidate state (election in progress)',
          details: { state },
        };
      }

      return {
        ok: true,
        message: `Raft group is ${state}${leaderId !== null ? `, leader: ${leaderId}` : ', no leader yet'}`,
        details: { state, leaderId: leaderId !== null ? String(leaderId) : null },
      };
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this.lastError = msg;
      return { ok: false, message: msg };
    }
  }

  startup(): ProbeResult {
    try {
      const raftAdapter = (this.group as any).raft;
      if (!raftAdapter) {
        return { ok: false, message: 'RaftAdapter not initialized' };
      }
      const node = (raftAdapter as any).node;
      if (!node) {
        return { ok: false, message: 'RaftNode not initialized' };
      }

      const state: string = node.getState?.() ?? 'unknown';
      const leaderId = node.getLeaderId?.() ?? null;

      // Started if we have a coherent state (any role means running)
      const ok = state !== 'unknown' && state !== undefined;
      return {
        ok,
        message: ok
          ? `Raft group started as ${state}${leaderId !== null ? `, leader: ${leaderId}` : ''}`
          : 'Raft group has not yet established a consensus state',
      };
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return { ok: false, message: msg };
    }
  }

  /**
   * Get a full ComponentHealth snapshot for this Raft group.
   */
  async getComponentHealth(): Promise<ComponentHealth> {
    const liveness = this.liveness();
    const readiness = this.readiness();
    const uptimeSeconds = Math.floor((Date.now() - this.startedAt) / 1000);

    return {
      componentId: `${PROBE_ID}.${this.shardId}`,
      componentName: `${COMPONENT_NAME}[${this.shardId}]`,
      liveness,
      readiness,
      uptimeSeconds,
      lastError: this.lastError,
    };
  }

  /**
   * Reset the start time and last error (e.g., after a restart).
   */
  reset(): void {
    this.startedAt = Date.now();
    this.lastError = null;
  }
}
