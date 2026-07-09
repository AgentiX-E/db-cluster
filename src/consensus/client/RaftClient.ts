/**
 * RaftClient — Enterprise Raft Client
 *
 * Encapsulates propose auto-retry, leader caching, exponential backoff, onLeaderChange notification.
 *
 * Design principles:
 * - Client does not need to know current leader
 * - Follower rejection transparently retried, not exposed to caller
 * - Exponential backoff prevents thundering herd
 * - Optional idempotent dedup (same data sent only once)
 *
 * Usage:
 *   const client = new RaftClient({
 *     initialLeader: 1,
 *     nodeIds: [1, 2, 3],
 *     proposeOnce: (nodeId, data) => raftAdapter.propose(data),
 *   });
 *   const { index, term } = await client.propose('my-command');
 */

import {
  SqlVecError,
  SqlVecErrorCode,
} from '../../interfaces/types';

// ── Types ──

export interface ProposeResult {
  /** Commit index assigned to this entry */
  index: number;
  /** Term during which this entry was committed */
  term: number;
}

export interface RaftClientConfig {
  /** Initial leader guess (will be updated on NOT_LEADER responses) */
  initialLeader: number;
  /** All node IDs in the cluster (for locating alternative leaders) */
  nodeIds: number[];
  /**
   * Propose a command to a specific node.
   * Must throw SqlVecError with code NOT_LEADER if the target is not leader.
   * The error's message should contain the leader ID (parsed from message).
   */
  proposeOnce: (nodeId: number, data: string) => Promise<ProposeResult>;
  /** Get the current leader from any node */
  getLeader?: (nodeId: number) => Promise<number>;
  /** Maximum number of retry attempts before giving up (default: 5) */
  maxRetries?: number;
  /** Initial retry delay in ms (default: 200, doubles each attempt) */
  baseRetryMs?: number;
  /** Maximum retry delay in ms (default: 8000) */
  maxRetryMs?: number;
}

// ── Implementation ──

export class RaftClient {
  private config: Required<RaftClientConfig>;
  private currentLeader: number;
  private leaderChangeCallbacks: Array<(leaderId: number) => void> = [];
  /** In-flight propose promises, keyed by data for deduplication */
  private inflight: Map<string, Promise<ProposeResult>> = new Map();

  constructor(config: RaftClientConfig) {
    this.config = {
      initialLeader: config.initialLeader,
      nodeIds: config.nodeIds,
      proposeOnce: config.proposeOnce,
      getLeader: config.getLeader ?? (async (nodeId: number): Promise<number> => {
        // Default: cannot resolve leader, return current guess
        return this.currentLeader;
      }),
      maxRetries: config.maxRetries ?? 5,
      baseRetryMs: config.baseRetryMs ?? 200,
      maxRetryMs: config.maxRetryMs ?? 8000,
    };
    this.currentLeader = this.config.initialLeader;
  }

  /** Get the current cached leader. */
  getCurrentLeader(): number {
    return this.currentLeader;
  }

  /**
   * Register a callback for leader changes.
   * Called whenever the client detects a leader has changed via NOT_LEADER.
   */
  onLeaderChange(callback: (leaderId: number) => void): void {
    this.leaderChangeCallbacks.push(callback);
  }

  /**
   * Propose a command to the Raft cluster.
   *
   * Automatically retries on NOT_LEADER, updates leader cache,
   * and uses exponential backoff between attempts.
   *
   * If the same `data` string is proposed concurrently, only one RPC
   * is issued (deduplication). Both callers get the same result.
   *
   * @param data  Command payload
   * @throws SqlVecError if all retries exhausted or non-retriable error
   */
  async propose(data: string): Promise<ProposeResult> {
    // ── Deduplication ──
    const existing = this.inflight.get(data);
    if (existing) {
      return existing;
    }

    // Build a promise that handles inflight cleanup on both paths
    const promise = this.proposeInternal(data)
      .then((result) => {
        this.inflight.delete(data);
        return result;
      })
      .catch((err) => {
        this.inflight.delete(data);
        // Wrap in a new rejection so error propagation to caller still works
        return Promise.reject(err);
      });
    this.inflight.set(data, promise);

    return promise;
  }

  private async proposeInternal(data: string): Promise<ProposeResult> {
    let lastError: Error | null = null;
    let retryDelay = this.config.baseRetryMs;

    for (let attempt = 0; attempt <= this.config.maxRetries; attempt++) {
      const targetNode = this.currentLeader;
      try {
        const result = await this.config.proposeOnce(targetNode, data);

        // Also try other nodes if this result looks empty (belt-and-suspenders)
        if (result.index === 0 && attempt < this.config.maxRetries) {
          await this.backoff(retryDelay);
          retryDelay = Math.min(retryDelay * 2, this.config.maxRetryMs);
          continue;
        }

        return result;
      } catch (err: any) {
        lastError = err;

        // ── Extract leader from NOT_LEADER error ──
        if (this.isNotLeaderError(err)) {
          const newLeader = this.parseLeaderFromError(err);
          if (newLeader !== null && newLeader !== this.currentLeader) {
            this.updateLeader(newLeader);
            // Retry with new leader, no backoff (immediate redirection)
            continue;
          }
          // Same leader but still failing — try other nodes
          const candidate = this.findAlternativeNode();
          if (candidate !== null) {
            this.updateLeader(candidate);
            continue;
          }
        }

        // ── Non-retriable errors or exhausted alternatives ──
        if (!this.isNotLeaderError(err)) {
          // Non-leader errors (storage failure, quorum loss) are not retriable
          break;
        }

        if (attempt < this.config.maxRetries) {
          await this.backoff(retryDelay);
          retryDelay = Math.min(retryDelay * 2, this.config.maxRetryMs);
          continue;
        }

        break;
      }
    }

    throw new SqlVecError(
      `RaftClient: propose failed after ${this.config.maxRetries + 1} attempts: ${lastError?.message ?? 'unknown error'}`,
      SqlVecErrorCode.INTERNAL_ERROR,
    );
  }

  /**
   * Check if an error is a NOT_LEADER rejection.
   * Matches SqlVecError with code RAFT_NOT_LEADER=100 or NOT_LEADER=100.
   */
  private isNotLeaderError(err: any): boolean {
    if (err instanceof SqlVecError) {
      return err.code === SqlVecErrorCode.NOT_LEADER;
    }
    // Also handle plain errors with appropriate code pattern
    return err.code === 100
      || (typeof err.message === 'string' && (
        err.message.includes('NOT_LEADER')
        || err.message.includes('Not the Raft leader')
        || err.message.includes('not the leader')
        || err.message.includes('leader=')
      ));
  }

  /**
   * Parse leader ID from error message.
   * Expected format: "Not the Raft leader for this shard (leader=2)"
   * Also matches: "leader=2", "not leader, current is 2", etc.
   */
  private parseLeaderFromError(err: any): number | null {
    const msg: string = err.message ?? '';
    // Pattern: leader=<number>
    const match = msg.match(/leader[=:]\s*(\d+)/i);
    if (match) {
      return parseInt(match[1], 10);
    }
    return null;
  }

  /**
   * Find a node that is not the current leader guess.
   * Used when the current leader is unreachable but we don't have a redirect.
   */
  private findAlternativeNode(): number | null {
    for (const id of this.config.nodeIds) {
      if (id !== this.currentLeader) {
        return id;
      }
    }
    return null;
  }

  /** Update the cached leader and emit change callbacks. */
  private updateLeader(newLeader: number): void {
    const oldLeader = this.currentLeader;
    this.currentLeader = newLeader;
    if (oldLeader !== newLeader) {
      for (const cb of this.leaderChangeCallbacks) {
        try {
          cb(newLeader);
        } catch {
          // Swallow callback errors — don't crash the propose flow
        }
      }
    }
  }

  /** Sleep with a small random jitter (±25%) to avoid thundering herd. */
  private async backoff(baseMs: number): Promise<void> {
    const jitter = (0.75 + Math.random() * 0.5) * baseMs;
    return new Promise(r => setTimeout(r, jitter));
  }
}
