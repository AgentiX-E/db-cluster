import type { IDeadlockDetector, IDeadlockInfo } from '@agentix-e/db-operations-monitor';
import type { RaftGroup } from '@agentix-e/db-cluster';

/**
 * DeadlockEvent — internal record of a Raft election deadlock.
 * Based on the Raft consensus scenario where split votes prevent
 * leader election (no single candidate gets a majority).
 */
export interface RaftElectionDeadlockEvent {
  /** Timestamp when the deadlock was detected. */
  timestamp: number;
  /** Current term during the deadlock. */
  term: number;
  /** Node IDs involved in the split vote. */
  candidates: number[];
  /** Number of election rounds without a leader. */
  electionRound: number;
  /** Whether the deadlock was resolved. */
  resolved: boolean;
}

/**
 * DbClusterDeadlockAdapter — Monitors Raft election cycles for
 * split-vote deadlocks where no single candidate can win a majority.
 *
 * In Raft, a split vote occurs when multiple candidates receive votes
 * from different followers, and no candidate reaches a majority. This
 * is a "deadlock" in the election process that is normally resolved by
 * randomized election timeouts. This adapter detects such scenarios
 * and reports them as deadlock events.
 */
export class DbClusterDeadlockAdapter implements IDeadlockDetector {
  private readonly group: RaftGroup;
  private readonly shardId: number;
  private _running = false;
  private _monitorTimer: ReturnType<typeof setInterval> | null = null;
  private _onDeadlockCallback: ((info: IDeadlockInfo) => void) | null = null;
  private _deadlockHistory: IDeadlockInfo[] = [];

  // Election tracking state
  private previousLeader: number | null = null;
  private previousTerm: number = 0;
  private electionRound: number = 0;
  private consecutiveFailedElections: number = 0;
  private candidateIds: number[] = [];

  /** Minimum consecutive failed election rounds before reporting a deadlock. */
  private readonly deadlockThreshold: number = 3;
  /** Poll interval in ms. */
  private readonly pollIntervalMs: number = 2000;

  constructor(group: RaftGroup) {
    this.group = group;
    this.shardId = group.shardId;
  }

  async start(): Promise<void> {
    if (this._running) return;
    this._running = true;

    // Bootstrap initial state
    await this._captureState();

    this._monitorTimer = setInterval(async () => {
      await this._poll();
    }, this.pollIntervalMs);
  }

  async stop(): Promise<void> {
    this._running = false;
    if (this._monitorTimer) {
      clearInterval(this._monitorTimer);
      this._monitorTimer = null;
    }
  }

  onDeadlock(callback: (info: IDeadlockInfo) => void): void {
    this._onDeadlockCallback = callback;
  }

  async getDeadlockHistory(): Promise<IDeadlockInfo[]> {
    return [...this._deadlockHistory];
  }

  private async _captureState(): Promise<void> {
    try {
      const raftAdapter = (this.group as any).raft;
      if (!raftAdapter) return;
      const node = (raftAdapter as any).node;
      if (!node) return;

      const leaderId: number | null = node.getLeaderId?.() ?? null;
      const term: number = node.getCurrentTerm?.() ?? 0;

      this.previousLeader = leaderId;
      this.previousTerm = term;
    } catch {
      // Swallow
    }
  }

  private async _poll(): Promise<void> {
    try {
      const raftAdapter = (this.group as any).raft;
      if (!raftAdapter) return;
      const node = (raftAdapter as any).node;
      if (!node) return;

      const state: string = node.getState?.() ?? 'follower';
      const leaderId: number | null = node.getLeaderId?.() ?? null;
      const term: number = node.getCurrentTerm?.() ?? 0;

      // If we have a leader, the election succeeded — reset tracking
      if (leaderId !== null) {
        this.consecutiveFailedElections = 0;
        this.electionRound = 0;
        this.candidateIds = [];
        this.previousLeader = leaderId;
        this.previousTerm = term;
        return;
      }

      // No leader: check if we're in election phase
      if (state === 'candidate') {
        this.electionRound++;
        this.candidateIds.push(this.group.shardId);

        // Check: if we've been in candidate state for multiple rounds
        // with no leader, it's likely a split-vote deadlock
        if (this.electionRound >= this.deadlockThreshold) {
          this.consecutiveFailedElections++;

          const event: IDeadlockInfo = {
            cycle: this.candidateIds.map((id) => `node-${id}`),
            victimTransaction: `election-round-${this.electionRound}`,
            detectedAt: new Date(),
          };

          this._deadlockHistory.push(event);
          this._onDeadlockCallback?.(event);

          // Reset candidate tracking to avoid duplicate reports
          this.candidateIds = [];
          this.electionRound = 0;
        }
      } else if (state === 'follower') {
        // Follower with no leader: election hasn't started yet
        // But if we've been here too long, it could be a deadlock
        if (this.consecutiveFailedElections > 0) {
          // Still in deadlock state
        }
      }
    } catch {
      // Swallow
    }
  }

  /**
   * Manually report a deadlock event (useful for external detection).
   *
   * @param description - Human-readable description of the deadlock
   */
  reportDeadlock(description: string): void {
    const event: IDeadlockInfo = {
      cycle: [`shard-${this.shardId}`],
      victimTransaction: description,
      detectedAt: new Date(),
    };

    this._deadlockHistory.push(event);
    this._onDeadlockCallback?.(event);
  }

  /**
   * Clear deadlock history.
   */
  clearHistory(): void {
    this._deadlockHistory = [];
  }

  /**
   * Get the current deadlock detection state.
   */
  getStatus(): { running: boolean; consecutiveFailedElections: number } {
    return {
      running: this._running,
      consecutiveFailedElections: this.consecutiveFailedElections,
    };
  }
}
