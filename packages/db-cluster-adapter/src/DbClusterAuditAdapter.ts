import type {
  IAuditStore,
  WriteAuditEntryOptions,
  AuditQueryFilter,
} from '@agentix-e/db-operations-audit';
import type { AuditEntry, AuditSeverity } from '@agentix-e/db-operations-core/types';
import type { RaftGroup, LogEntry } from '@agentix-e/db-cluster';

/**
 * DbClusterAuditAdapter — Adapts a RaftGroup's committed log entries
 * into the db-operations IAuditStore interface.
 *
 * Maps Raft concepts:
 *   LogEntry (raft log entry with index/term/command/data)
 *   → AuditEntry (operation audit record)
 *
 * The Raft log serves as a write-ahead audit trail: every committed
 * entry represents an operation that was accepted by the consensus group.
 */
export class DbClusterAuditAdapter implements IAuditStore {
  private readonly group: RaftGroup;
  private readonly shardId: number;
  private readonly entries: Map<string, AuditEntry> = new Map();
  private entryCounter = 0;

  constructor(group: RaftGroup) {
    this.group = group;
    this.shardId = group.shardId;
  }

  async write(options: WriteAuditEntryOptions): Promise<string> {
    const id = this.nextId();
    const entry: AuditEntry = {
      id,
      timestamp: new Date().toISOString(),
      operation: options.operation,
      actor: options.actor,
      resource: options.resource,
      result: options.result,
      severity: options.severity ?? 'info',
      details: options.details,
      traceId: options.traceId,
    };
    this.entries.set(id, entry);
    return id;
  }

  async query(filter: AuditQueryFilter): Promise<AuditEntry[]> {
    let results = Array.from(this.entries.values());

    if (filter.startTime) {
      const start = filter.startTime.getTime();
      results = results.filter((e) => new Date(e.timestamp).getTime() >= start);
    }
    if (filter.endTime) {
      const end = filter.endTime.getTime();
      results = results.filter((e) => new Date(e.timestamp).getTime() <= end);
    }
    if (filter.operation) {
      results = results.filter((e) => e.operation === filter.operation);
    }
    if (filter.actor) {
      results = results.filter((e) => e.actor === filter.actor);
    }
    if (filter.result) {
      results = results.filter((e) => e.result === filter.result);
    }
    if (filter.severity) {
      results = results.filter((e) => e.severity === filter.severity);
    }

    // Sort by timestamp descending (most recent first)
    results.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());

    // Apply pagination
    const offset = filter.offset ?? 0;
    const limit = filter.limit ?? 100;
    return results.slice(offset, offset + limit);
  }

  async get(id: string): Promise<AuditEntry | null> {
    return this.entries.get(id) ?? null;
  }

  async count(filter?: AuditQueryFilter): Promise<number> {
    if (!filter) return this.entries.size;
    const results = await this.query(filter);
    return results.length;
  }

  async pruneOlderThan(timestamp: Date): Promise<number> {
    const threshold = timestamp.getTime();
    let removed = 0;
    for (const [id, entry] of this.entries) {
      if (new Date(entry.timestamp).getTime() < threshold) {
        this.entries.delete(id);
        removed++;
      }
    }
    return removed;
  }

  /**
   * Convert a committed Raft LogEntry into an audit entry and store it.
   * Call this from the apply handler when a Raft entry is committed.
   *
   * @param logEntry - The committed Raft log entry
   * @param actor - The actor/initiator of the operation (optional)
   */
  async recordCommittedEntry(logEntry: LogEntry, actor: string = 'raft'): Promise<string> {
    const entry: AuditEntry = {
      id: `raft-${logEntry.index}-${logEntry.term}`,
      timestamp: new Date(logEntry.timestamp).toISOString(),
      operation: logEntry.command,
      actor,
      resource: `raft/shard-${this.shardId}/log-${logEntry.index}`,
      result: 'success',
      severity: this.mapSeverity(logEntry.command),
      details: {
        logIndex: logEntry.index,
        logTerm: logEntry.term,
        data: logEntry.data,
      },
    };
    this.entries.set(entry.id, entry);
    return entry.id;
  }

  /**
   * Convert a raw raft log entry (index/term/data) from the apply handler
   * into an audit entry.
   */
  async recordApply(index: number, term: number, data: unknown, actor: string = 'raft'): Promise<string> {
    const id = `raft-${index}-${term}`;
    const entry: AuditEntry = {
      id,
      timestamp: new Date().toISOString(),
      operation: 'raft.apply',
      actor,
      resource: `raft/shard-${this.shardId}/apply-${index}`,
      result: 'success',
      severity: 'info',
      details: {
        logIndex: index,
        logTerm: term,
        data,
      },
    };
    this.entries.set(id, entry);
    return id;
  }

  /**
   * Record a Raft membership change as an audit entry.
   */
  async recordMembershipChange(change: string, peers: number[]): Promise<string> {
    return this.write({
      operation: 'raft.membership_change',
      actor: 'raft',
      resource: `raft/shard-${this.shardId}/config`,
      result: 'success',
      severity: 'warning',
      details: { change, peers },
    });
  }

  private nextId(): string {
    this.entryCounter++;
    return `audit-${this.shardId}-${Date.now()}-${this.entryCounter}`;
  }

  private mapSeverity(command: string): AuditSeverity {
    if (command.startsWith('raft.')) return 'info';
    if (command.startsWith('config') || command.startsWith('membership')) return 'warning';
    if (command.startsWith('snapshot') || command.startsWith('recovery')) return 'critical';
    return 'info';
  }
}
