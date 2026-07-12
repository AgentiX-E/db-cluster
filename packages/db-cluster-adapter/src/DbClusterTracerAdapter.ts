import type { IDistributedTracer, Span } from '@agentix-e/db-operations-observability';
import type { RaftGroup } from '@agentix-e/db-cluster';
import * as crypto from 'node:crypto';

/**
 * DbClusterTracerAdapter — Adapts Raft consensus operations into
 * the db-operations IDistributedTracer interface.
 *
 * Creates spans for the core Raft lifecycle:
 *   propose   → entry submitted to the leader
 *   commit    → entry committed by consensus
 *   apply     → entry applied to the state machine
 *
 * Additional trace spans:
 *   campaign  → leader election
 *   snapshot  → snapshot creation/installation
 *   transfer  → leadership transfer
 */
export class DbClusterTracerAdapter implements IDistributedTracer {
  private readonly group: RaftGroup;
  private readonly shardId: number;
  private readonly spans: Span[] = [];
  private readonly activeSpans: Map<string, { startTime: number; traceId: string; parentSpanId?: string; name: string }> = new Map();
  private spanCounter = 0;

  /** Current trace context (for async context propagation) */
  private currentContext: { traceId: string; spanId: string } | undefined;

  constructor(group: RaftGroup) {
    this.group = group;
    this.shardId = group.shardId;
  }

  startSpan(name: string, options?: {
    parentSpanId?: string;
    traceId?: string;
    attributes?: Record<string, string | number | boolean>;
  }): { spanId: string; end: (status?: 'ok' | 'error', errorMessage?: string) => void } {
    const spanId = this.nextId();
    const traceId = options?.traceId ?? this.currentContext?.traceId ?? this.nextSpanId();
    const parentSpanId = options?.parentSpanId ?? this.currentContext?.spanId;

    const startTime = Date.now();

    const entry = { startTime, traceId, parentSpanId, name };
    this.activeSpans.set(spanId, entry);

    // Set as current context
    this.currentContext = { traceId, spanId };

    // Enrich with shard attribute
    const attributes = {
      'raft.shard_id': this.shardId,
      ...(options?.attributes ?? {}),
    } as Record<string, string | number | boolean>;

    // Create future reference — the span record will be finalized on end()
    const adapter = this;

    return {
      spanId,
      end(status?: 'ok' | 'error', errorMessage?: string) {
        adapter._endSpan(spanId, name, traceId, parentSpanId, startTime, attributes, status, errorMessage);
      },
    };
  }

  private _endSpan(
    spanId: string,
    name: string,
    traceId: string,
    parentSpanId: string | undefined,
    startTime: number,
    attributes: Record<string, string | number | boolean>,
    status: 'ok' | 'error' = 'ok',
    errorMessage?: string,
  ): void {
    const endTime = Date.now();
    const span: Span = {
      spanId,
      traceId,
      parentSpanId,
      name,
      startTime,
      endTime,
      attributes,
      status,
      errorMessage,
    };
    this.spans.push(span);
    this.activeSpans.delete(spanId);
  }

  getCurrentSpan(): { traceId: string; spanId: string } | undefined {
    return this.currentContext;
  }

  getCompletedSpans(): Span[] {
    return [...this.spans];
  }

  clearCompletedSpans(): void {
    this.spans.length = 0;
  }

  inject(carrier: Record<string, string>): void {
    const ctx = this.currentContext;
    if (!ctx) return;
    carrier['traceparent'] = `00-${ctx.traceId}-${ctx.spanId}-01`;
  }

  extract(carrier: Record<string, string>): { traceId: string; parentSpanId: string } | undefined {
    const traceparent = carrier['traceparent'];
    if (traceparent) {
      // W3C Trace Context format: 00-{traceId}-{spanId}-{flags}
      const parts = traceparent.split('-');
      if (parts.length >= 3) {
        const traceId = parts[1];
        const spanId = parts[2];
        // Set extracted context as current
        this.currentContext = { traceId, spanId };
        return { traceId, parentSpanId: spanId };
      }
    }
    return undefined;
  }

  /**
   * Create a span for a propose operation (entry submitted to leader).
   * Returns the span ID.
   */
  tracePropose(entryIndex: number, term: number): string {
    const { spanId, end } = this.startSpan('raft.propose', {
      attributes: {
        'raft.entry_index': entryIndex,
        'raft.term': term,
      },
    });

    // In a real system, end is called when commit is observed.
    // For the adapter, we return the end function reference stored in activeSpans.
    return spanId;
  }

  /**
   * Create a span for a commit operation (entry committed by consensus).
   */
  traceCommit(entryIndex: number, term: number): string {
    const { spanId, end } = this.startSpan('raft.commit', {
      attributes: {
        'raft.entry_index': entryIndex,
        'raft.term': term,
      },
    });
    return spanId;
  }

  /**
   * Create a span for an apply operation (entry applied to state machine).
   */
  traceApply(entryIndex: number, term: number): string {
    const { spanId, end } = this.startSpan('raft.apply', {
      attributes: {
        'raft.entry_index': entryIndex,
        'raft.term': term,
      },
    });

    // Auto-end the apply span (apply is synchronous)
    end('ok');
    return spanId;
  }

  /**
   * Create a span for an election campaign.
   */
  traceCampaign(term: number): string {
    const { spanId, end } = this.startSpan('raft.campaign', {
      attributes: {
        'raft.term': term,
      },
    });

    // Campaign spans end when a leader is elected or timeout
    return spanId;
  }

  /**
   * End a previously started span by ID.
   */
  endSpan(spanId: string, status: 'ok' | 'error' = 'ok', errorMessage?: string): void {
    const active = this.activeSpans.get(spanId);
    if (!active) return;

    const endTime = Date.now();
    const span: Span = {
      spanId,
      traceId: active.traceId,
      parentSpanId: active.parentSpanId,
      name: active.name,
      startTime: active.startTime,
      endTime,
      attributes: {
        'raft.shard_id': this.shardId,
      },
      status,
      errorMessage: status === 'error' ? errorMessage : undefined,
    };
    this.spans.push(span);
    this.activeSpans.delete(spanId);
  }

  private nextId(): string {
    this.spanCounter++;
    return `span-${this.shardId}-${Date.now()}-${this.spanCounter}`;
  }

  private nextSpanId(): string {
    return crypto.randomBytes(8).toString('hex');
  }
}
