import type { IMetricsRegistry, ICounter, IGauge, IHistogram } from '@agentix-e/db-operations-metrics';
import type { MetricsSnapshot, MetricPoint, MetricType, MetricLabel } from '@agentix-e/db-operations-core/types';
import type { RaftGroup } from '@agentix-e/db-cluster';

/**
 * DbClusterMetricAdapter — Exposes Raft consensus metrics through
 * the db-operations IMetricsRegistry interface.
 *
 * Tracks:
 *   - raft_current_term          (gauge)
 *   - raft_committed_index       (gauge)
 *   - raft_last_applied_index    (gauge)
 *   - raft_leader_id             (gauge, numeric)
 *   - raft_peers_total           (gauge)
 *   - raft_entries_total         (counter)
 *   - raft_state                 (gauge: 0=follower, 1=candidate, 2=leader)
 *   - raft_leader_changes_total  (counter)
 *   - raft_elections_total       (counter)
 */
export class DbClusterMetricAdapter implements IMetricsRegistry {
  private readonly group: RaftGroup;
  private readonly shardId: number;
  private readonly _counters: Map<string, ICounter> = new Map();
  private readonly _gauges: Map<string, IGauge> = new Map();
  private readonly _histograms: Map<string, IHistogram> = new Map();

  private lastKnownLeader: number | null = null;

  constructor(group: RaftGroup) {
    this.group = group;
    this.shardId = group.shardId;
  }

  counter(name: string, labels?: Record<string, string>): ICounter {
    const key = this.key(name, labels);
    let c = this._counters.get(key);
    if (!c) {
      c = new CounterImpl();
      this._counters.set(key, c);
    }
    return c;
  }

  gauge(name: string, labels?: Record<string, string>): IGauge {
    const key = this.key(name, labels);
    let g = this._gauges.get(key);
    if (!g) {
      g = new GaugeImpl();
      this._gauges.set(key, g);
    }
    return g;
  }

  histogram(name: string, labels?: Record<string, string>, _buckets?: number[]): IHistogram {
    const key = this.key(name, labels);
    let h = this._histograms.get(key);
    if (!h) {
      h = new HistogramImpl();
      this._histograms.set(key, h);
    }
    return h;
  }

  snapshot(): MetricsSnapshot {
    // Refresh Raft metrics from the underlying group before snapshotting
    this._refresh();

    const points: MetricPoint[] = [];
    const now = Date.now();

    for (const [key, c] of this._counters) {
      points.push({
        name: this.parseName(key),
        type: 'counter',
        value: c.get(),
        labels: this.parseLabels(key),
        timestamp: now,
      });
    }
    for (const [key, g] of this._gauges) {
      points.push({
        name: this.parseName(key),
        type: 'gauge',
        value: g.get(),
        labels: this.parseLabels(key),
        timestamp: now,
      });
    }
    for (const [key, h] of this._histograms) {
      points.push({
        name: this.parseName(key),
        type: 'histogram',
        value: h.getCount(),
        labels: this.parseLabels(key),
        timestamp: now,
      });
    }

    return { points, capturedAt: now };
  }

  /**
   * Refresh Raft metrics from the underlying group state.
   * Call this periodically or on-demand before reading metrics.
   */
  private _refresh(): void {
    try {
      const raftAdapter = (this.group as any).raft;
      if (!raftAdapter) return;

      const node = (raftAdapter as any).node;
      if (!node) return;

      const term = node.getCurrentTerm?.() ?? 0;
      const commitIdx = node.getCommittedIndex?.() ?? 0;
      const lastApplied = node.getLastApplied?.() ?? 0;
      const lastLogIdx = node.getLastLogIndex?.() ?? 0;
      const state: string = node.getState?.() ?? 'follower';
      const leaderId: number | null = node.getLeaderId?.() ?? null;

      const labels = { shard: String(this.shardId) };

      // Gauge: current term
      this._gauge('raft_current_term', labels).set(term);

      // Gauge: commit index
      this._gauge('raft_committed_index', labels).set(commitIdx);

      // Gauge: last applied
      this._gauge('raft_last_applied_index', labels).set(lastApplied);

      // Gauge: last log index
      this._gauge('raft_last_log_index', labels).set(lastLogIdx);

      // Gauge: leader (0 = none, otherwise node id)
      this._gauge('raft_leader_id', labels).set(leaderId ?? 0);

      // Gauge: peers count
      const peers = (raftAdapter as any).config?.peers;
      const peerCount = Array.isArray(peers) ? peers.length : 0;
      this._gauge('raft_peers_total', labels).set(peerCount);

      // Gauge: state (0=follower, 1=candidate, 2=leader)
      const stateValue = state === 'leader' ? 2 : state === 'candidate' ? 1 : 0;
      this._gauge('raft_state', labels).set(stateValue);

      // Counter: log entries processed
      this._counter('raft_log_entries_total', labels).incBy(Math.max(0, lastLogIdx));

      // Counter: leader changes
      if (leaderId !== null && this.lastKnownLeader !== null && leaderId !== this.lastKnownLeader) {
        this._counter('raft_leader_changes_total', labels).inc();
      }
      if (leaderId !== null) {
        this.lastKnownLeader = leaderId;
      }
    } catch {
      // Swallow errors during refresh — metrics collection should never throw
    }
  }

  private _counter(name: string, labels?: Record<string, string>): ICounter {
    return this.counter(name, labels);
  }

  private _gauge(name: string, labels?: Record<string, string>): IGauge {
    return this.gauge(name, labels);
  }

  /**
   * Register an election event (called externally when an election is detected).
   */
  recordElection(): void {
    this._counter('raft_elections_total', { shard: String(this.shardId) }).inc();
  }

  private key(name: string, labels?: Record<string, string>): string {
    if (!labels || Object.keys(labels).length === 0) return name;
    const sorted = Object.entries(labels)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${v}`)
      .join(',');
    return `${name}{${sorted}}`;
  }

  private parseName(key: string): string {
    const brace = key.indexOf('{');
    return brace === -1 ? key : key.slice(0, brace);
  }

  private parseLabels(key: string): MetricLabel[] {
    const brace = key.indexOf('{');
    if (brace === -1) return [];
    const inner = key.slice(brace + 1, key.length - 1);
    if (!inner) return [];
    return inner.split(',').map((pair) => {
      const [k, v] = pair.split('=');
      return { key: k, value: v };
    });
  }
}

// ── In-memory metric implementations ──────────────────────────

class CounterImpl implements ICounter {
  private value = 0;
  inc(): void {
    this.value += 1;
  }
  incBy(value: number): void {
    this.value += value;
  }
  reset(): void {
    this.value = 0;
  }
  get(): number {
    return this.value;
  }
}

class GaugeImpl implements IGauge {
  private value = 0;
  set(value: number): void {
    this.value = value;
  }
  inc(): void {
    this.value += 1;
  }
  dec(): void {
    this.value -= 1;
  }
  add(value: number): void {
    this.value += value;
  }
  get(): number {
    return this.value;
  }
}

class HistogramImpl implements IHistogram {
  private values: number[] = [];
  observe(value: number): void {
    this.values.push(value);
  }
  getCount(): number {
    return this.values.length;
  }
  getSum(): number {
    return this.values.reduce((a, b) => a + b, 0);
  }
  getQuantile(p: number): number | undefined {
    if (this.values.length === 0) return undefined;
    const sorted = [...this.values].sort((a, b) => a - b);
    const idx = Math.floor(p * (sorted.length - 1));
    return sorted[idx];
  }
  reset(): void {
    this.values = [];
  }
}
