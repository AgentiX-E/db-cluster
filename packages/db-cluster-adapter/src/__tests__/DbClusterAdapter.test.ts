import { describe, it, expect, beforeEach, vi } from 'vitest';

// ── Mock RaftGroup (since full integration requires network) ──

interface MockRaftNode {
  getState: () => string;
  getCurrentTerm: () => number;
  getLeaderId: () => number | null;
  getCommittedIndex: () => number;
  getLastApplied: () => number;
  getLastLogIndex: () => number;
  campaign?: () => Promise<void>;
}

interface MockRaftAdapter {
  node: MockRaftNode;
  config?: { peers: number[] };
  getStatus: () => Promise<any>;
}

function createMockGroup(shardId: number, overrides?: Partial<MockRaftNode>): any {
  const node: MockRaftNode = {
    getState: () => overrides?.getState?.() ?? 'leader',
    getCurrentTerm: () => overrides?.getCurrentTerm?.() ?? 1,
    getLeaderId: () => overrides?.getLeaderId?.() ?? null,
    getCommittedIndex: () => overrides?.getCommittedIndex?.() ?? 0,
    getLastApplied: () => overrides?.getLastApplied?.() ?? 0,
    getLastLogIndex: () => overrides?.getLastLogIndex?.() ?? 0,
    ...overrides,
  };

  const adapter: MockRaftAdapter = {
    node,
    config: { peers: [1, 2, 3] },
    getStatus: async () => ({
      role: node.getState(),
      term: String(node.getCurrentTerm()),
      lead: node.getLeaderId() !== null ? String(node.getLeaderId()) : null,
    }),
  };

  return {
    shardId,
    raft: adapter,
    replica: { apply: vi.fn() },
  };
}

// ── Tests ───────────────────────────────────────────────────

describe('DbClusterHealthAdapter', () => {
  let DbClusterHealthAdapter: any;
  let adapter: any;

  beforeEach(async () => {
    const mod = await import('../DbClusterHealthAdapter.js');
    DbClusterHealthAdapter = mod.DbClusterHealthAdapter;
  });

  it('should report liveness ok when RaftGroup exists', () => {
    const group = createMockGroup(1, { getState: () => 'leader', getLeaderId: () => 42 });
    adapter = new DbClusterHealthAdapter(group);
    const result = adapter.liveness();
    expect(result.ok).toBe(true);
    expect(result.message).toBe('RaftGroup is alive');
  });

  it('should report readiness ok for leader with leader ID', () => {
    const group = createMockGroup(1, { getState: () => 'leader', getLeaderId: () => 42 });
    adapter = new DbClusterHealthAdapter(group);
    const result = adapter.readiness();
    expect(result.ok).toBe(true);
    expect(result.message).toContain('leader');
    expect(result.message).toContain('42');
  });

  it('should report readiness ok for follower', () => {
    const group = createMockGroup(1, { getState: () => 'follower', getLeaderId: () => 42 });
    adapter = new DbClusterHealthAdapter(group);
    const result = adapter.readiness();
    expect(result.ok).toBe(true);
    expect(result.message).toContain('follower');
  });

  it('should report readiness degraded for candidate state', () => {
    const group = createMockGroup(1, { getState: () => 'candidate', getLeaderId: () => null });
    adapter = new DbClusterHealthAdapter(group);
    const result = adapter.readiness();
    expect(result.ok).toBe(false);
    expect(result.message).toContain('candidate');
    expect(result.message).toContain('election');
  });

  it('should report startup ok after initialization', () => {
    const group = createMockGroup(1, { getState: () => 'leader', getLeaderId: () => 42 });
    adapter = new DbClusterHealthAdapter(group);
    const result = adapter.startup();
    expect(result.ok).toBe(true);
  });

  it('should generate component health with shardId', async () => {
    const group = createMockGroup(5, { getState: () => 'leader', getLeaderId: () => 1 });
    adapter = new DbClusterHealthAdapter(group);
    const health = await adapter.getComponentHealth();
    expect(health.componentId).toContain('5');
    expect(health.componentName).toContain('5');
    expect(health.liveness.ok).toBe(true);
    expect(health.uptimeSeconds).toBeGreaterThanOrEqual(0);
  });
});

describe('DbClusterMetricAdapter', () => {
  let DbClusterMetricAdapter: any;
  let adapter: any;
  let group: any;

  beforeEach(async () => {
    const mod = await import('../DbClusterMetricAdapter.js');
    DbClusterMetricAdapter = mod.DbClusterMetricAdapter;
  });

  it('should create and increment a counter', () => {
    group = createMockGroup(1, { getState: () => 'leader', getLeaderId: () => 1 });
    adapter = new DbClusterMetricAdapter(group);
    const c = adapter.counter('test_counter');
    expect(c.get()).toBe(0);
    c.inc();
    expect(c.get()).toBe(1);
    c.incBy(3);
    expect(c.get()).toBe(4);
  });

  it('should create and set a gauge', () => {
    group = createMockGroup(1, { getState: () => 'leader', getLeaderId: () => 1 });
    adapter = new DbClusterMetricAdapter(group);
    const g = adapter.gauge('test_gauge');
    g.set(100);
    expect(g.get()).toBe(100);
    g.inc();
    expect(g.get()).toBe(101);
    g.dec();
    expect(g.get()).toBe(100);
    g.add(-10);
    expect(g.get()).toBe(90);
  });

  it('should include raft state metrics in snapshot', () => {
    group = createMockGroup(1, {
      getState: () => 'leader',
      getLeaderId: () => 42,
      getCurrentTerm: () => 5,
      getCommittedIndex: () => 100,
      getLastApplied: () => 95,
      getLastLogIndex: () => 105,
    });
    adapter = new DbClusterMetricAdapter(group);
    const snap = adapter.snapshot();
    expect(snap.points.length).toBeGreaterThan(0);
    expect(snap.capturedAt).toBeGreaterThan(0);

    // Find specific metrics
    const termPoint = snap.points.find((p: any) => p.name === 'raft_current_term');
    expect(termPoint).toBeDefined();
    expect(termPoint.value).toBe(5);

    const commitPoint = snap.points.find((p: any) => p.name === 'raft_committed_index');
    expect(commitPoint).toBeDefined();
    expect(commitPoint.value).toBe(100);

    const statePoint = snap.points.find((p: any) => p.name === 'raft_state');
    expect(statePoint).toBeDefined();
    // leader = 2
    expect(statePoint.value).toBe(2);
  });

  it('should track leader changes as a counter', () => {
    // First snapshot: leader is 1
    group = createMockGroup(1, {
      getState: () => 'leader',
      getLeaderId: () => 1,
      getCurrentTerm: () => 1,
      getCommittedIndex: () => 0,
      getLastApplied: () => 0,
      getLastLogIndex: () => 0,
    });
    adapter = new DbClusterMetricAdapter(group);
    const snap1 = adapter.snapshot();

    // Second snapshot: leader is 2 → change detected
    // We need to change the group's leader after the first snapshot
    Object.defineProperty(group.raft.node, 'getLeaderId', {
      value: () => 2,
    });
    const snap2 = adapter.snapshot();
    const changesPoint = snap2.points.find((p: any) => p.name === 'raft_leader_changes_total');
    // changes should exist
    expect(changesPoint).toBeDefined();
  });
});

describe('DbClusterAuditAdapter', () => {
  let DbClusterAuditAdapter: any;
  let adapter: any;

  beforeEach(async () => {
    const mod = await import('../DbClusterAuditAdapter.js');
    DbClusterAuditAdapter = mod.DbClusterAuditAdapter;
  });

  it('should write and retrieve audit entries', async () => {
    const group = createMockGroup(1);
    adapter = new DbClusterAuditAdapter(group);
    const id = await adapter.write({
      operation: 'test.op',
      actor: 'tester',
      resource: 'test/resource',
      result: 'success',
    });
    expect(id).toBeTruthy();

    const entry = await adapter.get(id);
    expect(entry).not.toBeNull();
    expect(entry!.operation).toBe('test.op');
    expect(entry!.actor).toBe('tester');
    expect(entry!.result).toBe('success');
  });

  it('should record Raft committed entries', async () => {
    const group = createMockGroup(1);
    adapter = new DbClusterAuditAdapter(group);
    const id = await adapter.recordCommittedEntry({
      index: 42,
      term: 7,
      command: 'test.command',
      data: { key: 'value' },
      timestamp: Date.now(),
    });
    expect(id).toBe('raft-42-7');

    const entry = await adapter.get(id);
    expect(entry).not.toBeNull();
    expect(entry!.operation).toBe('test.command');
    expect(entry!.details?.logIndex).toBe(42);
    expect(entry!.details?.logTerm).toBe(7);
  });

  it('should query with filters', async () => {
    const group = createMockGroup(1);
    adapter = new DbClusterAuditAdapter(group);

    await adapter.write({
      operation: 'op1',
      actor: 'alice',
      resource: 'r1',
      result: 'success',
      severity: 'info',
    });
    await adapter.write({
      operation: 'op2',
      actor: 'bob',
      resource: 'r2',
      result: 'failure',
      severity: 'error',
    });

    const bobEntries = await adapter.query({ actor: 'bob' });
    expect(bobEntries.length).toBe(1);
    expect(bobEntries[0].actor).toBe('bob');

    const failEntries = await adapter.query({ result: 'failure' });
    expect(failEntries.length).toBe(1);
    expect(failEntries[0].result).toBe('failure');
  });

  it('should prune old entries', async () => {
    const group = createMockGroup(1);
    adapter = new DbClusterAuditAdapter(group);

    await adapter.write({
      operation: 'old',
      actor: 'test',
      resource: 'r',
      result: 'success',
    });

    // Prune with a future date (should remove everything)
    const removed = await adapter.pruneOlderThan(new Date(Date.now() + 10000));
    expect(removed).toBe(1);
    const count = await adapter.count();
    expect(count).toBe(0);
  });
});

describe('DbClusterDeadlockAdapter', () => {
  let DbClusterDeadlockAdapter: any;
  let adapter: any;

  beforeEach(async () => {
    const mod = await import('../DbClusterDeadlockAdapter.js');
    DbClusterDeadlockAdapter = mod.DbClusterDeadlockAdapter;
  });

  it('should start and stop without errors', async () => {
    const group = createMockGroup(1, { getState: () => 'leader', getLeaderId: () => 1 });
    adapter = new DbClusterDeadlockAdapter(group);
    await adapter.start();
    expect(adapter.getStatus().running).toBe(true);
    await adapter.stop();
    expect(adapter.getStatus().running).toBe(false);
  });

  it('should report manually triggered deadlocks', async () => {
    const group = createMockGroup(1);
    adapter = new DbClusterDeadlockAdapter(group);
    const callback = vi.fn();
    adapter.onDeadlock(callback);
    adapter.reportDeadlock('split vote detected');
    const history = await adapter.getDeadlockHistory();
    expect(history.length).toBe(1);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledWith(
      expect.objectContaining({
        cycle: expect.any(Array),
        victimTransaction: 'split vote detected',
      }),
    );
  });

  it('should clear deadlock history', async () => {
    const group = createMockGroup(1);
    adapter = new DbClusterDeadlockAdapter(group);
    adapter.reportDeadlock('test deadlock');
    expect((await adapter.getDeadlockHistory()).length).toBe(1);
    adapter.clearHistory();
    expect((await adapter.getDeadlockHistory()).length).toBe(0);
  });
});

describe('DbClusterTracerAdapter', () => {
  let DbClusterTracerAdapter: any;
  let adapter: any;

  beforeEach(async () => {
    const mod = await import('../DbClusterTracerAdapter.js');
    DbClusterTracerAdapter = mod.DbClusterTracerAdapter;
  });

  it('should create and end spans', () => {
    const group = createMockGroup(1);
    adapter = new DbClusterTracerAdapter(group);
    const { spanId, end } = adapter.startSpan('test.op');
    expect(spanId).toBeTruthy();
    end('ok');
    const spans = adapter.getCompletedSpans();
    expect(spans.length).toBe(1);
    expect(spans[0].name).toBe('test.op');
    expect(spans[0].status).toBe('ok');
    expect(spans[0].spanId).toBe(spanId);
  });

  it('should create tracing spans for Raft operations', () => {
    const group = createMockGroup(1);
    adapter = new DbClusterTracerAdapter(group);

    adapter.tracePropose(1, 1);
    adapter.traceCommit(1, 1);
    adapter.traceApply(1, 1);

    const spans = adapter.getCompletedSpans();
    // propose and commit spans are not auto-ended; apply is auto-ended
    const applySpan = spans.find((s: any) => s.name === 'raft.apply');
    expect(applySpan).toBeDefined();
    expect(applySpan!.status).toBe('ok');
    expect(applySpan!.attributes['raft.entry_index']).toBe(1);
  });

  it('should report run error status', () => {
    const group = createMockGroup(1);
    adapter = new DbClusterTracerAdapter(group);
    const { spanId, end } = adapter.startSpan('failing.op');
    end('error', 'something went wrong');
    const spans = adapter.getCompletedSpans();
    expect(spans[0].status).toBe('error');
    expect(spans[0].errorMessage).toBe('something went wrong');
  });

  it('should inject and extract trace context', () => {
    const group = createMockGroup(1);
    adapter = new DbClusterTracerAdapter(group);

    // Start a span to set current context
    adapter.startSpan('context.test');
    const carrier: Record<string, string> = {};
    adapter.inject(carrier);
    expect(carrier['traceparent']).toBeDefined();
    expect(carrier['traceparent']).toMatch(/^00-/);

    // Extract from a fresh adapter
    const adapter2 = new DbClusterTracerAdapter(group);
    const extracted = adapter2.extract(carrier);
    expect(extracted).toBeDefined();
    expect(extracted!.traceId).toBeTruthy();
  });
});

describe('createAdapters factory', () => {
  it('should create all five adapters from a single RaftGroup', async () => {
    const mod = await import('../index.js');
    const { createAdapters } = mod;

    const group = createMockGroup(1, {
      getState: () => 'leader',
      getLeaderId: () => 42,
      getCurrentTerm: () => 3,
      getCommittedIndex: () => 50,
      getLastApplied: () => 48,
      getLastLogIndex: () => 52,
    });

    const adapters = createAdapters(group);

    // Health
    expect(adapters.health.liveness().ok).toBe(true);
    expect(adapters.health.readiness().ok).toBe(true);

    // Metrics
    const snap = adapters.metrics.snapshot();
    expect(snap.points.length).toBeGreaterThan(0);

    // Audit
    const auditId = await adapters.audit.write({
      operation: 'factory.test',
      actor: 'factory',
      resource: 'test',
      result: 'success',
    });
    expect(auditId).toBeTruthy();

    // Deadlock
    await adapters.deadlock.start();
    expect(adapters.deadlock.getStatus().running).toBe(true);
    await adapters.deadlock.stop();

    // Tracer
    const { spanId, end } = adapters.tracer.startSpan('factory.op');
    expect(spanId).toBeTruthy();
    end('ok');
  });
});
