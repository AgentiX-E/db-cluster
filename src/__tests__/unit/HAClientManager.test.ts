/**
 * HAClientManager Unit Tests
 *
 * Iteration 13 — Stage 1: HAClientManager core logic tests.
 *
 * Acceptance Criteria:
 *   AC 13.1: registerClient — adds client, tracks in shard index
 *   AC 13.2: registerClient — re-register updates shard tracking
 *   AC 13.3: unregisterClient — removes client, cleans up shard index
 *   AC 13.4: unregisterClient — no-op for unknown client
 *   AC 13.5: getClientCountForShard — returns correct count
 *   AC 13.6: getTotalClientCount — returns total across all shards
 *   AC 13.7: getClientStats — returns per-shard breakdown
 *   AC 13.8: getAllClientIds — returns all registered clients
 *   AC 13.9: getConfig — returns config (defaults or custom)
 *   AC 13.10: clearAll — resets all state
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { HAClientManager } from '../../ha/HAClientManager';

describe('HAClientManager — Unit', () => {
  let clientManager: HAClientManager;

  beforeEach(() => {
    clientManager = new HAClientManager();
  });

  //
  // AC 13.1: registerClient — adds client, tracks in shard index
  //
  it('AC 13.1: registerClient — adds client and tracks by shard', () => {
    clientManager.registerClient('client-1', 'shard-0');

    expect(clientManager.getClientCountForShard('shard-0')).toBe(1);
    expect(clientManager.getTotalClientCount()).toBe(1);
    expect(clientManager.getAllClientIds()).toContain('client-1');
  });

  //
  // AC 13.2: registerClient — re-register with different shard updates tracking
  //
  it('AC 13.2: registerClient — re-register with different shard updates index', () => {
    clientManager.registerClient('client-1', 'shard-0');
    clientManager.registerClient('client-1', 'shard-1');

    expect(clientManager.getClientCountForShard('shard-0')).toBe(0);
    expect(clientManager.getClientCountForShard('shard-1')).toBe(1);
    expect(clientManager.getTotalClientCount()).toBe(1);
  });

  //
  // AC 13.3: unregisterClient — removes client and cleans up shard index
  //
  it('AC 13.3: unregisterClient — removes client and cleans up shard index', () => {
    clientManager.registerClient('client-1', 'shard-0');
    clientManager.registerClient('client-2', 'shard-0');

    clientManager.unregisterClient('client-1');

    expect(clientManager.getClientCountForShard('shard-0')).toBe(1);
    expect(clientManager.getTotalClientCount()).toBe(1);
    expect(clientManager.getAllClientIds()).toEqual(['client-2']);
  });

  //
  // AC 13.4: unregisterClient — no-op for unknown client
  //
  it('AC 13.4: unregisterClient — no-op for unknown client', () => {
    expect(() => {
      clientManager.unregisterClient('nonexistent-client');
    }).not.toThrow();
    expect(clientManager.getTotalClientCount()).toBe(0);
  });

  //
  // AC 13.5: getClientCountForShard — returns correct count
  //
  it('AC 13.5: getClientCountForShard — returns correct counts across shards', () => {
    clientManager.registerClient('c1', 's0');
    clientManager.registerClient('c2', 's0');
    clientManager.registerClient('c3', 's1');
    clientManager.registerClient('c4', 's1');
    clientManager.registerClient('c5', 's1');

    expect(clientManager.getClientCountForShard('s0')).toBe(2);
    expect(clientManager.getClientCountForShard('s1')).toBe(3);
    expect(clientManager.getClientCountForShard('s2')).toBe(0);
  });

  //
  // AC 13.6: getTotalClientCount — returns total across all shards
  //
  it('AC 13.6: getTotalClientCount — returns total across all shards', () => {
    clientManager.registerClient('c1', 's0');
    clientManager.registerClient('c2', 's0');
    clientManager.registerClient('c3', 's1');

    expect(clientManager.getTotalClientCount()).toBe(3);

    clientManager.unregisterClient('c1');
    expect(clientManager.getTotalClientCount()).toBe(2);
  });

  //
  // AC 13.7: getClientStats — returns per-shard breakdown
  //
  it('AC 13.7: getClientStats — returns per-shard breakdown', () => {
    clientManager.registerClient('c1', 's0');
    clientManager.registerClient('c2', 's0');
    clientManager.registerClient('c3', 's1');

    const stats = clientManager.getClientStats();
    expect(stats).toEqual({
      s0: 2,
      s1: 1,
    });
  });

  //
  // AC 13.8: getAllClientIds — returns all registered clients
  //
  it('AC 13.8: getAllClientIds — returns all registered client IDs', () => {
    clientManager.registerClient('c1', 's0');
    clientManager.registerClient('c2', 's1');

    const ids = clientManager.getAllClientIds();
    expect(ids).toContain('c1');
    expect(ids).toContain('c2');
    expect(ids.length).toBe(2);
  });

  //
  // AC 13.9: getConfig — returns config (defaults or custom)
  //
  it('AC 13.9: getConfig — returns default config when no custom config provided', () => {
    const config = clientManager.getConfig();
    expect(config.retryIntervalMs).toBe(1000);
    expect(config.maxRetries).toBe(10);
    expect(config.enableAutoRouting).toBe(true);
  });

  it('AC 13.9b: getConfig — custom config extends defaults', () => {
    const custom = new HAClientManager({ retryIntervalMs: 500, maxRetries: 3 });
    const config = custom.getConfig();
    expect(config.retryIntervalMs).toBe(500);
    expect(config.maxRetries).toBe(3);
    expect(config.enableAutoRouting).toBe(true); // Default preserved
  });

  //
  // AC 13.10: clearAll — resets all state
  //
  it('AC 13.10: clearAll — resets all state', () => {
    clientManager.registerClient('c1', 's0');
    clientManager.registerClient('c2', 's1');
    expect(clientManager.getTotalClientCount()).toBe(2);

    clientManager.clearAll();

    expect(clientManager.getTotalClientCount()).toBe(0);
    expect(clientManager.getClientCountForShard('s0')).toBe(0);
    expect(clientManager.getClientCountForShard('s1')).toBe(0);
    expect(clientManager.getAllClientIds()).toEqual([]);
  });
});
