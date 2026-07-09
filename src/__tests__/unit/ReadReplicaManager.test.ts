/**
 * ReadReplicaManager Unit Tests
 *
 * Iteration 19 — Read replica management for horizontal read scaling.
 *
 * Manages non-voting Raft learner nodes for read offloading with
 * round-robin routing, health checking, and WAL lag tracking.
 *
 * Acceptance Criteria:
 *   AC 19.1: Constructor accepts optional heartbeat interval
 *   AC 19.2: registerReplica creates new replica with healthy status
 *   AC 19.3: registerReplica throws on duplicate ID
 *   AC 19.4: deregisterReplica removes existing replica
 *   AC 19.5: deregisterReplica throws on non-existent replica
 *   AC 19.6: routeReadQuery returns replica (round-robin)
 *   AC 19.7: routeReadQuery throws when all replicas disconnected
 *   AC 19.8: routeReadQuery skips disconnected replicas
 *   AC 19.9: reportHeartbeat updates lag and compute status
 *   AC 19.10: reportHeartbeat does nothing for unknown replica
 *   AC 19.11: checkHealth updates status based on heartbeat timeout
 *   AC 19.12: reportHeartbeat marks healthy with low lag
 *   AC 19.13: reportHeartbeat marks lagging with high lag
 *   AC 19.14: listReplicas returns registered replicas
 *   AC 19.15: getMaxLag returns 0 with no replicas
 *   AC 19.16: getMaxLag returns max across replicas
 *   AC 19.17: getReplica returns copy or undefined
 *   AC 19.18: count returns number of replicas
 *   AC 19.19: reset clears all replicas
 *   AC 19.20: routeReadQuery round-robin cycles through available replicas
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { ReadReplicaManager, READ_REPLICA_HEARTBEAT_TIMEOUT } from '../../ha/ReadReplicaManager';

describe('ReadReplicaManager - Unit', () => {
  let mgr: ReadReplicaManager;

  beforeEach(() => {
    mgr = new ReadReplicaManager();
  });

  afterEach(() => {
    mgr.reset();
  });

  //
  // AC 19.1: Constructor
  //
  it('AC 19.1: Constructor accepts optional heartbeat interval', () => {
    const m1 = new ReadReplicaManager();
    expect(m1).toBeInstanceOf(ReadReplicaManager);

    const m2 = new ReadReplicaManager(10000);
    expect(m2).toBeInstanceOf(ReadReplicaManager);
  });

  //
  // AC 19.2: registerReplica creates with healthy status
  //
  it('AC 19.2: registerReplica creates with healthy status', () => {
    const r = mgr.registerReplica('r1', 'host1:3000');
    expect(r.id).toBe('r1');
    expect(r.address).toBe('host1:3000');
    expect(r.role).toBe('learner');
    expect(r.lag).toBe(0);
    expect(r.status).toBe('healthy');
    expect(r.lastHeartbeat).toBeGreaterThan(0);
  });

  //
  // AC 19.3: registerReplica throws on duplicate ID
  //
  it('AC 19.3: registerReplica throws on duplicate ID', () => {
    mgr.registerReplica('r1', 'host1:3000');
    expect(() => mgr.registerReplica('r1', 'host2:3001')).toThrow(/already exists/);
  });

  //
  // AC 19.4: deregisterReplica removes existing replica
  //
  it('AC 19.4: deregisterReplica removes existing replica', () => {
    mgr.registerReplica('r1', 'host1:3000');
    expect(mgr.count()).toBe(1);
    mgr.deregisterReplica('r1');
    expect(mgr.count()).toBe(0);
  });

  //
  // AC 19.5: deregisterReplica throws on non-existent
  //
  it('AC 19.5: deregisterReplica throws on non-existent', () => {
    expect(() => mgr.deregisterReplica('unknown')).toThrow(/not found/);
  });

  //
  // AC 19.6: routeReadQuery returns replica address
  //
  it('AC 19.6: routeReadQuery returns replica', () => {
    mgr.registerReplica('r1', 'host1:3000');
    const result = mgr.routeReadQuery('SELECT 1');
    expect(result.replicaId).toBe('r1');
    expect(result.address).toBe('host1:3000');
  });

  //
  // AC 19.7: routeReadQuery throws when all disconnected
  //
  it('AC 19.7: routeReadQuery throws when all disconnected', () => {
    mgr.registerReplica('r1', 'host1:3000');
    // Simulate disconnect via heartbeat timeout
    const r = mgr.getReplica('r1')!;
    mgr.reportHeartbeat('r1', 0);
    // Force disconnected by setting old timestamp directly
    // Need to use checkHealth with elapsed > timeout
    // Simulate: set replica's lastHeartbeat to old value
    const replicaInternal = (mgr as any).replicas.get('r1');
    replicaInternal.lastHeartbeat = Date.now() - READ_REPLICA_HEARTBEAT_TIMEOUT - 1000;
    mgr.checkHealth();
    expect(() => mgr.routeReadQuery('SELECT 1')).toThrow(/No read replicas/);
  });

  //
  // AC 19.8: routeReadQuery skips disconnected replicas
  //
  it('AC 19.8: routeReadQuery skips disconnected replicas', () => {
    mgr.registerReplica('r1', 'host1:3000');
    mgr.registerReplica('r2', 'host2:3000');

    // Disconnect r1
    const r1 = (mgr as any).replicas.get('r1');
    r1.lastHeartbeat = Date.now() - READ_REPLICA_HEARTBEAT_TIMEOUT - 1000;
    mgr.checkHealth();

    // Route should skip r1 and return r2
    const result = mgr.routeReadQuery('SELECT 1');
    expect(result.replicaId).toBe('r2');
  });

  //
  // AC 19.9: reportHeartbeat updates lag and compute status
  //
  it('AC 19.9: reportHeartbeat updates lag', () => {
    mgr.registerReplica('r1', 'host1:3000');
    mgr.reportHeartbeat('r1', 500);
    const r = mgr.getReplica('r1')!;
    expect(r.lag).toBe(500);
  });

  //
  // AC 19.10: reportHeartbeat for unknown does nothing
  //
  it('AC 19.10: reportHeartbeat for unknown does nothing', () => {
    expect(() => mgr.reportHeartbeat('unknown', 100)).not.toThrow();
  });

  //
  // AC 19.11: checkHealth updates disconnected status
  //
  it('AC 19.11: checkHealth marks stale replicas as disconnected', async () => {
    mgr.registerReplica('r1', 'host1:3000');
    const r1 = (mgr as any).replicas.get('r1');
    r1.lastHeartbeat = Date.now() - READ_REPLICA_HEARTBEAT_TIMEOUT - 1000;
    await mgr.checkHealth();
    const r = mgr.getReplica('r1')!;
    expect(r.status).toBe('disconnected');
  });

  //
  // AC 19.12: reportHeartbeat marks healthy with low lag
  //
  it('AC 19.12: reportHeartbeat marks healthy with low lag', () => {
    mgr.registerReplica('r1', 'host1:3000');
    mgr.reportHeartbeat('r1', 100); // 100 bytes lag (well under 1MB)
    const r = mgr.getReplica('r1')!;
    expect(r.status).toBe('healthy');
  });

  //
  // AC 19.13: reportHeartbeat marks lagging with high lag
  //
  it('AC 19.13: reportHeartbeat with high lag marks lagging', () => {
    mgr.registerReplica('r1', 'host1:3000');
    mgr.reportHeartbeat('r1', 2 * 1024 * 1024); // 2 MB lag (over 1MB threshold)
    const r = mgr.getReplica('r1')!;
    expect(r.status).toBe('lagging');
  });

  //
  // AC 19.14: listReplicas returns registered replicas
  //
  it('AC 19.14: listReplicas returns registered replicas', () => {
    mgr.registerReplica('r1', 'host1:3000');
    mgr.registerReplica('r2', 'host2:3000');
    const list = mgr.listReplicas();
    expect(list.length).toBe(2);
  });

  //
  // AC 19.15: getMaxLag returns 0 with no replicas
  //
  it('AC 19.15: getMaxLag returns 0 with no replicas', () => {
    expect(mgr.getMaxLag()).toBe(0);
  });

  //
  // AC 19.16: getMaxLag returns max across replicas
  //
  it('AC 19.16: getMaxLag returns max across replicas', () => {
    mgr.registerReplica('r1', 'host1:3000');
    mgr.registerReplica('r2', 'host2:3000');
    mgr.reportHeartbeat('r1', 100);
    mgr.reportHeartbeat('r2', 2000);
    expect(mgr.getMaxLag()).toBe(2000);
  });

  //
  // AC 19.17: getReplica returns copy or undefined
  //
  it('AC 19.17: getReplica returns copy or undefined', () => {
    expect(mgr.getReplica('none')).toBeUndefined();
    mgr.registerReplica('r1', 'host1:3000');
    const r = mgr.getReplica('r1')!;
    expect(r.id).toBe('r1');
    // Modifying returned copy should not affect original
    r.lag = 9999;
    expect(mgr.getReplica('r1')!.lag).not.toBe(9999);
  });

  //
  // AC 19.18: count returns number of replicas
  //
  it('AC 19.18: count returns number of replicas', () => {
    expect(mgr.count()).toBe(0);
    mgr.registerReplica('r1', 'host1:3000');
    expect(mgr.count()).toBe(1);
    mgr.registerReplica('r2', 'host2:3000');
    expect(mgr.count()).toBe(2);
  });

  //
  // AC 19.19: reset clears all replicas
  //
  it('AC 19.19: reset clears all replicas', () => {
    mgr.registerReplica('r1', 'host1:3000');
    mgr.registerReplica('r2', 'host2:3000');
    expect(mgr.count()).toBe(2);
    mgr.reset();
    expect(mgr.count()).toBe(0);
  });

  //
  // AC 19.20: routeReadQuery round-robin cycles
  //
  it('AC 19.20: routeReadQuery round-robin cycles through replicas', () => {
    mgr.registerReplica('r1', 'host1:3000');
    mgr.registerReplica('r2', 'host2:3000');

    const ids: string[] = [];
    for (let i = 0; i < 4; i++) {
      ids.push(mgr.routeReadQuery('SELECT 1').replicaId);
    }
    expect(ids).toEqual(['r1', 'r2', 'r1', 'r2']);
  });
});
