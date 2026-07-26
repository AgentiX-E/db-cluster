/**
 * LTS — Long-Term Stability Test (5 min rapid cycle).
 *
 * Validates that a 3-node Raft cluster does not exhibit resource
 * degradation, throughput decay, or stability drift under continuous
 * operation with moderate sustained load. Runs a short but dense cycle
 * to fit within a single test timeout while still proving long-run
 * correctness properties.
 *
 * Acceptance criteria (adapted for 5 min window):
 *   LTS.1 — Throughput within 40% of half-period initial baseline
 *   LTS.2 — P99 latency within 8x of baseline (single-core Raft overhead)
 *   LTS.3 — No unbounded LMDB/store file growth (> 3x initial size)
 *   LTS.4 — No monotonic heap growth (> 10% in last 4 checks)
 *   LTS.5 — Leader changes ≤ 3 (acceptable for 5 min on shared CPU)
 *   LTS.6 — System responsive at end (can propose after full run)
 *
 * Usage:
 *   vitest run src/__tests__/longrun/long-term-stability.test.ts
 *
 * NOTE: If you need the full 30 min — 2 hr run, set the environment
 *       variable LTS_FULL=1 and increase --testTimeout to 7200000.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { TestCluster } from '../../__tests__/integration/TestCluster';
import fs from 'node:fs';
import path from 'node:path';

const isFullRun = process.env.LTS_FULL === '1';
const TOTAL_DURATION_MS = isFullRun ? 1_800_000 : 300_000; // 30 min vs 5 min
const CHECK_INTERVAL_MS = isFullRun ? 60_000 : 30_000;     // 60s vs 30s
const TIMEOUT_MS = isFullRun ? 7_200_000 : 480_000;         // 2h vs 8 min
const BASELINE_WARMUP_MS = 15_000;

interface Snapshot {
  timestamp: number;
  elapsed: number;
  throughput: number;   // ops/sec since last check
  heapUsedMB: number;
  storeSizeMB: number;
  leaderChanges: number;
  p50: number;
  p95: number;
  p99: number;
}

describe('LTS: Long-Term Stability', () => {
  let cluster: TestCluster;
  const storeSizes: Map<string, number[]> = new Map();
  const latencySamples: number[] = [];
  let leaderChanges = 0;
  let lastLeader: number | null = null;

  beforeAll(async () => {
    // Retry cluster creation in case ports are in TIME_WAIT from previous tests.
    let lastError: Error | undefined;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        cluster = new TestCluster({ nodeCount: 3 });
        await cluster.start();
        // Verify all nodes are healthy — leader with quorum
        const leader = await cluster.findLeader(15_000);
        expect(leader).toBeDefined();
        expect(leader.adapter).toBeDefined();
        return; // success
      } catch (e) {
        lastError = e as Error;
        await cluster?.stop().catch(() => {});
        await new Promise((r) => setTimeout(r, 2000)); // wait for TIME_WAIT
      }
    }
    throw lastError ?? new Error('Failed to start cluster after 3 attempts');
  }, 60_000);

  afterAll(async () => {
    await cluster.stop();
    // Let TCP sockets fully close before next test uses ports
    await new Promise((r) => setTimeout(r, 500));
  }, 15_000);

  it('LTS: should maintain stable throughput and no resource degradation under sustained load', async () => {
    // ── Phase 1: Baseline measurement ──────────────────
    const baselineOps: number[] = [];
    const baselineLatencies: number[] = [];
    const baselineStart = Date.now();

    while (Date.now() - baselineStart < BASELINE_WARMUP_MS) {
      try {
        const leader = await cluster.findLeader();
        const t0 = performance.now();
        await leader.adapter.proposeAndWait(`lts-baseline-${Date.now()}`);
        const lat = performance.now() - t0;
        baselineLatencies.push(lat);
        latencySamples.push(lat);
        baselineOps.push(1);
      } catch {
        // Leader flapping during warmup – retry
        await new Promise((r) => setTimeout(r, 200));
      }
    }

    const baselineDuration = (Date.now() - baselineStart) / 1000;
    const baselineThroughput = baselineOps.length / Math.max(baselineDuration, 0.1);
    const baselineSort = [...baselineLatencies].sort((a, b) => a - b);
    const baselineP50 = baselineSort[Math.floor(baselineSort.length * 0.5)] ?? 0;
    const baselineP95 = baselineSort[Math.floor(baselineSort.length * 0.95)] ?? 0;
    const baselineP99 = baselineSort[Math.floor(baselineSort.length * 0.99)] ?? 0;

    console.log(`[LTS] Baseline throughput: ${baselineThroughput.toFixed(1)} ops/sec (${baselineOps.length} samples / ${baselineDuration.toFixed(1)}s)`);
    console.log(`[LTS] Baseline latency P50/P95/P99: ${baselineP50.toFixed(1)}/${baselineP95.toFixed(1)}/${baselineP99.toFixed(1)} ms`);

    // Initial store sizes
    for (const dir of cluster.dataDirs) {
      if (dir && fs.existsSync(dir)) {
        storeSizes.set(dir, [dirSizeSync(dir)]);
      }
    }

    // ── Phase 2: Sustained load ───────────────────────
    const startTime = Date.now();
    let opsSinceLastCheck = 0;
    let lastCheckTime = startTime;
    const snapshots: Snapshot[] = [];

    const stopFlag = { current: false };

    // Controlled load loop with backoff (avoid flooding the pipeline)
    setTimeout(async function loadLoop(): Promise<void> {
      // eslint-disable-next-line no-unmodified-loop-condition
      while (!stopFlag.current && Date.now() - startTime < TOTAL_DURATION_MS) {
        try {
          const leader = await cluster.findLeader();
          const t0 = performance.now();
          await leader.adapter.proposeAndWait(`lts-data-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
          latencySamples.push(performance.now() - t0);
          opsSinceLastCheck++;
          // Small yield to prevent pipeline saturation
          await new Promise((r) => setTimeout(r, 1));
        } catch {
          // Leader change — brief backoff then retry
          await new Promise((r) => setTimeout(r, 200));
        }
      }
    }, 0);

    // Check loop
    while (Date.now() - startTime < TOTAL_DURATION_MS) {
      await new Promise((r) => setTimeout(r, CHECK_INTERVAL_MS));
      const now = Date.now();
      const elapsed = now - startTime;
      const elapsedSinceCheck = (now - lastCheckTime) / 1000;
      const throughputSlice = opsSinceLastCheck / Math.max(elapsedSinceCheck, 0.1);

      // Leader changes
      try {
        const currentLeader = await cluster.findLeader();
        const cid = currentLeader?.id ?? -1;
        if (lastLeader !== null && cid !== lastLeader) {
          leaderChanges++;
        }
        lastLeader = cid;
      } catch {
        // No leader available — skip this check
      }

      // Latencies (most recent 1000 samples)
      const recent = latencySamples.slice(-1000);
      const sorted = [...recent].sort((a, b) => a - b);
      const p50 = sorted[Math.floor(sorted.length * 0.5)] ?? 0;
      const p95 = sorted[Math.floor(sorted.length * 0.95)] ?? 0;
      const p99 = sorted[Math.floor(sorted.length * 0.99)] ?? 0;

      const heapUsedMB = process.memoryUsage().heapUsed / 1024 / 1024;

      // Store sizes
      for (const dir of cluster.dataDirs) {
        if (dir && fs.existsSync(dir)) {
          const sizes = storeSizes.get(dir) ?? [];
          sizes.push(dirSizeSync(dir));
          storeSizes.set(dir, sizes);
        }
      }

      let totalStoreSize = 0;
      let storeCount = 0;
      for (const [, sizes] of storeSizes) {
        if (sizes.length > 0) { totalStoreSize += sizes[sizes.length - 1]; storeCount++; }
      }

      const snap: Snapshot = {
        timestamp: now,
        elapsed,
        throughput: throughputSlice,
        heapUsedMB,
        storeSizeMB: storeCount > 0 ? totalStoreSize / storeCount / 1024 / 1024 : 0,
        leaderChanges,
        p50, p95, p99,
      };

      snapshots.push(snap);

      const pct = ((elapsed / TOTAL_DURATION_MS) * 100).toFixed(0);
      console.log(
        `[LTS] ${pct}% | tput=${snap.throughput.toFixed(1)} o/s | ` +
        `p50=${snap.p50.toFixed(1)} p95=${snap.p95.toFixed(1)} p99=${snap.p99.toFixed(1)} | ` +
        `heap=${snap.heapUsedMB.toFixed(1)}MB store=${snap.storeSizeMB.toFixed(1)}MB | ` +
        `leader_changes=${snap.leaderChanges}`
      );

      opsSinceLastCheck = 0;
      lastCheckTime = now;
    }

    stopFlag.current = true;
    const totalDuration = (Date.now() - startTime) / 1000;

    // ── Phase 3: Acceptance checks ────────────────────

    // Ensure we have enough data points
    expect(snapshots.length).toBeGreaterThan(3);

    // LTS.1: Throughput — compare second half to first half
    const mid = Math.floor(snapshots.length / 2);
    const firstHalf = snapshots.slice(0, mid);
    const secondHalf = snapshots.slice(mid);
    const avgFirst = firstHalf.reduce((s, x) => s + x.throughput, 0) / Math.max(firstHalf.length, 1);
    const avgSecond = secondHalf.reduce((s, x) => s + x.throughput, 0) / Math.max(secondHalf.length, 1);
    const ratio = avgSecond / Math.max(avgFirst, 0.01);

    console.log(`[LTS] Throughput: firstHalf=${avgFirst.toFixed(1)} secondHalf=${avgSecond.toFixed(1)} ratio=${ratio.toFixed(2)}`);
    expect(ratio).toBeGreaterThan(0.4);

    // LTS.2: P99 latency — within 8x of baseline (single-core allows overhead)
    const finalSnap = snapshots[snapshots.length - 1];
    const latRatio = finalSnap.p99 / Math.max(baselineP99, 0.1);
    console.log(`[LTS] P99 latency: final=${finalSnap.p99.toFixed(1)} baseline=${baselineP99.toFixed(1)} ratio=${latRatio.toFixed(1)}x`);
    expect(latRatio).toBeLessThan(8);

    // LTS.3: Store growth — < 3x initial
    const initStore = snapshots[0]?.storeSizeMB ?? 0;
    const maxStore = Math.max(...snapshots.map((s) => s.storeSizeMB));
    const storeRatio = maxStore / Math.max(initStore, 0.01);
    console.log(`[LTS] Store: initial=${initStore.toFixed(1)}MB max=${maxStore.toFixed(1)}MB ratio=${storeRatio.toFixed(1)}x`);
    expect(storeRatio).toBeLessThan(3);

    // LTS.4: Heap growth over last 4 checkpoints < 10%
    if (snapshots.length >= 4) {
      const trend = snapshots.slice(-4).map((s) => s.heapUsedMB);
      const growth = (trend[trend.length - 1] - trend[0]) / Math.max(trend[0], 0.1);
      console.log(`[LTS] Heap growth: ${(growth * 100).toFixed(1)}%`);
      expect(growth).toBeLessThan(0.10);
    }

    // LTS.5: Leader stability — ≤ 3 in short run, ≤ 1 per 5 min in full
    const maxChanges = isFullRun ? Math.max(5, snapshots.length / 6) : 3;
    console.log(`[LTS] Leader changes: ${leaderChanges} (max allowed: ${maxChanges})`);
    expect(leaderChanges).toBeLessThanOrEqual(maxChanges);

    // LTS.6: System responsive at end
    const finalLeader = await cluster.findLeader(10_000);
    expect(finalLeader).toBeDefined();
    expect(finalLeader.adapter).toBeDefined();
    await finalLeader.adapter.proposeAndWait(`lts-final-check-${Date.now()}`);

    console.log(`[LTS] PASS — ${snapshots.length} checkpoints / ${(totalDuration / 60).toFixed(1)} min`);
  }, TIMEOUT_MS);
});

/** Recursive directory size in bytes. */
function dirSizeSync(dir: string): number {
  let total = 0;
  try {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        total += dirSizeSync(full);
      } else if (entry.isFile()) {
        total += fs.statSync(full).size;
      }
    }
  } catch {
    // Directory may be cleaned up during test teardown
  }
  return total;
}
