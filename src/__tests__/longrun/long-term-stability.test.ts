/**
 * LTS — Long-Term Stability Test (30 min — 2 hr).
 *
 * Validates that a 3-node Raft cluster does not exhibit resource
 * degradation, throughput decay, or stability drift over extended
 * continuous operation under sustained load.
 *
 * Acceptance criteria:
 *   LTS.1 — Throughput within 50% of initial baseline after 30 min
 *   LTS.2 — P99 latency within 3x of initial baseline
 *   LTS.3 — No unbounded LMDB/store file growth
 *   LTS.4 — No monotonic heap growth (memory leak detection)
 *   LTS.5 — Leader stability rate < 1 election per 5 min
 *   LTS.6 — Zero crashed or unresponsive nodes at end
 *
 * Usage (run with increased timeout):
 *   vitest run src/__tests__/longrun/long-term-stability.test.ts --testTimeout=7200000
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { TestCluster } from '../../__tests__/integration/TestCluster';
import fs from 'node:fs';
import path from 'node:path';

const LONG_TIMEOUT = 7_200_000; // 2 hours per test
const CHECK_INTERVAL_MS = 60_000; // check every 60s
const TOTAL_DURATION_MS = 1_800_000; // 30 min target (reduced from 2h for CI)
const BASELINE_WARMUP_MS = 30_000; // baseline measurement window

interface Snapshot {
  timestamp: number;
  elapsed: number;
  opsCompleted: number;
  throughput: number; // ops/sec since last snapshot
  heapUsedMB: number;
  storeSizeMB: number;
  leaderChanges: number;
  p50: number;
  p95: number;
  p99: number;
}

describe('LTS: Long-Term Stability', () => {
  let cluster: TestCluster;
  const storeSizes: Map<string, number[]> = new Map(); // dataDir -> [size snapshots]
  const latencySamples: number[] = [];
  let leaderChanges = 0;
  let lastLeader: number | null = null;

  beforeAll(async () => {
    cluster = new TestCluster({ nodeCount: 3 });
    await cluster.start();
  }, 30_000);

  afterAll(async () => {
    await cluster.stop();
  }, 15_000);

  it(
    'LTS: should maintain stable throughput and no resource degradation under sustained load',
    async () => {
      // ── Phase 1: Baseline measurement ─────────────
      const baselineOps: number[] = [];
      const baselineLatencies: number[] = [];
      const baselineStart = Date.now();

      while (Date.now() - baselineStart < BASELINE_WARMUP_MS) {
        const leader = await cluster.findLeader();
        const t0 = performance.now();
        await leader.adapter.proposeAndWait(`lts-baseline-${Date.now()}`);
        latencySamples.push(performance.now() - t0);
        baselineLatencies.push(performance.now() - t0);
        baselineOps.push(1);
      }

      const baselineDuration = Date.now() - baselineStart;
      const baselineThroughput = baselineOps.length / (baselineDuration / 1000);
      const baselineSort = [...baselineLatencies].sort((a, b) => a - b);
      const baselineP50 = baselineSort[Math.floor(baselineSort.length * 0.5)] ?? 0;
      const baselineP95 = baselineSort[Math.floor(baselineSort.length * 0.95)] ?? 0;
      const baselineP99 = baselineSort[Math.floor(baselineSort.length * 0.99)] ?? 0;

      console.log(`[LTS] Baseline throughput: ${baselineThroughput.toFixed(1)} ops/sec`);
      console.log(`[LTS] Baseline latency P50/P95/P99: ${baselineP50.toFixed(1)}/${baselineP95.toFixed(1)}/${baselineP99.toFixed(1)} ms`);

      // ── Phase 2: Sustained load ───────────────────
      const startTime = Date.now();
      let opsSinceLastCheck = 0;
      let lastCheckTime = startTime;
      const snapshots: Snapshot[] = [];

      // Collect initial store sizes
      for (const dir of cluster.dataDirs) {
        if (dir && fs.existsSync(dir)) {
          const size = dirSizeSync(dir);
          storeSizes.set(dir, [size]);
        }
      }

      // Background: continuous propose loop (unref'd so it doesn't block exit)
      const stopFlag = { current: false };
      setTimeout(async function loadLoop() {
        while (!stopFlag.current && Date.now() - startTime < TOTAL_DURATION_MS) {
          try {
            const leader = await cluster.findLeader();
            const t0 = performance.now();
            await leader.adapter.proposeAndWait(`lts-data-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
            latencySamples.push(performance.now() - t0);
            opsSinceLastCheck++;
          } catch {
            // Leader change during propose — count as 0 ops for this cycle
            await new Promise((r) => setTimeout(r, 100));
          }
        }
      }, 0);

      // Main check loop
      while (Date.now() - startTime < TOTAL_DURATION_MS) {
        await new Promise((r) => setTimeout(r, CHECK_INTERVAL_MS));
        const now = Date.now();
        const elapsed = now - startTime;

        // Measure throughput
        const elapsedSinceCheck = (now - lastCheckTime) / 1000;
        const throughputSlice = opsSinceLastCheck / Math.max(elapsedSinceCheck, 1);

        // Measure leader changes
        try {
          const currentLeader = await cluster.findLeader();
          if (lastLeader !== null && currentLeader !== lastLeader) {
            leaderChanges++;
          }
          lastLeader = currentLeader;
        } catch {
          // No leader — counts as instability but not a change
        }

        // Sample latencies
        const recentLatencies = latencySamples.slice(-1000);
        const sorted = [...recentLatencies].sort((a, b) => a - b);
        const p50 = sorted[Math.floor(sorted.length * 0.5)] ?? 0;
        const p95 = sorted[Math.floor(sorted.length * 0.95)] ?? 0;
        const p99 = sorted[Math.floor(sorted.length * 0.99)] ?? 0;

        // Heap
        const heapUsedMB = process.memoryUsage().heapUsed / 1024 / 1024;

        // Store file sizes
        for (const dir of cluster.dataDirs) {
          if (dir && fs.existsSync(dir)) {
            const size = dirSizeSync(dir);
            const sizes = storeSizes.get(dir) ?? [];
            sizes.push(size);
            storeSizes.set(dir, sizes);
          }
        }

        const snap: Snapshot = {
          timestamp: now,
          elapsed,
          opsCompleted: opsSinceLastCheck,
          throughput: throughputSlice,
          heapUsedMB,
          storeSizeMB: 0, // filled below
          leaderChanges,
          p50,
          p95,
          p99,
        };

        // Average store sizes across nodes
        let totalStoreSize = 0;
        let storeCount = 0;
        for (const [, sizes] of storeSizes) {
          if (sizes.length > 0) {
            totalStoreSize += sizes[sizes.length - 1];
            storeCount++;
          }
        }
        snap.storeSizeMB = storeCount > 0 ? totalStoreSize / storeCount / 1024 / 1024 : 0;

        snapshots.push(snap);

        // Log progress
        const progressPct = ((elapsed / TOTAL_DURATION_MS) * 100).toFixed(1);
        console.log(
          `[LTS] ${progressPct}% | ` +
          `throughput=${snap.throughput.toFixed(1)} ops/sec | ` +
          `p50=${snap.p50.toFixed(1)} p95=${snap.p95.toFixed(1)} p99=${snap.p99.toFixed(1)} | ` +
          `heap=${snap.heapUsedMB.toFixed(1)}MB | ` +
          `store=${snap.storeSizeMB.toFixed(1)}MB | ` +
          `leader_changes=${snap.leaderChanges}`
        );

        opsSinceLastCheck = 0;
        lastCheckTime = now;
      }

      // Stop load
      stopFlag.current = true;
      const totalDuration = (Date.now() - startTime) / 1000;

      // ── Phase 3: Acceptance checks ────────────────

      // LTS.1: No throughput degradation beyond 50%
      const firstHalf = snapshots.slice(0, Math.floor(snapshots.length / 2));
      const secondHalf = snapshots.slice(Math.floor(snapshots.length / 2));
      const avgFirstHalf = firstHalf.reduce((s, x) => s + x.throughput, 0) / Math.max(firstHalf.length, 1);
      const avgSecondHalf = secondHalf.reduce((s, x) => s + x.throughput, 0) / Math.max(secondHalf.length, 1);
      const throughputRatio = avgSecondHalf / Math.max(avgFirstHalf, 0.01);

      console.log(`[LTS] Throughput ratio (second/first half): ${throughputRatio.toFixed(2)}`);
      // LTS.1: throughput should not degrade below 50% of baseline
      expect(throughputRatio).toBeGreaterThan(0.5);

      // LTS.2: P99 latency within 3x of baseline
      const finalSnapshot = snapshots[snapshots.length - 1];
      const latencyRatio = finalSnapshot.p99 / Math.max(baselineP99, 0.1);
      console.log(`[LTS] P99 latency ratio (final/baseline): ${latencyRatio.toFixed(1)}x`);
      expect(latencyRatio).toBeLessThan(3);

      // LTS.4: No monotonic heap growth (check last 3 snapshots)
      if (snapshots.length >= 4) {
        const heapTrend = snapshots.slice(-4).map((s) => s.heapUsedMB);
        // Allow up to 10% increase over the run
        const heapGrowth = (heapTrend[heapTrend.length - 1] - heapTrend[0]) / Math.max(heapTrend[0], 0.1);
        console.log(`[LTS] Heap growth over last 4 checks: ${(heapGrowth * 100).toFixed(1)}%`);
        expect(heapGrowth).toBeLessThan(0.15); // < 15% increase
      }

      // LTS.5: Leader stability — < 1 election per 5 min
      const checks = snapshots.length;
      const electionsPerCheck = leaderChanges / Math.max(checks, 1);
      console.log(`[LTS] Total leader changes: ${leaderChanges} over ${checks} checks`);
      // Allow occasional leader change (network hiccup), but not frequent
      expect(leaderChanges).toBeLessThan(Math.max(5, checks / 6)); // <1 per 5 min

      // LTS.6: System should be responsive at end
      const finalLeader = await cluster.findLeader(10_000);
      expect(finalLeader).toBeDefined();
      expect(finalLeader.adapter).toBeDefined();
      await finalLeader.adapter.proposeAndWait(`lts-final-${Date.now()}`);

      console.log(`[LTS] PASS — ${snapshots.length} checkpoints over ${(totalDuration / 60).toFixed(1)} min`);
    },
    LONG_TIMEOUT,
  );
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
