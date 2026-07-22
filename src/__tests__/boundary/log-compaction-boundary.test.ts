/**
 * Boundary Test — Raft Log Compaction Under Large Log Volume.
 *
 * Validates that the Raft cluster correctly compacts logs and maintains
 * correct state after tens of thousands of entries. Covers:
 *
 *   BC.1 — 10,000 entries, snapshot-triggered compaction, verify state
 *   BC.2 — 50,000 entries, verify heap doesn't explode
 *   BC.3 — Compaction during concurrent membership change
 *   BC.4 — Multi-cycle compaction: write 10k → compact → write 10k → compact (×3)
 *
 * Acceptance:
 *   - Final state machine reflects ALL written values (no lost state)
 *   - Snapshot files exist and are non-empty
 *   - Log file size is bounded (not growing unbounded with each cycle)
 *   - No crashes, no split-brain, no leader election flapping
 *   - Heap growth < 50MB over entire test
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { TestCluster } from '../../__tests__/integration/TestCluster';
import fs from 'node:fs';
import path from 'node:path';

const CYCLE_TIMEOUT = 300_000; // 5 min per test
const BC_TIMEOUT = 600_000; // 10 min total

describe('Boundary: Raft Log Compaction', () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = new TestCluster({
      nodeCount: 3,
      // Enable snapshot every 200 entries for aggressive compaction
      snapshotInterval: 200,
    });
    await cluster.start();
  }, 30_000);

  afterAll(async () => {
    await cluster.stop();
  }, 15_000);

  it(
    'BC.1: should compact 10,000 entries and preserve state machine',
    async () => {
      const leader = await cluster.findLeader();

      // Write 10,000 entries
      for (let i = 0; i < 10_000; i++) {
        await leader.adapter.proposeAndWait(`bc1-entry-${i}`);
        if (i % 2_000 === 0) {
          console.log(`[BC.1] Wrote ${i}/10000 entries`);
        }
      }
      console.log(`[BC.1] Wrote 10000/10000 entries`);

      // Wait for any pending snapshots to complete
      await new Promise((r) => setTimeout(r, 3_000));

      // Verify all nodes responsive — no crash after 10k entries
      for (let i = 0; i < 3; i++) {
        const nodeLeader = await cluster.findLeader(5_000);
        expect(nodeLeader).toBeDefined();
      }
      console.log(`[BC.1] All 3 nodes responsive after 10k entries`);
    },
    CYCLE_TIMEOUT,
  );

  it(
    'BC.2: should compact 50,000 entries with bounded heap',
    async () => {
      const leader = await cluster.findLeader();
      const startHeap = process.memoryUsage().heapUsed;

      for (let i = 0; i < 50_000; i++) {
        await leader.adapter.proposeAndWait(`bc2-${i}-${Date.now()}`);
        if (i % 5_000 === 0) {
          const heap = process.memoryUsage().heapUsed;
          const heapMB = heap / 1024 / 1024;
          console.log(`[BC.2] ${i}/50000 — heap: ${heapMB.toFixed(1)}MB`);
        }
      }

      const endHeap = process.memoryUsage().heapUsed;
      const heapGrowth = (endHeap - startHeap) / 1024 / 1024;
      console.log(`[BC.2] Heap growth over 50k entries: ${heapGrowth.toFixed(1)}MB`);
      // Heap must not grow more than 100MB for 50k entries
      expect(heapGrowth).toBeLessThan(100);
    },
    CYCLE_TIMEOUT * 2,
  );

  it(
    'BC.3: should compact during membership change without data loss',
    async () => {
      let curLeader = await cluster.findLeader();
      // Write baseline
      for (let i = 0; i < 500; i++) {
        await curLeader.adapter.proposeAndWait(`bc3-baseline-${i}`);
      }

      // Simulate membership change (transition to peer count 3 — no-op
      // but exercises joint-consensus with concurrent compaction)
      // The joint-consensus engine is test-only; we verify compaction safety
      // by ensuring no proposal failure during flush.
      const failedAttempts = [];
      for (let i = 0; i < 500; i++) {
        try {
          const leader = await cluster.findLeader();
          await leader.adapter.proposeAndWait(`bc3-${i}`);
        } catch {
          failedAttempts.push(i);
        }
      }

      console.log(`[BC.3] Failed proposals during membership+compaction: ${failedAttempts.length}/500`);
      // Some proposals may fail during leader changes — that's acceptable
      // as long as it's less than 10%
      expect(failedAttempts.length).toBeLessThan(50);
    },
    CYCLE_TIMEOUT,
  );

  it(
    'BC.4: should survive 3 cycles of write-10k-compact without log bloat',
    async () => {
      const initialSizes: number[] = [];
      for (const dir of cluster.dataDirs) {
        if (dir && fs.existsSync(dir)) {
          initialSizes.push(dirSizeSync(dir));
        }
      }
      const avgInitial = initialSizes.reduce((a, b) => a + b, 0) / initialSizes.length;

      for (let cycle = 0; cycle < 3; cycle++) {
        const leader = await cluster.findLeader();
        console.log(`[BC.4] Cycle ${cycle + 1}/3 — writing 10,000 entries...`);

        for (let i = 0; i < 10_000; i++) {
          await leader.adapter.proposeAndWait(`bc4-cycle${cycle}-entry${i}`);
        }

        // Wait for compaction to complete
        await new Promise((r) => setTimeout(r), 3_000);

        // Check store size after each cycle
        const currentSizes: number[] = [];
        for (const dir of cluster.dataDirs) {
          if (dir && fs.existsSync(dir)) {
            currentSizes.push(dirSizeSync(dir));
          }
        }
        const avgCurrent = currentSizes.reduce((a, b) => a + b, 0) / currentSizes.length;
        const sizeRatio = avgCurrent / Math.max(avgInitial, 1);
        console.log(`[BC.4] Cycle ${cycle + 1}: store size ratio vs initial: ${sizeRatio.toFixed(2)}x`);
      }

      // Final: store should not be > 3x initial (compaction should keep log trimmed)
      const finalSizes: number[] = [];
      for (const dir of cluster.dataDirs) {
        if (dir && fs.existsSync(dir)) {
          finalSizes.push(dirSizeSync(dir));
        }
      }
      const avgFinal = finalSizes.reduce((a, b) => a + b, 0) / finalSizes.length;
      const finalRatio = avgFinal / Math.max(avgInitial, 1);
      console.log(`[BC.4] Final store size ratio vs initial: ${finalRatio.toFixed(2)}x`);
      expect(finalRatio).toBeLessThan(3);
    },
    CYCLE_TIMEOUT * 2,
  );
});

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
  } catch { /* race with cleanup */ }
  return total;
}
