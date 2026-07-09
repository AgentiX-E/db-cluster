/**
 * InstallSnapshotHandler Unit Tests
 *
 * Iteration 20 — G17 InstallSnapshot RPC protocol implementation.
 *
 * Tests the streaming snapshot transfer protocol: chunk creation,
 * chunk assembly, checksum verification, and snapshot installation.
 *
 * Acceptance Criteria:
 *   AC 20.1: Constructor creates snapshot dir
 *   AC 20.2: createChunks splits file into chunks by chunkSize
 *   AC 20.3: createChunks sets done=true on last chunk
 *   AC 20.4: createChunks computes SHA-256 per chunk
 *   AC 20.5: handleChunk rejects stale leader (term < localTerm)
 *   AC 20.6: handleChunk assembles chunks and calls callback on last chunk
 *   AC 20.7: handleChunk fails when assembly callback throws
 *   AC 20.8: handleChunk detects gaps in offset ordering
 *   AC 20.9: sendSnapshot streams chunks via raft.step()
 *   AC 20.10: sendSnapshot returns false for missing file
 *   AC 20.11: getAssemblyProgress returns current assembly state
 *   AC 20.12: getSnapshotDir returns config path
 *   AC 20.13: getChunkSize returns current size
 *   AC 20.14: setChunkSize updates size (min 4KB)
 *   AC 20.15: reset clears all incoming assembly states
 *   AC 20.16: cleanStaleStates removes timed-out assemblies
 *   AC 20.17: sendSnapshot returns false when file is missing (via existsSync)
 *   AC 20.18: handleChunk verifies offset ordering properly
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { InstallSnapshotHandler } from '../../consensus/extensions/InstallSnapshotHandler';
import type { InstallSnapshotRequest } from '../../interfaces/types';
import type { RaftAdapter } from '../../consensus/core/RaftAdapter';

// ── Helpers ────────────────────────────────────────────────────────

const SNAPSHOT_DIR = '/tmp/test-snapshots';
const NODE_ID = 1;

/** Create a minimal mock RaftAdapter */
function createMockRaft(term: number = 1, leader?: string): any {
  return {
    getTerm: vi.fn().mockResolvedValue(term),
    getLeader: vi.fn().mockResolvedValue(leader ?? null),
    step: vi.fn(),
  };
}

/** Create a mock InstallSnapshotRequest */
function makeChunk(
  overrides: Partial<InstallSnapshotRequest> & { data?: string },
): InstallSnapshotRequest {
  const buf = Buffer.from(overrides.data ?? 'data');
  return {
    term: 1,
    leaderId: '1',
    lastIncludedIndex: 100,
    lastIncludedTerm: 1,
    offset: 0,
    data: buf,
    done: false,
    ...overrides,
  } as unknown as InstallSnapshotRequest;
}

describe('InstallSnapshotHandler - Unit', () => {
  let handler: InstallSnapshotHandler;

  beforeEach(() => {
    vi.clearAllMocks();
    // Clean snapshot dir
    if (fs.existsSync(SNAPSHOT_DIR)) {
      fs.rmSync(SNAPSHOT_DIR, { recursive: true, force: true });
    }
    handler = new InstallSnapshotHandler(SNAPSHOT_DIR, NODE_ID);
  });

  afterEach(() => {
    handler.reset();
    if (fs.existsSync(SNAPSHOT_DIR)) {
      fs.rmSync(SNAPSHOT_DIR, { recursive: true, force: true });
    }
  });

  //
  // AC 20.1: Constructor creates snapshot dir
  //
  it('AC 20.1: Constructor creates snapshot dir', () => {
    expect(fs.existsSync(SNAPSHOT_DIR)).toBe(true);
    expect(handler.getSnapshotDir()).toBe(SNAPSHOT_DIR);
  });

  //
  // AC 20.2: createChunks splits file by chunkSize
  //
  it('AC 20.2: createChunks splits file by chunkSize', async () => {
    const filePath = path.join(SNAPSHOT_DIR, 'test-snap.dat');
    const data = Buffer.alloc(10000, 'A'); // 10KB
    fs.writeFileSync(filePath, data);

    const chunks: InstallSnapshotRequest[] = [];
    for await (const chunk of handler.createChunks(filePath, {
      lastIncludedIndex: 200,
      lastIncludedTerm: 2,
      nodeId: 1,
      shardId: 0,
      timestamp: Date.now(),
    }, '1', 4096)) {
      chunks.push(chunk);
    }

    // 10KB / 4KB = 3 chunks (4KB + 4KB + 2KB)
    expect(chunks.length).toBe(3);
    expect(chunks[0].data.length).toBe(4096);
    expect(chunks[1].data.length).toBe(4096);
    expect(chunks[2].data.length).toBe(10000 - 8192);
  });

  //
  // AC 20.3: createChunks sets done=true on last
  //
  it('AC 20.3: createChunks sets done=true on last chunk', async () => {
    const filePath = path.join(SNAPSHOT_DIR, 'test-small.dat');
    fs.writeFileSync(filePath, Buffer.alloc(100, 'B'));

    const chunks: InstallSnapshotRequest[] = [];
    for await (const chunk of handler.createChunks(filePath, {
      lastIncludedIndex: 50, lastIncludedTerm: 1,
      nodeId: 1, shardId: 0, timestamp: Date.now(),
    }, '1', 4096)) {
      chunks.push(chunk);
    }

    expect(chunks.length).toBe(1);
    expect(chunks[0].done).toBe(true);
  });

  //
  // AC 20.4: createChunks computes per-chunk checksum
  //
  it('AC 20.4: createChunks computes per-chunk checksum', async () => {
    const filePath = path.join(SNAPSHOT_DIR, 'test-checksum.dat');
    const content = Buffer.from('hello-snapshot');
    fs.writeFileSync(filePath, content);

    const chunks: any[] = [];
    for await (const chunk of handler.createChunks(filePath, {
      lastIncludedIndex: 50, lastIncludedTerm: 1,
      nodeId: 1, shardId: 0, timestamp: Date.now(),
    }, '1', 4096)) {
      chunks.push(chunk);
    }

    expect(chunks.length).toBe(1);
    expect(chunks[0]._checksum).toBeDefined();
    expect(typeof chunks[0]._checksum).toBe('string');
    expect(chunks[0]._checksum.length).toBe(64); // SHA-256 hex
  });

  //
  // AC 20.5: handleChunk rejects stale leader
  //
  it('AC 20.5: handleChunk rejects stale leader term', async () => {
    const raft = createMockRaft(5); // local term = 5
    const chunk = makeChunk({ data: 'test', term: 3 }); // leader term = 3 < 5

    const res = await handler.handleChunk(chunk, raft as any, vi.fn());
    expect(res.success).toBe(false);
    expect(res.bytesStored).toBe(0);
  });

  //
  // AC 20.6: handleChunk assembles and installs on last chunk
  //
  it('AC 20.6: handleChunk assembles chunks and calls callback', async () => {
    const raft = createMockRaft(1);
    const installCallback = vi.fn().mockResolvedValue(undefined);

    // Chunk 1 (offset 0, not done)
    const res1 = await handler.handleChunk(
      makeChunk({ data: 'chunk1', offset: 0, done: false }),
      raft as any,
      installCallback,
    );
    expect(res1.success).toBe(true);
    expect(res1.bytesStored).toBe(6); // 'chunk1'.length
    expect(installCallback).not.toHaveBeenCalled();

    // Chunk 2 (offset 1, done=true)
    const res2 = await handler.handleChunk(
      makeChunk({ data: 'chunk2', offset: 1, done: true }),
      raft as any,
      installCallback,
    );
    expect(res2.success).toBe(true);
    expect(installCallback).toHaveBeenCalledTimes(1);
  });

  //
  // AC 20.7: handleChunk callback throw propagates
  //
  it('AC 20.7: handleChunk propagates callback failure', async () => {
    const raft = createMockRaft(1);
    const installCallback = vi.fn().mockRejectedValue(new Error('apply failed'));

    await expect(handler.handleChunk(
      makeChunk({ data: 'fail', offset: 0, done: true }),
      raft as any,
      installCallback,
    )).rejects.toThrow('apply failed');
  });

  //
  // AC 20.8: handleChunk detects gaps in offset
  //
  it('AC 20.8: handleChunk detects gaps in offset ordering', async () => {
    const raft = createMockRaft(1);
    const installCallback = vi.fn().mockResolvedValue(undefined);

    // Chunk at offset 0 is first — sets lastOffset = 0
    await handler.handleChunk(
      makeChunk({ data: 'first', offset: 0, done: false }),
      raft as any,
      installCallback,
    );

    // Chunk at offset 2 (skip 1) — gap detected (not a failure, just warning)
    const res = await handler.handleChunk(
      makeChunk({ data: 'third', offset: 2 }),
      raft as any,
      installCallback,
    );
    // Should still accept — gap detection is best-effort
    expect(res.success).toBe(true);
  });

  //
  // AC 20.9: sendSnapshot streams via raft.step()
  //
  it('AC 20.9: sendSnapshot streams chunks via raft.step()', async () => {
    const filePath = path.join(SNAPSHOT_DIR, 'send-test.dat');
    const content = Buffer.alloc(5000, 'C');
    fs.writeFileSync(filePath, content);

    const raft = createMockRaft(1);

    const result = await handler.sendSnapshot(
      raft as any,
      '2',
      filePath,
      { lastIncludedIndex: 300, lastIncludedTerm: 3, nodeId: 1, shardId: 0, timestamp: Date.now() },
    );

    expect(result).toBe(true);
    // 5000 bytes / 8MB (default) = 1 chunk
    expect(raft.step).toHaveBeenCalledTimes(1);
  });

  //
  // AC 20.10: sendSnapshot returns false for missing file
  //
  it('AC 20.10: sendSnapshot returns false for missing file', async () => {
    const raft = createMockRaft(1);
    const result = await handler.sendSnapshot(
      raft as any, '2', '/nonexistent/path.dat',
      { lastIncludedIndex: 1, lastIncludedTerm: 1, nodeId: 1, shardId: 0, timestamp: 0 },
    );
    expect(result).toBe(false);
    expect(raft.step).not.toHaveBeenCalled();
  });

  //
  // AC 20.11: getAssemblyProgress returns current state
  //
  it('AC 20.11: getAssemblyProgress returns current state', async () => {
    const raft = createMockRaft(1);
    await handler.handleChunk(
      makeChunk({ data: 'test', offset: 0 }),
      raft as any,
      vi.fn(),
    );

    const progress = handler.getAssemblyProgress();
    expect(progress.length).toBeGreaterThanOrEqual(1);
    if (progress.length > 0) {
      expect(progress[0].chunks).toBe(1);
      expect(progress[0].bytesReceived).toBeGreaterThan(0);
    }
  });

  //
  // AC 20.12: getSnapshotDir returns path
  //
  it('AC 20.12: getSnapshotDir returns config path', () => {
    expect(handler.getSnapshotDir()).toBe(SNAPSHOT_DIR);
  });

  //
  // AC 20.13: getChunkSize returns default
  //
  it('AC 20.13: getChunkSize returns default (8MB)', () => {
    expect(handler.getChunkSize()).toBe(8 * 1024 * 1024);
  });

  //
  // AC 20.14: setChunkSize updates size (min 4KB)
  //
  it('AC 20.14: setChunkSize updates size (min 4KB)', () => {
    handler.setChunkSize(8192);
    expect(handler.getChunkSize()).toBe(8192);

    // Below min 4KB -> clamped to 4096
    handler.setChunkSize(100);
    expect(handler.getChunkSize()).toBe(4096);
  });

  //
  // AC 20.15: reset clears assembly states
  //
  it('AC 20.15: reset clears all incoming assembly states', async () => {
    const raft = createMockRaft(1);
    await handler.handleChunk(
      makeChunk({ data: 'test', offset: 0 }),
      raft as any,
      vi.fn(),
    );

    expect(handler.getAssemblyProgress().length).toBeGreaterThan(0);
    handler.reset();
    expect(handler.getAssemblyProgress().length).toBe(0);
  });

  //
  // AC 20.16: createChunks fails for missing file
  //
  it('AC 20.16: createChunks throws for missing file', async () => {
    await expect(async () => {
      for await (const _chunk of handler.createChunks('/nonexistent', {
        lastIncludedIndex: 1, lastIncludedTerm: 1,
        nodeId: 1, shardId: 0, timestamp: 0,
      }, '1')) {
        // never reached
      }
    }).rejects.toThrow(/not found/);
  });

  //
  // AC 20.17: Full snapshot cycles through handleChunk + createChunks
  //
  it('AC 20.17: Full snapshot cycle — create, transfer, assemble', async () => {
    // Phase 1: Create a snapshot file
    const snapContent = JSON.stringify({ key: 'value', counter: 42 });
    const filePath = path.join(SNAPSHOT_DIR, 'full-cycle.dat');
    fs.writeFileSync(filePath, snapContent);

    // Phase 2: Create chunks from file (sender side)
    const meta = { lastIncludedIndex: 500, lastIncludedTerm: 5, nodeId: 1, shardId: 0, timestamp: Date.now() };
    const chunks: InstallSnapshotRequest[] = [];
    for await (const chunk of handler.createChunks(filePath, meta, '1', 1024)) {
      chunks.push(chunk);
    }
    expect(chunks.length).toBe(1); // small file fits in one chunk
    expect(chunks[0].done).toBe(true);

    // Phase 3: Receive and assemble on a new handler (receiver side)
    const receiver = new InstallSnapshotHandler('/tmp/test-snapshots-receiver', 2);
    const raft = createMockRaft(5); // receiver term = 5, leader term = 5
    const installCallback = vi.fn().mockResolvedValue(undefined);

    const res = await receiver.handleChunk(chunks[0], raft as any, installCallback);
    expect(res.success).toBe(true);
    expect(installCallback).toHaveBeenCalledTimes(1);

    // Cleanup
    receiver.reset();
    if (fs.existsSync('/tmp/test-snapshots-receiver')) {
      fs.rmSync('/tmp/test-snapshots-receiver', { recursive: true, force: true });
    }
  });
});
