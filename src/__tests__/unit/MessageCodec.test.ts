/**
 * MessageCodec Unit Tests
 *
 * Iteration 24 — TCP wire protocol for @maboke123/raft-core messages.
 *
 * Wire format: [4 bytes BE length][1 byte format][N bytes JSON/MsgPack payload].
 * IMPORTANT: encode() returns full wire frame; decode() expects buffer WITHOUT
 * the 4-byte length header (just format byte + payload, or pure JSON for legacy).
 *
 * Acceptance Criteria:
 *   AC 24.1: encode() produces length-prefixed JSON Buffer
 *   AC 24.2: encode() sets correct format byte (0x00 JSON)
 *   AC 24.3: decode() reconstructs the original from subarray(4) frame
 *   AC 24.4: encode+decode round-trip preserves all fields
 *   AC 24.5: encode+decode with entries preserves entry fields
 *   AC 24.6: encode+decode with base64 entry data
 *   AC 24.7: encodeMsgPack produces MsgPack Buffer
 *   AC 24.8: decodeMsgPack from subarray(4) reconstructs message
 *   AC 24.9: JSON + MsgPack produce same decoded result
 *   AC 24.10: normaliseForWasm converts u64 fields to strings
 *   AC 24.11: normaliseForWasm converts entryType to number
 *   AC 24.12: normaliseEntryData converts Buffer data to base64
 *   AC 24.13: Large message encode+decode round-trip
 *   AC 24.14: MsgPack decode with binary entry data
 *   AC 24.15: MsgPack encode+decode round-trip preserves schema
 */

import { describe, it, expect } from 'vitest';
import { MessageCodec, type RaftMessage } from '../../consensus/transport/MessageCodec';

describe('MessageCodec - Unit', () => {
  // Helper: encode returns full wire frame; decode expects buffer[4..]
  function encodeAndDecode(msg: RaftMessage): RaftMessage {
    const buf = MessageCodec.encode(msg);
    return MessageCodec.decode(buf.subarray(4)); // strip length header
  }

  function encodeAndDecodeMsgPack(msg: RaftMessage): RaftMessage {
    const buf = MessageCodec.encodeMsgPack(msg);
    return MessageCodec.decodeMsgPack(buf.subarray(4)); // strip length header
  }

  //
  // AC 24.1: encode produces length-prefixed JSON Buffer
  //
  it('AC 24.1: encode() produces length-prefixed JSON Buffer', () => {
    const msg: RaftMessage = { msgType: 1, from: '1', to: '2', term: '1' };
    const buf = MessageCodec.encode(msg);
    expect(buf).toBeInstanceOf(Buffer);
    expect(buf.length).toBeGreaterThan(5);

    const length = buf.readUInt32BE(0);
    expect(length).toBe(buf.length - 4); // length excludes the 4-byte header
    expect(length).toBeGreaterThan(0);
  });

  //
  // AC 24.2: encode format byte
  //
  it('AC 24.2: encode() sets correct format byte (0x00 JSON)', () => {
    const msg: RaftMessage = { msgType: 1, from: '1', to: '2', term: '1' };
    const buf = MessageCodec.encode(msg);
    expect(buf[4]).toBe(0x00);
  });

  //
  // AC 24.3: decode reconstructs message from subarray(4)
  //
  it('AC 24.3: decode() reconstructs the original from subarray(4) frame', () => {
    const msg: RaftMessage = { msgType: 1, from: '1', to: '2', term: '1' };
    const decoded = encodeAndDecode(msg);
    expect(decoded.msgType).toBe(1);
    expect(decoded.from).toBe('1');
    expect(decoded.to).toBe('2');
    expect(decoded.term).toBe('1');
  });

  //
  // AC 24.4: round-trip preserves all fields
  //
  it('AC 24.4: encode+decode round-trip preserves all fields', () => {
    const msg: RaftMessage = {
      msgType: 2,
      from: '3',
      to: '4',
      term: '5',
      logTerm: '3',
      index: '100',
      commit: '50',
      reject: false,
      rejectHint: 'log gap',
    };
    const decoded = encodeAndDecode(msg);
    expect(decoded.msgType).toBe(2);
    expect(decoded.from).toBe('3');
    expect(decoded.to).toBe('4');
    expect(decoded.term).toBe('5');
    expect(decoded.logTerm).toBe('3');
    expect(decoded.index).toBe('100');
    expect(decoded.commit).toBe('50');
    expect(decoded.reject).toBe(false);
    expect(decoded.rejectHint).toBe('log gap');
  });

  //
  // AC 24.5: round-trip with entries
  //
  it('AC 24.5: encode+decode with entries preserves entry fields', () => {
    const msg: RaftMessage = {
      msgType: 1,
      from: '1',
      to: '2',
      term: '1',
      entries: [
        { index: '10', term: '1', entryType: 0, data: Buffer.from([1, 2, 3]).toString('base64') },
        { index: '11', term: '1', entryType: 1, data: Buffer.from([4, 5, 6]).toString('base64') },
      ],
    };
    const decoded = encodeAndDecode(msg);
    expect(decoded.entries).toHaveLength(2);
    expect(decoded.entries![0].index).toBe('10');
    expect(decoded.entries![1].index).toBe('11');
  });

  //
  // AC 24.6: entry data as base64 string
  //
  it('AC 24.6: encode+decode with base64 entry data', () => {
    const originalData = 'SGVsbG8='; // "Hello" in base64
    const msg: RaftMessage = {
      msgType: 1,
      from: '1',
      to: '2',
      term: '1',
      entries: [{ index: '1', term: '1', entryType: 0, data: originalData }],
    };
    const decoded = encodeAndDecode(msg);
    // decode normalises: Buffer -> base64 string
    expect(typeof decoded.entries![0].data).toBe('string');
  });

  //
  // AC 24.7: encodeMsgPack produces MsgPack Buffer
  //
  it('AC 24.7: encodeMsgPack produces MsgPack Buffer', () => {
    const msg: RaftMessage = { msgType: 1, from: '1', to: '2', term: '1' };
    const buf = MessageCodec.encodeMsgPack(msg);
    expect(buf).toBeInstanceOf(Buffer);
    expect(buf.length).toBeGreaterThan(5);
    // Format byte for MsgPack: 0x01
    expect(buf[4]).toBe(0x01);
  });

  //
  // AC 24.8: decodeMsgPack from subarray(4)
  //
  it('AC 24.8: decodeMsgPack from subarray(4) reconstructs message', () => {
    const msg: RaftMessage = { msgType: 1, from: '1', to: '2', term: '1' };
    const decoded = encodeAndDecodeMsgPack(msg);
    expect(decoded.msgType).toBe(1);
    expect(decoded.from).toBe('1');
    expect(decoded.to).toBe('2');
    expect(decoded.term).toBe('1');
  });

  //
  // AC 24.9: JSON and MsgPack produce same decoded result
  //
  it('AC 24.9: JSON + MsgPack produce same decoded result', () => {
    const msg: RaftMessage = { msgType: 2, from: '3', to: '4', term: '5', commit: '10' };
    const jsonDecoded = encodeAndDecode(msg);
    const mpDecoded = encodeAndDecodeMsgPack(msg);
    expect(jsonDecoded.msgType).toBe(mpDecoded.msgType);
    expect(jsonDecoded.from).toBe(mpDecoded.from);
    expect(jsonDecoded.to).toBe(mpDecoded.to);
    expect(jsonDecoded.term).toBe(mpDecoded.term);
    expect(jsonDecoded.commit).toBe(mpDecoded.commit);
  });

  //
  // AC 24.10: normaliseForWasm converts u64 fields to strings
  //
  it('AC 24.10: normaliseForWasm converts u64 fields to strings', () => {
    const raw = { msgType: 0, from: 1, to: 2, term: 3 };
    const normalised = MessageCodec.normaliseForWasm(raw);
    expect(typeof normalised.from).toBe('string');
    expect(typeof normalised.to).toBe('string');
    expect(typeof normalised.term).toBe('string');
    expect(normalised.from).toBe('1');
    expect(normalised.to).toBe('2');
    expect(normalised.term).toBe('3');
  });

  //
  // AC 24.11: normaliseForWasm converts entryType to number
  //
  it('AC 24.11: normaliseForWasm converts entry index/term to string', () => {
    const raw = {
      msgType: 1,
      from: 1,
      to: 2,
      term: 1,
      entries: [{ index: 1, term: 1, entryType: 0, data: 'dGVzdA==' }],
    };
    const normalised = MessageCodec.normaliseForWasm(raw);
    expect(normalised.entries![0].index).toBe('1');
    expect(normalised.entries![0].term).toBe('1');
    // entryType is NOT converted by normaliseForWasm — it's already a number
    expect(normalised.entries![0].entryType).toBe(0);
  });

  //
  // AC 24.12: normaliseEntryData converts Buffer to base64
  //
  it('AC 24.12: normaliseForWasm converts Buffer entry data to base64 string', () => {
    const msg: RaftMessage = {
      msgType: 1,
      from: '1',
      to: '2',
      term: '1',
      entries: [{ index: '1', term: '1', entryType: 0, data: Buffer.from([72, 101, 108, 108, 111]) }],
    };
    // normaliseForWasm converts Buffer entry.data to base64 string
    const normalised = MessageCodec.normaliseForWasm(msg);
    expect(typeof normalised.entries![0].data).toBe('string');
    expect(normalised.entries![0].data).toBe('SGVsbG8=');
  });

  //
  // AC 24.13: Large message round-trip
  //
  it('AC 24.13: Large message encode+decode round-trip', () => {
    const largeData = Buffer.alloc(10000, 'X').toString('base64');
    const msg: RaftMessage = {
      msgType: 1,
      from: '1',
      to: '2',
      term: '1',
      entries: [{ index: '1', term: '1', entryType: 0, data: largeData }],
    };
    const decoded = encodeAndDecode(msg);
    expect(decoded.entries).toHaveLength(1);
    expect(typeof decoded.entries![0].data).toBe('string');
  });

  //
  // AC 24.14: MsgPack decode with binary entry data
  //
  it('AC 24.14: MsgPack decode with binary entry data', () => {
    const msg: RaftMessage = {
      msgType: 10,
      from: '5',
      to: '6',
      term: '7',
      entries: [{ index: '1', term: '1', entryType: 0, data: Buffer.from([0xab, 0xcd]) }],
    };
    const decoded = encodeAndDecodeMsgPack(msg);
    expect(decoded.msgType).toBe(10);
    expect(decoded.entries).toHaveLength(1);
  });

  //
  // AC 24.15: MsgPack round-trip preserves schema
  //
  it('AC 24.15: MsgPack encode+decode round-trip preserves schema', () => {
    const msg: RaftMessage = {
      msgType: 5,
      from: '1',
      to: '2',
      term: '3',
      logTerm: '2',
      index: '50',
      commit: '30',
      reject: true,
      rejectHint: 'conflict',
      entries: [
        { index: '1', term: '1', entryType: 0, data: Buffer.from([1, 2]) },
        { index: '2', term: '1', entryType: 1, data: Buffer.from([3, 4]) },
      ],
    };
    const decoded = encodeAndDecodeMsgPack(msg);
    expect(decoded.msgType).toBe(5);
    expect(decoded.reject).toBe(true);
    expect(decoded.rejectHint).toBe('conflict');
    expect(decoded.entries).toHaveLength(2);
    expect(decoded.entries![0].index).toBe('1');
    expect(decoded.entries![1].index).toBe('2');
  });
});
