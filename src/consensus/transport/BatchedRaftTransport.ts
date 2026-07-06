/**
 * BatchedRaftTransport — 适配 @maboke123/raft-core 的 Transport 接口
 *
 * 将 RaftTcpTransport（消息路由基于 msg.to）包装为请求-响应模式。
 *
 * 在 Wire 层加 requestId 匹配请求↔响应。消息序列化为 JSON Buffer，
 * 使用 RaftTcpTransport.sendToPeer() 直接发送到目标 peer。
 */

import { RaftTcpTransport, RaftTcpTransportConfig } from './RaftTcpTransport';
import type { Transport, MessageHandler, RPCMessage } from '@maboke123/raft-core';

let _reqIdCounter = 0;
function nextRequestId(): string {
  return `${process.pid}-${++_reqIdCounter}`;
}

interface PendingEntry {
  resolve: (msg: RPCMessage) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

const HEADER_SIZE = 5; // 4-bit length + 1-bit format

/**
 * 构建 MessageCodec 兼容的 wire frame（JSON format）。
 * [4字节长度][1字节format=0x00][JSON payload]
 */
function encodeWireBuffer(payload: Record<string, unknown>): Buffer {
  const json = JSON.stringify(payload);
  const body = Buffer.from(json, 'utf-8');
  const header = Buffer.alloc(HEADER_SIZE);
  header.writeUInt32BE(body.length + 1, 0); // +1 for format byte
  header[4] = 0x00; // FORMAT_JSON
  return Buffer.concat([header, body]);
}

export class BatchedRaftTransport implements Transport {
  private transport: RaftTcpTransport;
  private handler: MessageHandler | null = null;
  private pendingMap = new Map<string, PendingEntry>();
  private started = false;
  readonly requestTimeoutMs: number;

  constructor(config: RaftTcpTransportConfig, requestTimeoutMs = 15000) {
    this.transport = new RaftTcpTransport(config);
    this.requestTimeoutMs = requestTimeoutMs;
  }

  get underlying(): RaftTcpTransport { return this.transport; }

  async addPeer?(_peerId: string, _address: string): Promise<void> {}
  removePeer?(_peerId: string): void {}

  async start(): Promise<void> {
    if (this.started) return;
    this.transport.on('message', (msg: any, fromPeerId: string) => {
      this.onIncoming(msg, fromPeerId);
    });
    await this.transport.start();
    this.started = true;
  }

  async stop(): Promise<void> {
    this.started = false;
    for (const [, entry] of this.pendingMap) {
      clearTimeout(entry.timer);
      entry.reject(new Error('Transport stopped'));
    }
    this.pendingMap.clear();
    await this.transport.stop();
  }

  isStarted(): boolean { return this.started; }

  onMessage(handler: MessageHandler): void {
    this.handler = handler;
  }

  /** 发送 RPC 请求并等待响应。 */
  async send(peerId: string, message: RPCMessage): Promise<RPCMessage> {
    const requestId = nextRequestId();
    const peerNumId = parseInt(peerId, 10);
    if (isNaN(peerNumId)) throw new Error(`Invalid peer id: ${peerId}`);

    return new Promise<RPCMessage>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingMap.delete(requestId);
        reject(new Error(`Raft transport request timeout: ${message.type} to ${peerId}`));
      }, this.requestTimeoutMs);

      this.pendingMap.set(requestId, { resolve, reject, timer });

      // Build wire frame: { requestId, msg }
      const frame = encodeWireBuffer({ requestId, msg: message as any });
      this.transport.sendToPeer(peerNumId, frame);
    });
  }

  async waitForConnections(): Promise<void> {
    await this.transport.waitForConnections();
  }

  /** 处理入站消息 */
  private onIncoming(wireMsg: any, fromPeerId: string): void {
    if (!this.started) return;

    const requestId = wireMsg?.requestId;
    const inner = requestId ? wireMsg.msg : wireMsg;

    // 响应方向消息：匹配 pending 请求
    if (requestId && inner?.direction === 'response') {
      const pending = this.pendingMap.get(requestId);
      if (pending) {
        clearTimeout(pending.timer);
        this.pendingMap.delete(requestId);
        pending.resolve(inner);
      }
      return;
    }

    // 请求方向消息：传给 handler
    if (!this.handler) return;

    this.handler(fromPeerId, inner).then((response: RPCMessage) => {
      if (!response || !this.started) return;

      const respRequestId = requestId || nextRequestId();
      const respPeerNumId = parseInt(fromPeerId, 10);
      if (isNaN(respPeerNumId)) return;

      const frame = encodeWireBuffer({ requestId: respRequestId, msg: response as any });
      this.transport.sendToPeer(respPeerNumId, frame);
    }).catch(() => {});
  }
}
