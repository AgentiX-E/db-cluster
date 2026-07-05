import { IDeadlockDetector } from '../../interfaces/IDeadlockDetector';

export class DeadlockDetector implements IDeadlockDetector {
  private wfg: Map<number, Set<string>> = new Map();
  private holds: Map<number, Set<string>> = new Map();

  async start(): Promise<void> {
    // no-op stub
  }

  async stop(): Promise<void> {
    // no-op stub
  }

  detectDeadlocks(
    holds: Map<number, Set<string>>,
    waits: Map<number, Set<number>>
  ): number | null {
    return null as any;
  }

  wouldCauseDeadlock(
    sessionId: number,
    resourceId: string,
    holds: Map<number, Set<string>>,
    waits: Map<number, Set<number>>
  ): boolean {
    return false;
  }

  releaseSession(sessionId: number): void {
    this.holds.delete(sessionId);
    this.wfg.delete(sessionId);
  }

  reportHold(sessionId: number, resourceId: string): void {
    if (!this.holds.has(sessionId)) this.holds.set(sessionId, new Set());
    this.holds.get(sessionId)!.add(resourceId);
  }

  onDeadlock(callback: (info: { cycle: number[]; victimTransaction: number; detectedAt: number }) => void): () => void {
    return () => {};
  }

  getDeadlockHistory(): Array<{ cycle: number[]; victimTransaction: number; detectedAt: number }> {
    return [];
  }
}
