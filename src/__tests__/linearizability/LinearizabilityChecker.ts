/**
 * LinearizabilityChecker — Register-model linearizability verification.
 *
 * D2 iteration: Jepsen-style linearizability verification for Raft cluster.
 *
 * Algorithm:
 *   WG (Wing-Gong) linearizability checking with pruning.
 *   Given a history of concurrent operations (call/return intervals),
 *   find a legal sequential ordering that respects real-time order.
 *
 * Model: Single Register (read/write)
 *   read(k) → v    (returns the last written value for key k)
 *   write(k, v)    (sets key k to value v)
 *
 * Usage:
 *   const history: Operation[] = [...];
 *   const result = LinearizabilityChecker.isLinearizable(history);
 *   // result === true if a legal sequentialization exists
 */

// ── Types ───────────────────────────────────────────────────

export interface CallEvent {
  type: 'call';
  clientId: string;
  operation: 'read' | 'write';
  key: string;
  value?: string;
  time: number; // monotonic timestamp
}

export interface ReturnEvent {
  type: 'return';
  clientId: string;
  operation: 'read' | 'write';
  key: string;
  value?: string; // read result
  time: number;
}

export type Event = CallEvent | ReturnEvent;

export interface Operation {
  clientId: string;
  call: CallEvent;
  ret: ReturnEvent;
}

export interface RegisterState {
  [key: string]: string | undefined;
}

// ── Linearizability Checker ─────────────────────────────────

export class LinearizabilityChecker {
  /**
   * Check if a history of register operations is linearizable.
   *
   * Uses a bounded search: if the state space exceeds maxStates,
   * returns 'unknown' instead of false (conservative but avoids
   * exponential blowup for long histories).
   *
   * @param events — Flat array of call/return events
   * @param maxStates — Maximum number of states to explore (default 1_000_000)
   * @returns true if linearizable, false if violation found, 'unknown' if search bound exceeded
   */
  static isLinearizable(
    events: Event[],
    maxStates: number = 1_000_000,
  ): boolean | 'unknown' {
    // Step 1: Pair calls with returns
    const operations = LinearizabilityChecker.pairEvents(events);

    // Step 2: Build precedence graph
    // op_i must precede op_j if op_i.return < op_j.call (real-time order)
    const n = operations.length;
    if (n === 0) return true;

    // Step 3: Find a legal sequential ordering via DFS with pruning
    const visited = new Set<string>();
    const used = new Array(n).fill(false);
    let stateCount = 0;

    function dfs(
      seq: Operation[],
      state: RegisterState,
    ): boolean {
      if (stateCount > maxStates) return false; // bound exceeded
      stateCount++;

      if (seq.length === n) return true; // complete ordering found

      // Try each unused operation that is "ready" (all predecessors used)
      for (let i = 0; i < n; i++) {
        if (used[i]) continue;

        // Check real-time precedence: all operations that returned before
        // this one's call must already be in seq
        const op = operations[i];
        let allPredecessorsUsed = true;
        for (let j = 0; j < n; j++) {
          if (i === j || used[j]) continue;
          // j must precede i if j's return is before i's call
          if (operations[j].ret.time < op.call.time) {
            allPredecessorsUsed = false;
            break;
          }
        }
        if (!allPredecessorsUsed) continue;

        // Try this operation — check if it's legal in current state
        used[i] = true;

        let legal = false;
        let newState: RegisterState = { ...state };

        if (op.call.operation === 'write') {
          // write always legal
          newState[op.call.key] = op.call.value;
          legal = true;
        } else if (op.call.operation === 'read') {
          // read is legal if the returned value matches state
          const expected = state[op.call.key];
          const actual = op.ret.value;
          if (actual === expected || (expected === undefined && actual === null)) {
            legal = true;
            // read doesn't change state
          }
        }

        if (legal) {
          seq.push(op);
          if (dfs(seq, newState)) return true;
          seq.pop();
        }

        used[i] = false;
      }

      return false;
    }

    const result = dfs([], {});
    if (!result && stateCount > maxStates) return 'unknown';
    return result;
  }

  /**
   * Pair call/return events into Operation objects.
   * Throws if events are malformed (unbalanced or mismatched).
   */
  static pairEvents(events: Event[]): Operation[] {
    const byClient = new Map<string, Event[]>();

    for (const e of events) {
      const list = byClient.get(e.clientId) ?? [];
      list.push(e);
      byClient.set(e.clientId, list);
    }

    const operations: Operation[] = [];
    for (const [clientId, clientEvents] of byClient) {
      // Sort by time
      clientEvents.sort((a, b) => a.time - b.time);

      for (let i = 0; i < clientEvents.length; i += 2) {
        if (i + 1 >= clientEvents.length) {
          throw new Error(`Unpaired event for client ${clientId}: missing return`);
        }
        const call = clientEvents[i];
        const ret = clientEvents[i + 1];
        if (call.type !== 'call' || ret.type !== 'return') {
          throw new Error(`Malformed event pair for client ${clientId}`);
        }
        if (call.operation !== ret.operation) {
          throw new Error(`Mismatched operation for client ${clientId}: ${call.operation} vs ${ret.operation}`);
        }
        operations.push({ clientId, call, ret });
      }
    }

    // Sort by return time for deterministic ordering
    operations.sort((a, b) => a.ret.time - b.ret.time);
    return operations;
  }

  /**
   * Generate a complete history from a list of operations by simulating
   * the system. Each operation gets a call time and return time.
   * For testing purposes.
   */
  static generateHistory(
    ops: Array<{ clientId: string; operation: 'read' | 'write'; key: string; value?: string; result?: string }>,
    startTime: number = 0,
    latencyMs: number = 10,
  ): Event[] {
    const events: Event[] = [];
    let t = startTime;

    for (const op of ops) {
      const callTime = t;
      const returnTime = t + latencyMs + Math.random() * 5;

      events.push({
        type: 'call',
        clientId: op.clientId,
        operation: op.operation,
        key: op.key,
        value: op.operation === 'write' ? op.value : undefined,
        time: callTime,
      });

      events.push({
        type: 'return',
        clientId: op.clientId,
        operation: op.operation,
        key: op.key,
        value: op.result,
        time: returnTime,
      });

      t += latencyMs / 2 + Math.random() * 3;
    }

    return events;
  }
}

// ── Operation Recorder (for chaos test integration) ─────────

export class OperationRecorder {
  private events: Event[] = [];
  private pendingCalls = new Map<string, number>();
  private callSeq = 0;

  /**
   * Record a call event in real time.
   * Returns a unique callId that must be passed to recordReturn.
   */
  recordCall(
    clientId: string,
    operation: 'read' | 'write',
    key: string,
    value?: string,
  ): string {
    const callId = `${clientId}:${this.callSeq++}`;
    const now = Date.now();

    this.events.push({
      type: 'call',
      clientId,
      operation,
      key,
      value,
      time: now,
    });

    this.pendingCalls.set(callId, this.events.length - 1);
    return callId;
  }

  /**
   * Record a return event matching a prior call.
   */
  recordReturn(
    callId: string,
    operation: 'read' | 'write',
    key: string,
    value?: string,
  ): void {
    const now = Date.now();
    this.events.push({
      type: 'return',
      clientId: callId.split(':')[0],
      operation,
      key,
      value,
      time: now,
    });
  }

  getEvents(): Event[] {
    return [...this.events];
  }

  /**
   * Check if the recorded history is linearizable.
   */
  isLinearizable(maxStates?: number): boolean | 'unknown' {
    return LinearizabilityChecker.isLinearizable(this.events, maxStates);
  }

  reset(): void {
    this.events = [];
    this.pendingCalls.clear();
    this.callSeq = 0;
  }
}
