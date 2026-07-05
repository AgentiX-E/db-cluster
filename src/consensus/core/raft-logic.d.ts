declare module 'raft-logic' {
  export interface RaftNodeConfig {
    id: string | number;
    peers: (string | number)[];
    electionTick: number;
    heartbeatTick: number;
    storage: any;
    transport?: any;
    tickInterval?: number;
    tickIntervalMs?: number;
    preVote?: boolean;
    checkQuorum?: boolean;
    tag?: string;
    apply?: (data: string, index?: number, term?: number) => void;
  }

  export class RaftNode {
    constructor(config: RaftNodeConfig);
    start(): void;
    stop(): void;
    tick(): void;
    step(msg: any): Promise<void>;
    propose(data: string): void;
    campaign(): void;
    status(): { id: number; term: string; commit: string; applied: string; leader: string; raftState: number };
    on(type: string, handler: (...args: any[]) => void): void;
  }

  export class SqliteStorage {
    constructor(config: { file: string });
    open(): void;
    close(): void;
  }

  export class InMemoryStorage {
    constructor();
  }

  export class InMemoryTransport {
    constructor();
  }

  export var INITIAL_TERM: number;
}
