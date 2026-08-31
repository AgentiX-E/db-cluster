// Forward pino output to /dev/null during tests
process.env.LOG_LEVEL = 'silent';
process.env.VITEST = 'true';

// Materialize a `pg` package stub under node_modules so the production
// `require('pg')` (inside RaftGroupManager._execOnLeader auto-forward) resolves
// in the test environment. `pg` is an optional runtime dependency not installed
// here; this stub is created once per worker (setupFiles run per fork) and is
// idempotent, so parallel test files never race on it. node_modules is
// gitignored, so this is non-invasive.
// The stub reads per-test behaviour from globalThis.__pgActions dynamically.
import * as fs from 'fs';
import * as path from 'path';

const pgStubDir = path.resolve(process.cwd(), 'node_modules/pg');
if (!fs.existsSync(pgStubDir)) {
  fs.mkdirSync(pgStubDir, { recursive: true });
  fs.writeFileSync(
    path.join(pgStubDir, 'package.json'),
    JSON.stringify({ name: 'pg', version: '0.0.0-test-stub', main: 'index.js' }),
  );
  fs.writeFileSync(
    path.join(pgStubDir, 'index.js'),
    `
class Client {
  constructor(cfg) {
    this.cfg = cfg;
    const a = globalThis.__pgActions || {};
    if (a.onCreate) a.onCreate(cfg);
  }
  on(ev, fn) { const a = globalThis.__pgActions || {}; if (a.onEvent) a.onEvent(ev, fn); }
  connect() { const a = globalThis.__pgActions || {}; return a.onConnect ? a.onConnect(this.cfg) : Promise.resolve(); }
  query(sql) { const a = globalThis.__pgActions || {}; return a.onQuery ? a.onQuery(sql, this.cfg) : Promise.resolve({ rows: [] }); }
  end() { const a = globalThis.__pgActions || {}; return a.onEnd ? a.onEnd(this.cfg) : Promise.resolve(); }
}
module.exports = { Client };
`,
  );
}
