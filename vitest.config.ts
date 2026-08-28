import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    setupFiles: ['./vitest.setup.ts'],
    silent: true, // suppress console noise from Raft pino logs in fork workers
    include: ['src/**/*.test.ts'],
    // Exclude the heaviest integration tests for CI speed;
    // they can be run in a separate suite.
    exclude: [
      'src/__tests__/stress/**',
    ],
    testTimeout: 120_000,
    hookTimeout: 60_000,
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: false,
      },
    },
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      exclude: [
        'src/**/*.test.ts',
        'src/**/__tests__/**',
        'src/**/*.d.ts',
        'src/index.ts',
        'src/interfaces/**',
        'src/consensus/interfaces/**',
        'src/types/**',
        // Debug / diagnostic files
        'src/__tests__/debug-*.ts',
        // Performance benchmarks (not unit-tested)
        'src/__tests__/performance/**',
        'src/__tests__/stress/**',
      ],
      reporter: ['text', 'json-summary', 'lcov'],
      thresholds: {
        lines: 85,
        branches: 80,
        functions: 90,
        statements: 85,
      },
    },
  },
  resolve: {
    alias: {
      '@agentix-e/db-cluster': path.resolve(__dirname, './src'),
      '@agentix-e/db-operations-chaosengine': path.resolve(
        __dirname,
        '../db-operations/packages/chaosengine/src',
      ),
    },
  },
});
