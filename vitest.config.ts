import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/**/*.test.ts'],
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
      include: ['src/interfaces/**/*.ts'],
    },
  },
  resolve: {
    alias: {
      '@agentix-e/db-cluster': path.resolve(__dirname, './src'),
    },
  },
});

// Increase testTimeout for the stress test file specifically
// via retry base to handle parallel resource contention
