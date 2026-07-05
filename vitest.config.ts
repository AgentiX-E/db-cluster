import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/**/*.test.ts'],
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
