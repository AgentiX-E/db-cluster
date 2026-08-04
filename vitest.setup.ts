// Forward pino output to /dev/null during tests
process.env.LOG_LEVEL = 'silent';
process.env.VITEST = 'true';
