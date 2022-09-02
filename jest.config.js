/*
 * For a detailed explanation regarding each configuration property and type check, visit:
 * https://jestjs.io/docs/configuration
 */

/** @type {import('@jest/types').Config.InitialOptions} */
const config = {
  transform: {
    '^.+\\.(ts|tsx)$': ['esbuild-jest', { sourcemap: true }]
  },
  clearMocks: true,
  testMatch: ['**/*.test.ts'],
  collectCoverage: false,
  coverageDirectory: 'coverage',
  coverageReporters: ['json-summary', 'lcov'],
  coverageProvider: 'v8',
  collectCoverageFrom: ['src/**/*.ts', '!src/**/types.ts'],
  setupFilesAfterEnv: ['./jest-set-env.js'],
  testEnvironment: 'node'
};

module.exports = config;
