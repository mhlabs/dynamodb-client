import { configDefaults, defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    ...configDefaults,
    globals: true,
    clearMocks: true,
    coverage: {
      reporter: ['lcov', 'json', 'json-summary'],
      provider: 'c8',
    },
  },
});
