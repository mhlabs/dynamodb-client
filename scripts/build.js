const { build } = require('esbuild');

const { dependencies } = require('../package.json');

const sharedConfig = {
  entryPoints: ['src/index.ts'],
  bundle: true,
  minify: true,
  sourcemap: true,
  external: Object.keys(dependencies)
};

build({
  ...sharedConfig,
  platform: 'node',
  outfile: 'dist/index.cjs.js',
  format: 'cjs'
});

build({
  ...sharedConfig,
  outfile: 'dist/index.js',
  platform: 'neutral',
  format: 'esm'
});
