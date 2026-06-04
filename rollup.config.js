import resolve from '@rollup/plugin-node-resolve'
import commonjs from '@rollup/plugin-commonjs'

export default [{
  input: './tests/index.js',
  output: {
    file: './dist/test.js',
    format: 'iife',
    sourcemap: true
  },
  plugins: [
    resolve({ mainFields: ['module', 'browser', 'main'] }),
    commonjs()
  ]
}, {
  input: './src/y-idb.js',
  output: {
    name: 'Y',
    file: 'dist/y-idb.cjs',
    format: 'cjs',
    sourcemap: true
  },
  external: id => /^(lib0|yjs)\//.test(id)
}]
