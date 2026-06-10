// Generates dist/y-idb.d.cts from the tsc-emitted ESM declarations so that
// TypeScript consumers using node16/nodenext resolution get matching types
// for the CommonJS entry point (a .d.ts in a "type": "module" package would
// be interpreted as ESM-only). The sourceMappingURL is stripped because the
// declaration map is not copied along.
import { readFileSync, writeFileSync } from 'node:fs'

const dts = readFileSync('dist/src/y-idb.d.ts', 'utf8').replace(/^\/\/# sourceMappingURL=.*$/m, '')
writeFileSync('dist/y-idb.d.cts', dts)
