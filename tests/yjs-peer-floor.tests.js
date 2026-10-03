/* eslint-env browser */

/**
 * Regression test: every yjs release the peer range admits must be able to
 * hydrate a doc that holds parked (pending) structs.
 *
 * Loading rows into a doc whose `store.pendingStructs` / `pendingDs` is set
 * queues the parked bytes, which Yjs keeps V2-encoded, through
 * `Y.convertUpdateFormatV2ToV1`. yjs defines that function internally from
 * 13.5.x on, but only EXPORTS it from 13.5.23 on: 13.5.0–13.5.22 have no
 * `Y.convertUpdateFormatV2ToV1` in their public namespace. With a peer range
 * of `^13.5.0`, npm installs this package next to such a release without a
 * warning. Hydration then throws before any stored row is applied, emits
 * 'error' and still emits 'synced': whenSynced resolves with none of the
 * persisted content, and every later trim fails the same way.
 *
 * Either the peer range excludes those releases, or the library works
 * without the export. The second case is checked by importing a private
 * copy of src/y-idb.js whose `yjs` is the very module instance the suite
 * uses, minus that one export — which is what the namespace of yjs
 * 13.5.0–13.5.22 looks like to the library.
 */

import * as Y from 'yjs'
import * as t from 'lib0/testing.js'
import { isNode } from 'lib0/environment.js'
import { clearDocument } from '../src/y-idb.js'

/**
 * The first yjs release whose public namespace exports
 * `Y.convertUpdateFormatV2ToV1` (13.5.22 defines it but does not export it).
 */
const FIRST_YJS_EXPORTING_V2_TO_V1 = [13, 5, 23]

/**
 * @param {Array<number>} a
 * @param {Array<number>} b
 * @return {number}
 */
const compareVersions = (a, b) => {
  for (let i = 0; i < 3; i++) {
    if (a[i] !== b[i]) return a[i] - b[i]
  }
  return 0
}

/**
 * Lower bound of a single (possibly partial / x-range) version, ignoring
 * any prerelease or build suffix.
 *
 * @param {string} version
 * @return {Array<number>}
 */
const parseLowerVersion = version => {
  const parts = version.split(/[-+]/)[0].split('.')
  return [0, 1, 2].map(i => {
    const n = Number.parseInt(parts[i], 10)
    return Number.isNaN(n) ? 0 : n
  })
}

/**
 * The smallest version an npm semver range admits, and whether that bound
 * is exclusive (`>x.y.z`). Handles `||`, space-separated comparator sets,
 * hyphen ranges, `^`, `~`, `>=`, `>`, `<`, `<=`, `=` and x-ranges.
 *
 * @param {string} range
 * @return {{ version: Array<number>, exclusive: boolean }}
 */
const minAdmittedVersion = range => {
  /** @type {{ version: Array<number>, exclusive: boolean } | null} */
  let min = null
  for (const alternative of range.split('||')) {
    const tokens = alternative.trim().replace(/([<>=~^]+)\s+/g, '$1').split(/\s+/).filter(s => s.length > 0)
    /** @type {{ version: Array<number>, exclusive: boolean }} */
    let lower = { version: [0, 0, 0], exclusive: false }
    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i]
      if (tokens[i + 1] === '-') {
        // hyphen range `a - b`: lower bound is `a`
        i += 2
      }
      const match = /^(<=|>=|<|>|=|\^|~>?|v)?v?(.*)$/.exec(token)
      const op = match ? match[1] || '' : ''
      const rest = match ? match[2] : token
      if (op === '<' || op === '<=' || rest === '*' || rest === '' || /^[xX*]/.test(rest)) {
        continue
      }
      const candidate = { version: parseLowerVersion(rest), exclusive: op === '>' }
      const cmp = compareVersions(candidate.version, lower.version)
      if (cmp > 0 || (cmp === 0 && candidate.exclusive)) {
        lower = candidate
      }
    }
    if (min === null) {
      min = lower
    } else {
      const cmp = compareVersions(lower.version, min.version)
      if (cmp < 0 || (cmp === 0 && !lower.exclusive)) {
        min = lower
      }
    }
  }
  return min || { version: [0, 0, 0], exclusive: false }
}

/**
 * Whether `range` admits a yjs release that does not export
 * `Y.convertUpdateFormatV2ToV1` (i.e. anything below 13.5.23).
 *
 * @param {string} range
 * @return {boolean}
 */
const admitsYjsWithoutV2ToV1 = range => {
  const { version, exclusive } = minAdmittedVersion(range)
  // `>13.5.22` admits 13.5.23 and up only.
  const lowest = exclusive ? [version[0], version[1], version[2] + 1] : version
  return compareVersions(lowest, FIRST_YJS_EXPORTING_V2_TO_V1) < 0
}

/**
 * `import.meta` of this module (tsconfig's pre-ES2020 module kind does not
 * type it).
 *
 * @type {{ url: string, resolve: (specifier: string) => string }}
 */
// @ts-ignore
const meta = import.meta

/**
 * @return {Promise<string>} peerDependencies.yjs of this package
 */
const readYjsPeerRange = async () => {
  // `import()` keeps the browser test bundle free of node builtins.
  // @ts-ignore
  const fs = /** @type {typeof import('fs')} */ (await import('fs'))
  // @ts-ignore
  const url = /** @type {typeof import('url')} */ (await import('url'))
  const pkgPath = url.fileURLToPath(new URL('../package.json', meta.url))
  const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'))
  t.assert(pkg.name === 'y-idb', `read the wrong package.json: ${pkgPath}`)
  const peerRange = pkg.peerDependencies && pkg.peerDependencies.yjs
  t.assert(typeof peerRange === 'string', 'yjs must be declared as a peer dependency')
  return peerRange
}

/**
 * Import a private copy of src/y-idb.js that sees the suite's own yjs module
 * instance with the `hidden` exports removed from its namespace. Every other
 * import of the copy resolves to the same module instance src/y-idb.js uses.
 *
 * @param {Array<string>} hidden
 * @return {Promise<typeof import('../src/y-idb.js')>}
 */
const importLibraryWithYjsLacking = async hidden => {
  // @ts-ignore
  const fs = /** @type {typeof import('fs')} */ (await import('fs'))
  // @ts-ignore
  const url = /** @type {typeof import('url')} */ (await import('url'))
  const names = Object.keys(Y).filter(name => !hidden.includes(name))
  hidden.forEach(name => t.assert(name in Y, `the suite's yjs exports ${name}`))
  const shim = `import * as Y from ${JSON.stringify(meta.resolve('yjs'))}\nexport const { ${names.join(', ')} } = Y\n`
  const shimUrl = 'data:text/javascript,' + encodeURIComponent(shim)
  const srcUrl = new URL('../src/y-idb.js', meta.url)
  let rewroteYjs = false
  const source = fs.readFileSync(url.fileURLToPath(srcUrl), 'utf8').replace(
    /^(import\s[^'\n]*\sfrom\s+)'([^']+)'/gm,
    /**
     * @param {string} _match
     * @param {string} head
     * @param {string} specifier
     */
    (_match, head, specifier) => {
      if (specifier === 'yjs') {
        rewroteYjs = true
        return head + JSON.stringify(shimUrl)
      }
      const resolved = specifier.startsWith('.') ? new URL(specifier, srcUrl).href : meta.resolve(specifier)
      return head + JSON.stringify(resolved)
    }
  )
  t.assert(rewroteYjs, 'src/y-idb.js imports yjs')
  // @ts-ignore
  return import('data:text/javascript,' + encodeURIComponent(source))
}

/**
 * With yjs 13.5.0–13.5.22 (no `Y.convertUpdateFormatV2ToV1` export), a
 * network provider applies a remote edit (' world') whose dependency
 * ('hello') exists only in IndexedDB, before y-idb hydrates the doc. Yjs
 * parks the edit in pendingStructs. Hydration must load the stored rows,
 * which integrates the parked edit, without an 'error', and keep both
 * across a reload — exactly as it does on yjs >= 13.5.23. Unless the peer
 * range keeps those releases out.
 *
 * @param {t.TestCase} tc
 */
export const testParkedStructsHydrateOnEveryAdmittedYjsRelease = async tc => {
  // package.json is only readable from the node runner
  t.skip(!isNode)
  // Sanity check of the range helper.
  t.assert(admitsYjsWithoutV2ToV1('^13.5.0'))
  t.assert(admitsYjsWithoutV2ToV1('^13.5.22'))
  t.assert(admitsYjsWithoutV2ToV1('>=13.5.0 <14.0.0'))
  t.assert(admitsYjsWithoutV2ToV1('^13.6.0 || ^13.5.10'))
  t.assert(admitsYjsWithoutV2ToV1('>13.5.21'))
  t.assert(!admitsYjsWithoutV2ToV1('>13.5.22'))
  t.assert(!admitsYjsWithoutV2ToV1('^13.5.23'))
  t.assert(!admitsYjsWithoutV2ToV1('>=13.5.23 <14.0.0'))
  t.assert(!admitsYjsWithoutV2ToV1('^13.6.0 || ^14.0.0'))
  // The yjs the suite runs against has the export the library depends on.
  t.assert(typeof Y.convertUpdateFormatV2ToV1 === 'function')

  const peerRange = await readYjsPeerRange()
  if (!admitsYjsWithoutV2ToV1(peerRange)) {
    // Releases without the export cannot be installed as the peer.
    return
  }

  // What the library sees on yjs 13.5.0–13.5.22.
  const lib = await importLibraryWithYjsLacking(['convertUpdateFormatV2ToV1'])
  const name = tc.testName
  await clearDocument(name)

  // Earlier session: 'hello' is persisted.
  const docA = new Y.Doc()
  const pA = new lib.IndexeddbPersistence(name, docA)
  await pA.whenSynced
  docA.getText('t').insert(0, 'hello')
  await pA.flush()
  await pA.destroy()

  // A remote peer that has 'hello' appends ' world'.
  const svBefore = Y.encodeStateVector(docA)
  const remote = new Y.Doc()
  Y.applyUpdate(remote, Y.encodeStateAsUpdate(docA))
  remote.getText('t').insert(5, ' world')
  const diff = Y.encodeStateAsUpdate(remote, svBefore)
  remote.destroy()
  docA.destroy()

  // This session: the network delivers ' world' before y-idb hydrates.
  const docB = new Y.Doc()
  Y.applyUpdate(docB, diff, 'remote')
  t.assert(docB.store.pendingStructs !== null, 'the remote edit is parked until "hello" is loaded')

  const pB = new lib.IndexeddbPersistence(name, docB)
  /** @type {Array<string>} */
  const errors = []
  pB.on('error', /** @param {Array<any>} args */ args => {
    const err = Array.isArray(args) ? args[0] : args
    errors.push(String(err && err.message ? err.message : err))
  })
  await pB.whenSynced
  const hydrated = docB.getText('t').toString()
  const hydrationErrors = errors.slice()
  await pB.flush()
  await pB.destroy()
  docB.destroy()

  // Reload.
  const docR = new Y.Doc()
  const pR = new lib.IndexeddbPersistence(name, docR)
  await pR.whenSynced
  const reloaded = docR.getText('t').toString()
  await pR.destroy()
  docR.destroy()
  await clearDocument(name)

  t.assert(
    hydrationErrors.length === 0 && hydrated === 'hello world' && reloaded === 'hello world',
    `peerDependencies.yjs is "${peerRange}", which admits yjs releases older than ${FIRST_YJS_EXPORTING_V2_TO_V1.join('.')} that do not export Y.convertUpdateFormatV2ToV1. ` +
    `With that export missing, hydrating a doc with parked structs emitted 'error' ${JSON.stringify(hydrationErrors)} (expected none), ` +
    `whenSynced resolved with text ${JSON.stringify(hydrated)} and a reload shows ${JSON.stringify(reloaded)} (expected "hello world" both times)`
  )
}
