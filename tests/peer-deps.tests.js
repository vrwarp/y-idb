/* eslint-env browser */

import * as Y from 'yjs'
import * as t from 'lib0/testing.js'
import { isNode } from 'lib0/environment.js'

/**
 * `Y.mergeUpdates` first shipped in yjs 13.5.0 (13.4.14 is the last release
 * without it). readSnapshot merges multi-row databases with it and the
 * incremental (tiered) trim folds tail rows with it, so every yjs version the
 * package admits as a peer must be at least this one.
 */
const FIRST_YJS_WITH_MERGE_UPDATES = [13, 5, 0]

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
 * Lower bound of a single version (partial versions and x-ranges included:
 * `13`, `13.x`, `13.5`, `13.5.*`), ignoring any prerelease/build suffix.
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
 * The smallest version a semver range admits, together with whether that
 * bound is exclusive (`>x.y.z`). Supports the npm range grammar used in
 * package.json files: `||` alternatives, space-separated comparator sets,
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
 * @param {string} range
 * @return {boolean}
 */
const rangeRequiresMergeUpdates = range => {
  const { version } = minAdmittedVersion(range)
  return compareVersions(version, FIRST_YJS_WITH_MERGE_UPDATES) >= 0
}

/**
 * The yjs peer range must not admit releases that lack `Y.mergeUpdates`.
 *
 * On yjs 13.0–13.4 the namespace import has no `mergeUpdates`, so readSnapshot
 * of any database with more than one row throws inside `tx.oncomplete` and
 * never settles, and every incremental trim silently degrades into a full
 * O(document) consolidation. A peer range of `^13.0.0` lets an app pinned to
 * yjs 13.4.x install this package without any npm warning.
 *
 * @param {t.TestCase} _tc
 */
export const testYjsPeerRangeExcludesReleasesWithoutMergeUpdates = async _tc => {
  // package.json is only readable from the node runner
  t.skip(!isNode)
  // Sanity check of the self-written range helper.
  t.assert(!rangeRequiresMergeUpdates('^13.0.0'))
  t.assert(!rangeRequiresMergeUpdates('^13.4.7'))
  t.assert(!rangeRequiresMergeUpdates('>13.4.0'))
  t.assert(!rangeRequiresMergeUpdates('^13.5.0 || ^13.1.0'))
  t.assert(rangeRequiresMergeUpdates('^13.5.0'))
  t.assert(rangeRequiresMergeUpdates('>=13.5.0 <14.0.0'))
  t.assert(rangeRequiresMergeUpdates('^13.6.0 || ^14.0.0'))
  t.assert(rangeRequiresMergeUpdates('13.5.0 - 13.99.0'))
  // The yjs the suite runs against has the API the library depends on.
  t.assert(typeof Y.mergeUpdates === 'function')

  // The runner entry point is tests/node.js, so package.json is one level up.
  // `import()` (not a static import) keeps the browser test bundle free of
  // node builtins; the ts-ignore covers tsconfig's pre-ES2020 module kind.
  // @ts-ignore
  const fs = /** @type {typeof import('fs')} */ (await import('fs'))
  // @ts-ignore
  const path = /** @type {typeof import('path')} */ (await import('path'))
  const pkgPath = path.join(path.dirname(path.resolve(process.argv[1])), '..', 'package.json')
  const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'))
  t.assert(pkg.name === 'y-idb', `read the wrong package.json: ${pkgPath}`)
  const peerRange = pkg.peerDependencies && pkg.peerDependencies.yjs
  t.assert(typeof peerRange === 'string', 'yjs must be declared as a peer dependency')
  t.assert(
    rangeRequiresMergeUpdates(peerRange),
    `peerDependencies.yjs is "${peerRange}", which admits yjs releases older than ${FIRST_YJS_WITH_MERGE_UPDATES.join('.')} that lack Y.mergeUpdates (needed by readSnapshot and the tiered trim)`
  )
}
