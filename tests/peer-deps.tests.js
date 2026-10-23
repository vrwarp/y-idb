/* eslint-env browser */

import * as Y from 'yjs'
import * as t from 'lib0/testing.js'
import { isNode } from 'lib0/environment.js'
import { compareVersions, minAdmittedVersion } from './semver-range.js'

/**
 * `Y.mergeUpdates` first shipped in yjs 13.5.0 (13.4.14 is the last release
 * without it). readSnapshot merges multi-row databases with it and the
 * incremental (tiered) trim folds tail rows with it, so every yjs version the
 * package admits as a peer must be at least this one.
 */
const FIRST_YJS_WITH_MERGE_UPDATES = [13, 5, 0]

/**
 * @param {string} range
 * @return {boolean}
 */
const rangeRequiresMergeUpdates = range => {
  const { version } = minAdmittedVersion(range)
  return compareVersions(version, FIRST_YJS_WITH_MERGE_UPDATES) >= 0
}

/**
 * The range helper decides which peer ranges the package tests accept, so
 * pin its lower-bound arithmetic (also used by yjs-peer-floor.tests.js).
 *
 * @param {t.TestCase} _tc
 */
export const testMinAdmittedVersion = async _tc => {
  t.compare(minAdmittedVersion('^13.5.0'), { version: [13, 5, 0], exclusive: false })
  t.compare(minAdmittedVersion('>13.4.0'), { version: [13, 4, 0], exclusive: true })
  t.compare(minAdmittedVersion('>=13.5.0 <14.0.0'), { version: [13, 5, 0], exclusive: false })
  t.compare(minAdmittedVersion('13.5.0 - 13.99.0'), { version: [13, 5, 0], exclusive: false })
  t.compare(minAdmittedVersion('13.x'), { version: [13, 0, 0], exclusive: false })
  t.compare(minAdmittedVersion('*'), { version: [0, 0, 0], exclusive: false })
  // The lowest alternative wins; at equal versions the inclusive bound does.
  t.compare(minAdmittedVersion('^13.6.0 || ^13.5.10'), { version: [13, 5, 10], exclusive: false })
  t.compare(minAdmittedVersion('>13.5.0 || ^13.5.0'), { version: [13, 5, 0], exclusive: false })
  t.assert(compareVersions([13, 5, 0], [13, 4, 14]) > 0)
  t.assert(compareVersions([13, 5, 0], [14, 0, 0]) < 0)
  t.assert(compareVersions([13, 5, 0], [13, 5, 0]) === 0)
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
