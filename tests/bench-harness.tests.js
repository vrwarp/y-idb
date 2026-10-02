/* eslint-env browser */

import * as t from 'lib0/testing.js'
import { isNode } from 'lib0/environment.js'

/**
 * Bench-harness regression: `npm run bench` (benchmarks/aging.mjs) must not
 * let fake-indexeddb keep every finished IDBTransaction alive.
 *
 * fake-indexeddb 6.x pushes each transaction onto its internal Database's
 * `transactions` list and only ever filters that list for scheduling; a
 * finished transaction still references its oncomplete/onerror closures, so
 * every session's IndexeddbPersistence and Y.Doc stay reachable for the whole
 * run. Browsers release finished transactions. Over the default 240-session
 * run the retained heap reaches ~6.4 GB and the major-GC pauses land inside
 * the timed hydration/readSnapshot samples, so the bench charges the library
 * for a harness artifact.
 *
 * The bench reports the retained count as `harnessTx` per epoch (read from
 * fake-indexeddb's internal Database); this pins it with a deterministic
 * counter. The workload is seeded, so the count is exact: about 62 retained
 * transactions per session while the harness leaks, a handful (the last
 * transactions, before fake-indexeddb's next scheduling pass) once it does
 * not.
 *
 * @param {t.TestCase} _tc
 */
export const testAgingBenchDoesNotRetainFinishedTransactions = async _tc => {
  // spawns the node benchmark
  t.skip(!isNode)
  // `import()` (not a static import) keeps the browser test bundle free of
  // node builtins; the ts-ignore covers tsconfig's pre-ES2020 module kind.
  // @ts-ignore
  const cp = /** @type {typeof import('child_process')} */ (await import('child_process'))
  // @ts-ignore
  const path = /** @type {typeof import('path')} */ (await import('path'))
  // The runner entry point is tests/node.js, so the repo root is one level up.
  const bench = path.join(path.dirname(path.resolve(process.argv[1])), '..', 'benchmarks', 'aging.mjs')
  const sessions = 8
  /** @type {string} */
  const stdout = await new Promise((resolve, reject) => {
    cp.execFile(process.execPath, [bench], {
      env: { ...process.env, AGING_SESSIONS: String(sessions), AGING_EPOCH: '2', AGING_JSON: '1' },
      maxBuffer: 16 * 1024 * 1024
    }, (err, out, errOut) => {
      if (err) {
        reject(new Error(`${bench} failed: ${err.message}\n${errOut}`))
      } else {
        resolve(String(out))
      }
    })
  })
  const lines = stdout.trim().split('\n')
  const result = JSON.parse(lines[lines.length - 1])
  /** @type {Array<{ session: number, harnessTx: number | null }>} */
  const epochs = result.epochs
  t.assert(epochs.length === 4, `expected 4 epochs, got ${epochs.length}`)
  const series = epochs.map(e => `${e.session}:${e.harnessTx}`).join(' ')
  for (const e of epochs) {
    t.assert(typeof e.harnessTx === 'number', `epoch ${e.session} reports no harnessTx (bench not on fake-indexeddb?)`)
  }
  const first = /** @type {number} */ (epochs[0].harnessTx)
  const last = /** @type {number} */ (epochs[epochs.length - 1].harnessTx)
  // Scaling: the retained count must not grow with the number of sessions
  // (4x the sessions between the first and last epoch).
  t.assert(
    last - first <= 8,
    `fake-indexeddb transactions retained by the bench grow with sessions (session:retained = ${series}); the harness keeps every session's provider and Y.Doc alive`
  )
  // Bound: once finished, a transaction is released (as in browsers).
  t.assert(
    last <= 16,
    `the bench still holds ${last} finished fake-indexeddb transactions after ${sessions} sessions (session:retained = ${series})`
  )
}
