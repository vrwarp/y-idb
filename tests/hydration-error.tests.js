/* eslint-env browser */

/**
 * Regression tests: a hydration failure that happens AFTER the stored rows
 * have been applied to the doc must still resolve `whenSynced` / emit
 * 'synced' (in addition to 'error'). The doc already holds the complete
 * persisted state at that point, so leaving `whenSynced` pending wedges every
 * consumer that waits on it before showing data or connecting sync.
 *
 * The counterpart is testInitialSyncFailureEmitsError in y-idb.tests.js: a
 * failure BEFORE any stored row was read must still NOT emit 'synced'.
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, writeSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'
import * as promise from 'lib0/promise.js'

/**
 * Safety bound only. Every wait below first synchronizes on an explicit
 * event (the hydration 'error' and the hydration transaction settling), so a
 * correct implementation has already resolved `whenSynced` when this bound
 * starts; it only keeps the buggy behavior (never resolves) from hanging the
 * suite.
 */
const BOUND_MS = 2000

/**
 * @template T
 * @param {Promise<T>} p
 * @param {number} ms
 * @return {Promise<T|'timeout'>}
 */
const within = (p, ms) => Promise.race([p, promise.wait(ms).then(() => /** @type {'timeout'} */ ('timeout'))])

/**
 * Resolves once a fresh transaction on the updates store has completed on
 * the provider's own connection. IndexedDB starts that transaction only after
 * the earlier (overlapping, readwrite) hydration transaction has finished, so
 * this is an explicit "hydration transaction has settled" point.
 *
 * @param {IndexeddbPersistence} persistence
 * @return {Promise<void>}
 */
const hydrationTransactionSettled = persistence => new Promise((resolve, reject) => {
  const tx = /** @type {IDBDatabase} */ (persistence.db).transaction(['updates'], 'readonly')
  tx.objectStore('updates').count()
  tx.oncomplete = () => resolve()
  tx.onerror = tx.onabort = () => reject(tx.error)
})

/**
 * @param {IndexeddbPersistence} persistence
 * @return {{ count: () => number }}
 */
const countSynced = persistence => {
  let n = 0
  persistence.on('synced', () => { n++ })
  return { count: () => n }
}

/**
 * @param {IndexeddbPersistence} persistence
 * @return {{ errors: Array<any>, firstError: Promise<any> }}
 */
const collectErrors = persistence => {
  /** @type {Array<any>} */
  const errors = []
  const firstError = promise.create(resolve => {
    persistence.on('error', /** @param {any} err */ err => {
      errors.push(err)
      resolve(err)
    })
  })
  return { errors, firstError }
}

/**
 * Write `update` as the stored state of `name`, then append one extra row at
 * the maximum auto-increment key (2^53). The updates store's key generator is
 * then exhausted, so the next key-less `add` on it fails ASYNCHRONOUSLY with a
 * request-level ConstraintError that aborts its transaction — the same shape
 * as a QuotaExceededError/UnknownError reported on that request by a browser.
 * The extra row is a valid (empty) Yjs update, so it does not change the doc
 * content.
 *
 * @param {string} name
 * @param {Uint8Array} update
 * @return {Promise<void>}
 */
const writeSnapshotWithExhaustedKeyGenerator = async (name, update) => {
  await writeSnapshot(name, update)
  await new Promise((resolve, reject) => {
    const req = indexedDB.open(name)
    req.onerror = () => reject(req.error)
    req.onsuccess = () => {
      const db = req.result
      const tx = db.transaction(['updates'], 'readwrite')
      tx.objectStore('updates').put(Y.encodeStateAsUpdate(new Y.Doc()), 2 ** 53)
      tx.oncomplete = () => {
        db.close()
        resolve(undefined)
      }
      tx.onerror = tx.onabort = () => {
        db.close()
        reject(tx.error)
      }
    }
  })
}

/**
 * Trigger (a): an app observer on the doc (e.g. a store mirror's observe
 * handler hitting bad data) throws while the stored rows are applied. Yjs
 * integrates every row and then rethrows the observer's exception out of the
 * hydration transaction.
 *
 * @param {t.TestCase} tc
 */
export const testHydrationObserverErrorAfterRowsAppliedStillResolvesWhenSynced = async tc => {
  await clearDocument(tc.testName)
  const source = new Y.Doc()
  source.getMap('m').set('stored', 'v')
  await writeSnapshot(tc.testName, Y.encodeStateAsUpdate(source))

  const doc = new Y.Doc()
  let thrown = false
  doc.getMap('m').observe(() => {
    if (!thrown) {
      thrown = true
      throw new Error('mirror failed')
    }
  })
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  try {
    const { errors, firstError } = collectErrors(persistence)
    const syncedEvents = countSynced(persistence)
    const synced = persistence.whenSynced.then(() => 'synced')

    // The hydration failure is surfaced ...
    t.assert(await within(firstError.then(() => 'error'), BOUND_MS) === 'error', "hydration failure must emit 'error'")
    t.assert(errors.some(err => err instanceof Error && err.message === 'mirror failed'), "the observer's exception is the reported error")
    // ... after every stored row was applied to the doc.
    t.assert(thrown, 'observer ran during hydration')
    t.compare(doc.getMap('m').toJSON(), { stored: 'v' }, 'doc holds the full persisted state')

    await hydrationTransactionSettled(persistence)
    t.assert(await within(synced, BOUND_MS) === 'synced', 'whenSynced must resolve: the persisted state is already in the doc')
    t.assert(persistence.synced, "provider must report synced (and have emitted 'synced')")
    t.assert(syncedEvents.count() === 1, "'synced' is emitted exactly once")
  } finally {
    await persistence.destroy()
  }
}

/**
 * Trigger (b): the hydration transaction gets a request-level error AFTER
 * getAll returned and the rows were applied — here on the raw initial-state
 * `add` (the doc already had content before the provider was attached). The
 * error aborts the transaction, so the pending getLastKey cursor fails too.
 *
 * @param {t.TestCase} tc
 */
export const testHydrationRequestErrorAfterRowsAppliedStillResolvesWhenSynced = async tc => {
  await clearDocument(tc.testName)
  const source = new Y.Doc()
  source.getMap('m').set('stored', 'v')
  await writeSnapshotWithExhaustedKeyGenerator(tc.testName, Y.encodeStateAsUpdate(source))

  const doc = new Y.Doc()
  // Pre-existing local content makes hydration issue the initial-state add.
  doc.getMap('m').set('pre', 'x')
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  try {
    const { firstError } = collectErrors(persistence)
    const syncedEvents = countSynced(persistence)
    const synced = persistence.whenSynced.then(() => 'synced')

    // Synchronize on the hydration failure being surfaced, then on the
    // hydration transaction having settled (aborted).
    t.assert(await within(firstError.then(() => 'error'), BOUND_MS) === 'error', "hydration failure must emit 'error'")
    t.compare(doc.getMap('m').toJSON(), { pre: 'x', stored: 'v' }, 'doc holds the full persisted state')

    await hydrationTransactionSettled(persistence)
    t.assert(await within(synced, BOUND_MS) === 'synced', 'whenSynced must resolve: the persisted state is already in the doc')
    t.assert(persistence.synced, "provider must report synced (and have emitted 'synced')")
    // The request error bubbles to the transaction before it aborts; one
    // failed hydration must still produce a single 'synced'.
    t.assert(syncedEvents.count() === 1, "'synced' is emitted exactly once")
  } finally {
    await persistence.destroy()
  }
}
