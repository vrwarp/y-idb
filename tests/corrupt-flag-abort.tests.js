/* eslint-env browser */

/**
 * Regression test: a full consolidation that ABORTS must not forget that the
 * store still holds an undecodable row.
 *
 * README ('error' event): a stored row that cannot be decoded is skipped and
 * reported, "and the next trim consolidates fully, deleting the bad row".
 * The trim that should delete it can abort instead (QuotaExceededError at
 * commit, a background kill): IndexedDB then rolls back the delete, so the
 * bad row is still stored, and the trim after it must still consolidate
 * fully. Otherwise an incremental trim writes another delta row next to the
 * bad one, which keeps failing every load and every readSnapshot.
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, storeState, PREFERRED_TRIM_SIZE } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'
import * as idb from 'lib0/indexeddb.js'

const requestMethods = /** @type {const} */ ([
  'add', 'put', 'delete', 'clear', 'get', 'getKey', 'getAll', 'getAllKeys', 'count', 'openCursor', 'openKeyCursor'
])

/**
 * One-shot fault injection: the next readwrite transaction opened on
 * database `dbName` is aborted when it would otherwise commit (every request
 * it issued has succeeded and its callbacks issued no further request), the
 * way a browser aborts a transaction on a QuotaExceededError at commit.
 * Only standard IDB API is patched. `whenDone` resolves once the transaction
 * has finished (committed or aborted).
 *
 * @param {string} dbName
 */
const abortNextWriteTransactionAtCommit = dbName => {
  const realTransaction = IDBDatabase.prototype.transaction
  const proto = /** @type {any} */ (IDBObjectStore.prototype)
  const realMethods = requestMethods.map(m => proto[m])
  /** @type {IDBTransaction|null} */
  let target = null
  /** @type {Array<IDBRequest>} */
  const requests = []
  const state = { aborted: false }
  /** @type {function(): void} */
  let markDone = () => {}
  /** @type {Promise<void>} */
  const whenDone = new Promise(resolve => { markDone = resolve })
  // @ts-ignore - override the prototype to pick the target transaction
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    if (target === null && this.name === dbName && tx.mode === 'readwrite') {
      target = tx
      tx.addEventListener('complete', () => markDone())
      tx.addEventListener('abort', () => markDone())
    }
    return tx
  }
  requestMethods.forEach((m, i) => {
    /**
     * @this {IDBObjectStore}
     * @param {...any} args
     */
    proto[m] = function (...args) {
      const req = realMethods[i].apply(this, args)
      if (target !== null && this.transaction === target && !state.aborted) {
        requests.push(req)
        req.addEventListener('success', () => {
          // Runs after the success callbacks' microtasks have drained and
          // before the transaction's next step (commit, if idle). Relies on
          // fake-indexeddb queueing that step with setImmediate after the
          // event dispatch; the caller's `state.aborted` precondition
          // catches a silent miss.
          setImmediate(() => {
            if (state.aborted || target === null) return
            if (requests.every(r => r.readyState === 'done')) {
              try {
                target.abort()
                state.aborted = true
              } catch (e) {
                // Already committing/finished: the injection missed.
              }
            }
          })
        })
      }
      return req
    }
  })
  const restore = () => {
    IDBDatabase.prototype.transaction = realTransaction
    requestMethods.forEach((m, i) => { proto[m] = realMethods[i] })
  }
  return { state, whenDone, restore }
}

/**
 * Resolves with the first lifecycle event ('synced' or 'error').
 *
 * @param {IndexeddbPersistence} provider
 * @return {Promise<{ event: 'synced'|'error', err?: any }>}
 */
const firstLifecycleEvent = provider => new Promise(resolve => {
  provider.on('synced', () => resolve({ event: 'synced' }))
  provider.on('error', /** @param {any} err */ err => resolve({ event: 'error', err }))
})

/**
 * Opens a raw connection with the provider's store layout.
 *
 * @param {string} name
 * @return {Promise<IDBDatabase>}
 */
const openRaw = name => idb.openDB(name, db =>
  idb.createStores(db, [
    ['updates', { autoIncrement: true }],
    ['custom']
  ])
)

/**
 * Applies every stored row to a fresh doc, each on its own, and counts the
 * rows that fail to decode.
 *
 * @param {string} name
 * @return {Promise<{ doc: Y.Doc, keys: Array<number>, corrupt: number }>}
 */
const loadRawRows = async name => {
  const raw = await openRaw(name)
  const [store] = idb.transact(raw, ['updates'], 'readonly')
  const rows = await idb.getAllKeysValues(store)
  raw.close()
  const doc = new Y.Doc()
  let corrupt = 0
  rows.forEach(row => {
    try {
      Y.applyUpdate(doc, row.v)
    } catch (e) {
      corrupt++
    }
  })
  return { doc, keys: rows.map(row => row.k), corrupt }
}

/**
 * A corrupt row sits between the base row and a delta row. The trim that
 * should delete it (a full consolidation) aborts at commit, so the bad row
 * is still stored. When the store reaches the trim threshold again, that
 * trim must consolidate fully and delete the bad row, as the README
 * promises, instead of writing another delta row next to it.
 *
 * @param {t.TestCase} tc
 */
export const testTrimAfterAbortedConsolidationStillDeletesCorruptRow = async tc => {
  await clearDocument(tc.testName)

  // Build [base row] [delta row] with real trim bookkeeping.
  const doc0 = new Y.Doc()
  const arr0 = doc0.getArray('t')
  const p0 = new IndexeddbPersistence(tc.testName, doc0)
  p0._storeTimeout = 1e9
  await p0.whenSynced
  arr0.insert(0, [-1])
  await p0.flush()
  await storeState(p0, true)
  for (let i = 0; i < PREFERRED_TRIM_SIZE + 5; i++) {
    arr0.insert(0, [i])
  }
  await p0.flush()
  await storeState(p0, false)
  t.assert(p0._dbsize === 2, 'precondition: base row + one delta row')
  const expected = arr0.toArray()
  await p0.destroy()

  // Plant a corrupt row between the base row and the delta row.
  const [baseKey, deltaKey] = (await loadRawRows(tc.testName)).keys
  t.assert(baseKey + 1 < deltaKey, 'precondition: a free key between base and delta row')
  const raw = await openRaw(tc.testName)
  const [rawStore] = idb.transact(raw, ['updates'])
  await idb.put(rawStore, new Uint8Array([1, 2, 3]), baseKey + 1)
  raw.close()

  const doc1 = new Y.Doc()
  const arr1 = doc1.getArray('t')
  const p1 = new IndexeddbPersistence(tc.testName, doc1)
  p1._storeTimeout = 1e9
  /** @type {Array<any>} */
  const errors = []
  p1.on('error', /** @param {any} err */ err => { errors.push(err) })
  /** @type {IndexeddbPersistence|null} */
  let p2 = null
  try {
    await firstLifecycleEvent(p1)
    t.compareArrays(arr1.toArray(), expected, 'every valid row is loaded')
    t.assert(errors.length === 1, 'precondition: the corrupt row is reported on load')

    // The store reaches the trim threshold; the trim (which should
    // consolidate fully) aborts at commit, e.g. on a QuotaExceededError.
    for (let i = 0; i < PREFERRED_TRIM_SIZE + 5; i++) {
      arr1.insert(0, [10000 + i])
    }
    await p1.flush()
    const fault = abortNextWriteTransactionAtCommit(tc.testName)
    try {
      await storeState(p1, false).catch(() => {})
      await fault.whenDone
    } finally {
      fault.restore()
    }
    t.assert(fault.state.aborted, 'precondition: the first trim aborted at commit')
    const afterAbort = await loadRawRows(tc.testName)
    t.assert(afterAbort.corrupt === 1, 'precondition: the aborted trim left the corrupt row in place')

    // The store reaches the trim threshold again. This trim commits and,
    // since the bad row is still stored, must consolidate fully.
    for (let i = 0; i < PREFERRED_TRIM_SIZE + 5; i++) {
      arr1.insert(0, [20000 + i])
    }
    await p1.flush()
    await storeState(p1, false)

    const persisted = await loadRawRows(tc.testName)
    t.assert(persisted.corrupt === 0, `the next trim deletes the corrupt row that survived the aborted one (rows: ${persisted.keys.join(', ')}; ${persisted.corrupt} undecodable)`)
    t.compareArrays(persisted.doc.getArray('t').toArray(), arr1.toArray(), 'persisted state is complete')
    await p1.destroy()

    const doc2 = new Y.Doc()
    p2 = new IndexeddbPersistence(tc.testName, doc2)
    const outcome = await firstLifecycleEvent(p2)
    t.assert(outcome.event === 'synced', `reload after the trim must sync cleanly (got '${outcome.event}': ${outcome.err && outcome.err.message})`)
    t.compareArrays(doc2.getArray('t').toArray(), arr1.toArray(), 'reload sees all valid content')
  } finally {
    await p1.destroy()
    if (p2) await p2.destroy()
  }
}
