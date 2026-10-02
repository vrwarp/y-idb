/* eslint-env browser */

/**
 * Regression tests: a trim or hydration transaction that ABORTS must not
 * advance the provider's read cursor past keys that were never committed.
 *
 * On abort, IndexedDB rolls back every write of the transaction, including
 * the autoIncrement key generator, so the next row ANY connection writes
 * (another tab) re-uses the key the aborted transaction had been handed. If
 * the provider already treats that key as applied, it never reads the other
 * tab's row, and its next full consolidation (which deletes every row older
 * than the new base row) silently destroys that tab's edit.
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, storeState, readSnapshot, PREFERRED_TRIM_SIZE } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

const requestMethods = /** @type {const} */ ([
  'add', 'put', 'delete', 'clear', 'get', 'getKey', 'getAll', 'getAllKeys', 'count', 'openCursor', 'openKeyCursor'
])

/**
 * One-shot fault injection: the next readwrite transaction opened on
 * database `dbName` is aborted, the way a browser aborts it on a failed
 * request, a QuotaExceededError at commit, or a background-tab kill.
 *
 * - `at = 'add'`: abort as soon as its first add() request has succeeded
 *   (mid-chain, after the new row was handed its auto-increment key).
 * - `at = 'commit'`: abort when the transaction would otherwise commit —
 *   every request it issued has succeeded and the caller issued no further
 *   request once its promise callbacks had run.
 *
 * Only standard IDB API is patched. `whenDone` resolves once the
 * transaction has finished (committed or aborted).
 *
 * @param {string} dbName
 * @param {'add'|'commit'} at
 */
const abortNextWriteTransaction = (dbName, at) => {
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
  const abort = () => {
    if (state.aborted || target === null) return
    try {
      target.abort()
      state.aborted = true
    } catch (e) {
      // Already committing/finished: the injection missed.
    }
  }
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
          if (at === 'add') {
            if (m === 'add') abort()
          } else {
            // Runs after the success callbacks' microtasks have drained and
            // before the transaction's next step (commit, if idle). Relies
            // on fake-indexeddb queueing that step with setImmediate after
            // the event dispatch (an implementation detail); the callers'
            // `state.aborted` precondition catches a silent miss.
            setImmediate(() => {
              if (requests.every(r => r.readyState === 'done')) abort()
            })
          }
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
 * The persisted content of `name`, as a fresh doc would load it after a
 * reload.
 *
 * @param {string} name
 */
const reloadMap = async name => {
  const snapshot = await readSnapshot(name)
  const doc = new Y.Doc()
  if (snapshot !== null) Y.applyUpdate(doc, snapshot)
  const json = doc.getMap('m').toJSON()
  doc.destroy()
  return json
}

/**
 * Tab B opens the same database, writes one edit, persists it and closes.
 *
 * @param {string} name
 */
const otherTabWritesAndCloses = async name => {
  const docB = new Y.Doc()
  const pB = new IndexeddbPersistence(name, docB)
  await pB.whenSynced
  docB.getMap('m').set('fromB', 'x')
  await pB.flush()
  await pB.destroy()
}

/**
 * Tab A's forced full consolidation aborts after its new base row was added.
 * Tab B then persists an edit (it gets the key the aborted row had). Tab A's
 * next full consolidation must fetch and keep B's row, not delete it.
 *
 * @param {t.TestCase} tc
 */
export const testAbortedFullConsolidationKeepsOtherTabsLaterEdit = async tc => {
  await clearDocument(tc.testName)
  const docA = new Y.Doc()
  const pA = new IndexeddbPersistence(tc.testName, docA)
  try {
    await pA.whenSynced
    docA.getMap('m').set('a', 1)
    await pA.flush()

    const { _dbref: dbref, _dbsize: dbsize } = pA
    const fault = abortNextWriteTransaction(tc.testName, 'add')
    try {
      await storeState(pA, true).catch(() => {})
      await fault.whenDone
    } finally {
      fault.restore()
    }
    t.assert(fault.state.aborted, 'precondition: the consolidation transaction was aborted')
    t.compare(await reloadMap(tc.testName), { a: 1 }, 'aborted consolidation left the store as it was')
    t.compare([pA._dbref, pA._dbsize], [dbref, dbsize], 'aborted consolidation left the cursor as it was')

    await otherTabWritesAndCloses(tc.testName)
    t.compare(await reloadMap(tc.testName), { a: 1, fromB: 'x' }, 'tab B persisted its edit')

    await storeState(pA, true)
    t.compare(await reloadMap(tc.testName), { a: 1, fromB: 'x' }, "tab A's full consolidation must not delete tab B's row")
    t.compare(docA.getMap('m').toJSON(), { a: 1, fromB: 'x' }, "tab A must have applied tab B's row")
  } finally {
    await pA.destroy()
  }
}

/**
 * Same, for the incremental (delta-row) trim, aborted at commit time
 * (e.g. QuotaExceededError): storeState resolves, but nothing was written.
 *
 * @param {t.TestCase} tc
 */
export const testCommitAbortedIncrementalTrimKeepsOtherTabsLaterEdit = async tc => {
  await clearDocument(tc.testName)
  // Seed: a base row (with trim bookkeeping) plus a tail of
  // PREFERRED_TRIM_SIZE fresh rows, so the next trim is an incremental one.
  const doc0 = new Y.Doc()
  const p0 = new IndexeddbPersistence(tc.testName, doc0)
  await p0.whenSynced
  doc0.getMap('m').set('a', 1)
  await p0.flush()
  await storeState(p0, true)
  for (let i = 0; i < PREFERRED_TRIM_SIZE; i++) {
    doc0.getArray('tail').push([i])
  }
  await p0.flush()
  await p0.destroy()

  const docA = new Y.Doc()
  const pA = new IndexeddbPersistence(tc.testName, docA)
  try {
    await pA.whenSynced
    t.compare(docA.getMap('m').toJSON(), { a: 1 })

    const { _dbref: dbref, _dbsize: dbsize } = pA
    const fault = abortNextWriteTransaction(tc.testName, 'commit')
    try {
      // The abort comes after every request succeeded, so storeState itself
      // resolves; only the transaction's abort event reports it.
      await storeState(pA, false).catch(() => {})
      await fault.whenDone
    } finally {
      fault.restore()
    }
    t.assert(fault.state.aborted, 'precondition: the trim transaction was aborted')
    t.compare([pA._dbref, pA._dbsize], [dbref, dbsize], 'aborted trim left the cursor as it was')

    await otherTabWritesAndCloses(tc.testName)
    t.compare(await reloadMap(tc.testName), { a: 1, fromB: 'x' }, 'tab B persisted its edit')

    await storeState(pA, true)
    t.compare(await reloadMap(tc.testName), { a: 1, fromB: 'x' }, "tab A's full consolidation must not delete tab B's row")
    t.compare(docA.getMap('m').toJSON(), { a: 1, fromB: 'x' }, "tab A must have applied tab B's row")
  } finally {
    await pA.destroy()
  }
}

/**
 * Same, for the hydration transaction that carries the initial-state row of
 * a doc that already had content, aborted at commit time. Unlike the trim
 * cases, this one predates the tiered trim (b8b3271): it is inherited from
 * upstream y-indexeddb.
 *
 * @param {t.TestCase} tc
 */
export const testCommitAbortedHydrationKeepsOtherTabsLaterEdit = async tc => {
  await clearDocument(tc.testName)
  const docA = new Y.Doc()
  docA.getMap('m').set('a', 1)

  const fault = abortNextWriteTransaction(tc.testName, 'commit')
  /** @type {IndexeddbPersistence|null} */
  let pA = null
  try {
    try {
      // Long debounce: tab B writes before tab A writes anything (whatever
      // tab A re-buffers after the failed hydration stays pending).
      pA = new IndexeddbPersistence(tc.testName, docA, { writeDebounceMs: 60000 })
      await pA.whenSynced
      await fault.whenDone
    } finally {
      fault.restore()
    }
    t.assert(fault.state.aborted, 'precondition: the hydration transaction was aborted')
    t.compare([pA._dbref, pA._dbsize], [0, 0], 'aborted hydration left the cursor at the empty store')

    await otherTabWritesAndCloses(tc.testName)
    t.assert((await reloadMap(tc.testName)).fromB === 'x', 'tab B persisted its edit')

    await storeState(pA, true)
    t.compare(await reloadMap(tc.testName), { a: 1, fromB: 'x' }, "tab A's full consolidation must not delete tab B's row")
    t.compare(docA.getMap('m').toJSON(), { a: 1, fromB: 'x' }, "tab A must have applied tab B's row")
  } finally {
    if (pA !== null) await pA.destroy()
  }
}
