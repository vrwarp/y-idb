/* eslint-env browser */

/**
 * Regression tests: the payload of the 'error' event must be an Error (or
 * DOMException) that keeps the real IndexedDB error name, as the README
 * documents (`function(error: Error)`, "e.g. QuotaExceededError") — on every
 * path, not only on the flush path.
 *
 * - Request-level write failures (a failing request's bubbling 'error'
 *   event reaches the transaction before the abort steps set `tx.error`)
 *   previously emitted `null` on the flush, teardown and writeSnapshot paths.
 * - The hydration and trim chains previously forwarded lib0's
 *   request-wrapper rejections unchanged: lib0's `rtop` rejects with
 *   `new Error(domException)` (name 'Error', the real name folded into the
 *   message) and its cursor iteration (`getLastKey`) rejects with the raw
 *   error Event (no name, no message). Consumers checking
 *   `err.name === 'QuotaExceededError'` missed such failures.
 */

import * as Y from 'yjs'
import * as idb from 'lib0/indexeddb.js'
import * as t from 'lib0/testing.js'
import { IndexeddbPersistence, clearDocument, writeSnapshot, PREFERRED_TRIM_SIZE } from '../src/y-idb.js'

/**
 * Arms a one-shot fault on `target` (a database, or `IDBDatabase.prototype`
 * for databases the code under test opens itself): the next readwrite
 * transaction opened on the updates store gets two `add` requests for the
 * same explicit key queued ahead of the caller's own requests. The second
 * one fails with a request-level ConstraintError, which (not being
 * prevented) aborts the whole transaction. This stands in for the realistic
 * trigger, a QuotaExceededError that Firefox and Safari report on the
 * failing request rather than at commit (the provider's own autoIncrement
 * adds cannot raise a ConstraintError): either way the request's bubbling
 * 'error' event reaches the transaction BEFORE the abort steps set
 * `tx.error`.
 *
 * @param {IDBDatabase} target
 * @return {() => void} Restores the original `transaction` method.
 */
const armRequestLevelConstraintError = target => {
  const realTransaction = target.transaction
  let armed = true
  /**
   * @this {IDBDatabase}
   * @param {string | string[]} storeNames
   * @param {IDBTransactionMode} [mode]
   * @param {IDBTransactionOptions} [options]
   * @return {IDBTransaction}
   */
  const faultyTransaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    const names = typeof storeNames === 'string' ? [storeNames] : Array.from(storeNames)
    if (armed && mode === 'readwrite' && names.includes('updates')) {
      armed = false
      const store = tx.objectStore('updates')
      store.add(new Uint8Array([0, 0]), 'fault-injection-duplicate-key')
      store.add(new Uint8Array([0, 0]), 'fault-injection-duplicate-key')
    }
    return tx
  }
  target.transaction = faultyTransaction
  return () => { target.transaction = realTransaction }
}

/**
 * Resolves with `p`'s value, or with 'timeout' after `ms` — only a guard so
 * a missing event fails the test instead of hanging the runner.
 *
 * @template T
 * @param {Promise<T>} p
 * @param {number} ms
 * @return {Promise<T|'timeout'>}
 */
const withDeadline = (p, ms) => {
  /** @type {any} */
  let id = null
  /** @type {Promise<'timeout'>} */
  const deadline = new Promise(resolve => { id = setTimeout(() => resolve('timeout'), ms) })
  return Promise.race([p, deadline]).finally(() => clearTimeout(id))
}

/**
 * README contract: `provider.on('error', function(error: Error))`. A flush
 * whose transaction fails because of a request-level error (here a
 * ConstraintError on one of its requests) must emit 'error' with an Error
 * the app can inspect (e.g. to detect quota exhaustion) — not `null`, which
 * also makes README-style handlers (`err.message || err`) throw.
 *
 * @param {t.TestCase} tc
 */
export const testRequestLevelFlushFailureEmitsAnErrorInstance = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  /** @type {Array<any>} */
  const errors = []
  /** @type {Promise<'error'>} */
  const firstError = new Promise(resolve => {
    persistence.on('error', (/** @type {any} */ err) => {
      errors.push(err)
      resolve('error')
    })
  })

  const restore = armRequestLevelConstraintError(/** @type {IDBDatabase} */ (persistence.db))
  try {
    doc.getArray('t').insert(0, [1])
    const outcome = await withDeadline(firstError, 5000)
    t.assert(outcome === 'error', "the failed flush must emit 'error'")
    t.assert(
      errors[0] instanceof Error,
      `'error' must carry an Error for a request-level flush failure, got: ${String(errors[0])}`
    )
    t.assert(errors[0].name === 'ConstraintError', 'the failing request\'s own error is forwarded')
  } finally {
    restore()
    await persistence.destroy()
  }
}

/**
 * Same contract for the final write issued by destroy(): editing and
 * destroying in the same tick routes the pending update through the
 * teardown write, which hits the request-level fault. Every 'error' emitted
 * for it must carry an Error.
 *
 * @param {t.TestCase} tc
 */
export const testRequestLevelFailureOfDestroyFinalWriteEmitsAnErrorInstance = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  /** @type {Array<any>} */
  const errors = []
  persistence.on('error', (/** @type {any} */ err) => { errors.push(err) })

  const restore = armRequestLevelConstraintError(/** @type {IDBDatabase} */ (persistence.db))
  try {
    doc.getArray('t').insert(0, [1])
    await persistence.destroy()
  } finally {
    restore()
  }

  t.assert(errors.length > 0, "the failed teardown write must emit 'error'")
  errors.forEach((err, i) => {
    t.assert(
      err instanceof Error,
      `'error' #${i} must carry an Error for a request-level teardown failure, got: ${String(err)}`
    )
  })
  t.assert(errors[0].name === 'ConstraintError', 'the failing request\'s own error is forwarded')
}

/**
 * writeSnapshot() rejects with the failing request's own error (so callers
 * can tell e.g. quota exhaustion apart), not with a generic fallback Error.
 *
 * @param {t.TestCase} tc
 */
export const testRequestLevelFailureOfWriteSnapshotRejectsWithTheRequestError = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  doc.getArray('t').insert(0, [1])
  const update = Y.encodeStateAsUpdate(doc)

  /** @type {any} */
  let rejection = null
  const restore = armRequestLevelConstraintError(IDBDatabase.prototype)
  try {
    await writeSnapshot(tc.testName, update).catch(err => { rejection = err })
  } finally {
    restore()
  }

  t.assert(rejection instanceof Error, `writeSnapshot must reject with an Error, got: ${String(rejection)}`)
  t.assert(rejection.name === 'ConstraintError', 'the failing request\'s own error is forwarded')
}

const TIMED_OUT = Symbol('timed out')

/**
 * Resolve with the payload of the first 'error' event, or TIMED_OUT.
 *
 * @param {IndexeddbPersistence} persistence
 * @param {number} ms
 * @return {Promise<any>}
 */
const nextError = (persistence, ms) => new Promise(resolve => {
  const timer = setTimeout(() => resolve(TIMED_OUT), ms)
  persistence.once('error', (/** @type {any} */ err) => {
    clearTimeout(timer)
    resolve(err)
  })
})

/**
 * Write `rows` straight into database `name` (same store layout as y-idb)
 * and wait for the commit.
 *
 * @param {string} name
 * @param {Array<Uint8Array>} rows
 * @return {Promise<void>}
 */
const prepopulate = (name, rows) => idb.openDB(name, db =>
  idb.createStores(db, [
    ['updates', { autoIncrement: true }],
    ['custom']
  ])
).then(db => new Promise((resolve, reject) => {
  const tx = db.transaction(['updates'], 'readwrite')
  const store = tx.objectStore('updates')
  rows.forEach(row => store.add(row))
  tx.oncomplete = () => { db.close(); resolve(undefined) }
  tx.onerror = tx.onabort = () => { db.close(); reject(tx.error) }
}))

/**
 * @param {number} n
 * @return {Array<Uint8Array>} n valid, independent Yjs updates
 */
const makeUpdates = n => {
  const doc = new Y.Doc()
  /** @type {Array<Uint8Array>} */
  const updates = []
  doc.on('update', (/** @type {Uint8Array} */ u) => { updates.push(u) })
  const arr = doc.getArray('t')
  for (let i = 0; i < n; i++) arr.insert(arr.length, [i])
  doc.destroy()
  return updates
}

/**
 * Make the next `add` that `shouldFail` selects reuse key 1 (which exists),
 * so its request fails with a real ConstraintError DOMException and the
 * transaction aborts. Returns the restore function.
 *
 * @param {function(IDBObjectStore):boolean} shouldFail
 * @return {function():void}
 */
const failNextAdd = shouldFail => {
  const realAdd = IDBObjectStore.prototype.add
  let armed = true
  /**
   * @this {IDBObjectStore}
   * @param {any} value
   * @param {IDBValidKey} [key]
   */
  const patchedAdd = function (value, key) {
    if (armed && shouldFail(this)) {
      armed = false
      return realAdd.call(this, value, 1)
    }
    return key === undefined ? realAdd.call(this, value) : realAdd.call(this, value, key)
  }
  IDBObjectStore.prototype.add = patchedAdd
  return () => { IDBObjectStore.prototype.add = realAdd }
}

/**
 * A request failure during the debounced trim (here a ConstraintError on
 * the full-consolidation write — in practice most likely a
 * QuotaExceededError, since that write is O(document)) must reach 'error'
 * listeners with its real name, not as `Error('ConstraintError: ...')`.
 *
 * @param {t.TestCase} tc
 */
export const testTrimRequestErrorPayloadKeepsDomExceptionName = async tc => {
  await clearDocument(tc.testName)
  // Enough rows for the trim to do real work (a full consolidation).
  await prepopulate(tc.testName, makeUpdates(PREFERRED_TRIM_SIZE))
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced
  t.assert(persistence._dbsize >= PREFERRED_TRIM_SIZE, 'hydration counted the stored rows')

  persistence._storeTimeout = 0
  // Only the trim transaction spans the 'custom' store; flushes and the
  // hydration transaction do not.
  const restore = failNextAdd(store => store.transaction.objectStoreNames.contains('custom'))
  try {
    const errorPromise = nextError(persistence, 5000)
    // The flush completes over the trim threshold and schedules the trim.
    doc.getArray('t').insert(0, ['x'])
    const err = await errorPromise
    t.assert(err !== TIMED_OUT, 'the failed trim emitted an error event')
    t.assert(err instanceof Error, `payload is an Error/DOMException (got ${Object.prototype.toString.call(err)})`)
    t.assert(err.name === 'ConstraintError', `payload keeps the real error name (got name ${JSON.stringify(err.name)}, message ${JSON.stringify(err.message)})`)
  } finally {
    restore()
    await persistence.destroy()
  }
}

/**
 * A request failure during hydration (here the initial-state write of a doc
 * that already has in-memory content fails, aborting the hydration
 * transaction while the last-key cursor is pending) must reach 'error'
 * listeners as an Error/DOMException with a real name and message, not as a
 * raw IDB error Event.
 *
 * @param {t.TestCase} tc
 */
export const testHydrationRequestErrorPayloadIsNamedError = async tc => {
  await clearDocument(tc.testName)
  // Key 1 exists, so the patched add below collides with it.
  await prepopulate(tc.testName, makeUpdates(1))
  const doc = new Y.Doc()
  doc.getArray('local').insert(0, ['pre-existing in-memory content'])
  // The first add after construction is hydration's initial-state write.
  const restore = failNextAdd(() => true)
  /** @type {IndexeddbPersistence|null} */
  let persistence = null
  try {
    persistence = new IndexeddbPersistence(tc.testName, doc)
    const err = await nextError(persistence, 5000)
    t.assert(err !== TIMED_OUT, 'the failed hydration emitted an error event')
    t.assert(err instanceof Error, `payload is an Error/DOMException (got ${Object.prototype.toString.call(err)}, constructor ${err && err.constructor && err.constructor.name})`)
    t.assert(typeof err.message === 'string' && err.message.length > 0, `payload has a message (got ${JSON.stringify(err.message)})`)
    // Either the root cause (ConstraintError) or the resulting abort of the
    // pending request (AbortError) is an acceptable, real name.
    t.assert(err.name === 'ConstraintError' || err.name === 'AbortError', `payload keeps a real IndexedDB error name (got ${JSON.stringify(err.name)})`)
  } finally {
    restore()
    if (persistence) await persistence.destroy()
  }
}

/**
 * The same holds for hydration's plain (non-cursor) requests: when the
 * hydration transaction is aborted while its getAll is pending, the 'error'
 * payload must be named 'AbortError', not `Error('AbortError: ...')`.
 *
 * @param {t.TestCase} tc
 */
export const testHydrationGetAllErrorPayloadKeepsDomExceptionName = async tc => {
  await clearDocument(tc.testName)
  await prepopulate(tc.testName, makeUpdates(1))
  const doc = new Y.Doc()
  const realGetAll = IDBObjectStore.prototype.getAll
  let armed = true
  /**
   * @this {IDBObjectStore}
   * @param {IDBValidKey|IDBKeyRange|null} [query]
   * @param {number} [count]
   */
  const patchedGetAll = function (query, count) {
    const request = realGetAll.call(this, query, count)
    // The first getAll after construction is hydration's.
    if (armed) {
      armed = false
      this.transaction.abort()
    }
    return request
  }
  IDBObjectStore.prototype.getAll = patchedGetAll
  /** @type {IndexeddbPersistence|null} */
  let persistence = null
  try {
    persistence = new IndexeddbPersistence(tc.testName, doc)
    const err = await nextError(persistence, 5000)
    t.assert(err !== TIMED_OUT, 'the failed hydration emitted an error event')
    t.assert(err instanceof Error, `payload is an Error/DOMException (got ${Object.prototype.toString.call(err)})`)
    t.assert(err.name === 'AbortError', `payload keeps the real error name (got name ${JSON.stringify(err.name)}, message ${JSON.stringify(err.message)})`)
  } finally {
    IDBObjectStore.prototype.getAll = realGetAll
    if (persistence) await persistence.destroy()
  }
}
