/* eslint-env browser */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, writeSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

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
