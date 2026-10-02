/* eslint-env browser */

/**
 * An 'error' listener is user code. If it throws (a deliberate rethrow, or
 * a handler like `err.message` on an unexpected payload), the exception must
 * not break the provider's write machinery: a failed flush must still
 * settle, so flush()/destroy() resolve, the failed batch is persisted, and a
 * serializing transactionRunner is not left holding its lock forever.
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * Upper bound used ONLY to detect a hang (a promise that never settles).
 * Every awaited operation below completes in a few event-loop turns (or one
 * ~200ms backoff retry) when the provider behaves correctly.
 */
const HANG_DEADLINE_MS = 3000

/**
 * @param {Promise<any>} p
 * @return {Promise<'resolved'|'rejected'|'pending'>}
 */
const settleWithin = p => new Promise(resolve => {
  const timer = setTimeout(() => resolve('pending'), HANG_DEADLINE_MS)
  p.then(
    () => { clearTimeout(timer); resolve('resolved') },
    () => { clearTimeout(timer); resolve('rejected') }
  )
})

/**
 * Capture the provider's next readwrite transaction on the `updates` store
 * (i.e. its next flush) so the test can fail it at a chosen moment. `next`
 * resolves after the flush has queued its add() requests synchronously but
 * before any of them has executed.
 *
 * @param {IndexeddbPersistence} persistence
 * @return {{ next: Promise<IDBTransaction>, restore: () => void }}
 */
const interceptNextFlushTransaction = persistence => {
  const db = /** @type {IDBDatabase} */ (persistence.db)
  const originalTransaction = db.transaction
  /** @type {(tx: IDBTransaction) => void} */
  let resolveNext = () => {}
  /** @type {Promise<IDBTransaction>} */
  const next = new Promise(resolve => { resolveNext = resolve })
  let captured = false
  // @ts-ignore
  db.transaction = function (storeNames, mode, options) {
    const tx = originalTransaction.call(this, storeNames, mode, options)
    if (!captured && mode === 'readwrite' && storeNames.includes('updates')) {
      captured = true
      resolveNext(tx)
    }
    return tx
  }
  return { next, restore: () => { db.transaction = originalTransaction } }
}

/**
 * The node harness turns any uncaught exception or unhandled rejection into
 * an immediate suite failure. A fix may legitimately re-report the
 * listener's OWN exception asynchronously (e.g. emit from a microtask), so
 * while the test runs, collect escapes instead; the test then asserts that
 * nothing but the listener's own error escaped.
 *
 * @return {{ escaped: Array<any>, restore: () => void }}
 */
const captureEscapedErrors = () => {
  /** @type {Array<any>} */
  const escaped = []
  if (typeof process === 'undefined') {
    return { escaped, restore: () => {} }
  }
  const savedUncaught = process.listeners('uncaughtException')
  const savedUnhandled = process.listeners('unhandledRejection')
  process.removeAllListeners('uncaughtException')
  process.removeAllListeners('unhandledRejection')
  /**
   * @param {any} err
   */
  const record = err => { escaped.push(err) }
  process.on('uncaughtException', record)
  process.on('unhandledRejection', record)
  return {
    escaped,
    restore: () => {
      process.off('uncaughtException', record)
      process.off('unhandledRejection', record)
      savedUncaught.forEach(l => process.on('uncaughtException', l))
      savedUnhandled.forEach(l => process.on('unhandledRejection', l))
    }
  }
}

/**
 * @param {Array<any>} escaped
 * @param {Error} listenerError
 */
const assertOnlyListenerErrorEscaped = (escaped, listenerError) => {
  const isListenerError = /** @param {any} e */ e =>
    e === listenerError ||
    (e != null && Array.isArray(e.errors) && e.errors.length > 0 && e.errors.every(/** @param {any} x */ x => x === listenerError))
  t.assert(escaped.every(isListenerError), `only the listener's own error may escape (got: ${escaped.map(e => String(e)).join(', ')})`)
}

/**
 * Read back what a fresh provider hydrates from `name`.
 *
 * @param {string} name
 * @return {Promise<Array<any>>}
 */
const readPersistedArray = async name => {
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(name, doc)
  await persistence.whenSynced
  const content = doc.getArray('t').toArray()
  await persistence.destroy()
  return content
}

/**
 * flush() called while a flush is in flight must resolve once the queue is
 * drained, even if the in-flight flush fails and an 'error' listener throws.
 *
 * @param {t.TestCase} tc
 */
export const testFlushResolvesWhenErrorListenerThrows = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  const listenerError = new Error('error listener failure')
  const throwingListener = () => { throw listenerError }
  const escapes = captureEscapedErrors()
  const intercept = interceptNextFlushTransaction(persistence)
  let errorEvents = 0
  let retryArmed = false
  persistence.on('error', () => {
    errorEvents++
    // Sampled once the failure handler has returned: the failed batch must
    // have a backoff retry armed despite the throwing listener below.
    queueMicrotask(() => { retryArmed = persistence._retryTimeoutId !== null })
  })
  persistence.on('error', throwingListener)
  /** @type {'resolved'|'rejected'|'pending'} */
  let flushStatus
  try {
    doc.getArray('t').insert(0, [1])
    const flushTx = await intercept.next
    // The flush transaction exists and its add() is queued: flush() must
    // now wait for that in-flight write.
    const flushed = persistence.flush()
    intercept.restore()
    flushTx.abort()
    flushStatus = await settleWithin(flushed)
  } finally {
    intercept.restore()
    escapes.restore()
    persistence.off('error', throwingListener)
  }

  t.assert(errorEvents >= 1, 'the failed flush should have been reported via the error event')
  t.assert(retryArmed, "a backoff retry must be armed after the failed flush, even though an 'error' listener threw")
  t.assert(flushStatus === 'resolved', `flush() must resolve after the failed batch is retried, even though an 'error' listener threw (flush() is ${flushStatus} after ${HANG_DEADLINE_MS}ms)`)
  assertOnlyListenerErrorEscaped(escapes.escaped, listenerError)

  await persistence.destroy()
  t.compareArrays(await readPersistedArray(tc.testName), [1])
}

/**
 * destroy() called while a flush is in flight must resolve and persist the
 * failed batch in its final write, even if an 'error' listener throws.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyResolvesWhenErrorListenerThrows = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  const listenerError = new Error('error listener failure')
  const throwingListener = () => { throw listenerError }
  const escapes = captureEscapedErrors()
  const intercept = interceptNextFlushTransaction(persistence)
  persistence.on('error', throwingListener)
  /** @type {'resolved'|'rejected'|'pending'} */
  let destroyStatus
  try {
    doc.getArray('t').insert(0, [1])
    const flushTx = await intercept.next
    const destroyed = persistence.destroy()
    intercept.restore()
    flushTx.abort()
    destroyStatus = await settleWithin(destroyed)
  } finally {
    intercept.restore()
    escapes.restore()
    persistence.off('error', throwingListener)
  }

  t.assert(destroyStatus === 'resolved', `destroy() must resolve when the in-flight flush fails and an 'error' listener throws (destroy() is ${destroyStatus} after ${HANG_DEADLINE_MS}ms)`)
  assertOnlyListenerErrorEscaped(escapes.escaped, listenerError)
  t.compareArrays(await readPersistedArray(tc.testName), [1], 'the failed batch must be persisted by the final write')
}

/**
 * destroy()'s own final write must settle when it fails with only an
 * 'abort' event (as a commit-time failure such as QuotaExceededError does)
 * and an 'error' listener throws.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyFinalWriteAbortResolvesWhenErrorListenerThrows = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  // A long debounce keeps the edit pending, so destroy()'s final write is
  // the transaction that fails.
  const persistence = new IndexeddbPersistence(tc.testName, doc, { writeDebounceMs: 60000 })
  await persistence.whenSynced

  const listenerError = new Error('error listener failure')
  const throwingListener = () => { throw listenerError }
  const escapes = captureEscapedErrors()
  const intercept = interceptNextFlushTransaction(persistence)
  let errorEvents = 0
  persistence.on('error', () => { errorEvents++ })
  persistence.on('error', throwingListener)
  /** @type {'resolved'|'rejected'|'pending'} */
  let destroyStatus
  try {
    doc.getArray('t').insert(0, [1])
    t.assert(persistence._pendingUpdates.length === 1, 'the final write must carry exactly one add request')
    const destroyed = persistence.destroy()
    const finalTx = await intercept.next
    intercept.restore()
    // Abort once the only add has succeeded: no request is left pending, so
    // the transaction fires 'abort' alone — no bubbling request 'error'.
    finalTx.addEventListener('success', () => finalTx.abort(), { capture: true })
    destroyStatus = await settleWithin(destroyed)
  } finally {
    intercept.restore()
    escapes.restore()
    persistence.off('error', throwingListener)
  }

  t.assert(errorEvents === 1, `the failed final write should be reported once (got ${errorEvents})`)
  t.assert(destroyStatus === 'resolved', `destroy() must resolve when its final write aborts and an 'error' listener throws (destroy() is ${destroyStatus} after ${HANG_DEADLINE_MS}ms)`)
  assertOnlyListenerErrorEscaped(escapes.escaped, listenerError)
}

/**
 * destroy() must still resolve and close the connection when its final
 * write fails synchronously and an 'error' listener throws.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyFinalWriteThrowResolvesWhenErrorListenerThrows = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc, { writeDebounceMs: 60000 })
  await persistence.whenSynced

  const listenerError = new Error('error listener failure')
  const throwingListener = () => { throw listenerError }
  const escapes = captureEscapedErrors()
  const db = /** @type {IDBDatabase} */ (persistence.db)
  let closed = false
  const close = db.close
  db.close = function () {
    closed = true
    close.call(this)
  }
  let errorEvents = 0
  persistence.on('error', () => { errorEvents++ })
  persistence.on('error', throwingListener)
  /** @type {'resolved'|'rejected'|'pending'} */
  let destroyStatus
  try {
    doc.getArray('t').insert(0, [1])
    persistence.db = /** @type {any} */ ({
      transaction: () => { throw new Error('teardown write failed') }
    })
    destroyStatus = await settleWithin(persistence.destroy())
  } finally {
    escapes.restore()
    persistence.off('error', throwingListener)
  }

  t.assert(destroyStatus === 'resolved', `destroy() must resolve when its final write throws and an 'error' listener throws (destroy() is ${destroyStatus})`)
  t.assert(closed, 'destroy() must close the database connection')
  t.assert(errorEvents === 1, `the failed final write should be reported once (got ${errorEvents})`)
  assertOnlyListenerErrorEscaped(escapes.escaped, listenerError)
}

/**
 * With a serializing transactionRunner, a failed flush whose 'error'
 * listener throws must still release the runner: later runner-wrapped
 * writes (set) and teardown (destroy) must complete.
 *
 * @param {t.TestCase} tc
 */
export const testThrowingErrorListenerDoesNotWedgeSerializingRunner = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  let tail = Promise.resolve()
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const serializingRunner = work => {
    const run = tail.then(() => work())
    tail = run.then(() => undefined, () => undefined)
    return run
  }
  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: serializingRunner })
  await persistence.whenSynced

  const listenerError = new Error('error listener failure')
  const throwingListener = () => { throw listenerError }
  const escapes = captureEscapedErrors()
  const intercept = interceptNextFlushTransaction(persistence)
  persistence.on('error', throwingListener)
  /** @type {'resolved'|'rejected'|'pending'} */
  let setStatus
  /** @type {'resolved'|'rejected'|'pending'} */
  let destroyStatus = 'pending'
  /** @type {any} */
  let stored
  try {
    doc.getArray('t').insert(0, [1])
    const flushTx = await intercept.next
    intercept.restore()
    flushTx.abort()
    setStatus = await settleWithin(persistence.set('after-failure', 'ok'))
    if (setStatus === 'resolved') {
      stored = await persistence.get('after-failure')
      destroyStatus = await settleWithin(persistence.destroy())
    }
  } finally {
    intercept.restore()
    escapes.restore()
    persistence.off('error', throwingListener)
  }

  t.assert(setStatus === 'resolved', `set() through the serializing runner must complete after a failed flush whose 'error' listener threw (set() is ${setStatus} after ${HANG_DEADLINE_MS}ms)`)
  t.assert(stored === 'ok', 'set() must have written its value')
  t.assert(destroyStatus === 'resolved', `destroy() must complete (destroy() is ${destroyStatus} after ${HANG_DEADLINE_MS}ms)`)
  assertOnlyListenerErrorEscaped(escapes.escaped, listenerError)
  t.compareArrays(await readPersistedArray(tc.testName), [1], 'the failed batch must be persisted')
}
