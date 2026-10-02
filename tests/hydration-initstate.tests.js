/* eslint-env browser */

/**
 * Regression tests: content a Y.Doc already holds when the provider attaches
 * (e.g. Zustand state mirrored into the doc at creation) must be persisted
 * even when the hydration transaction that normally carries it does not
 * commit — the transactionRunner rejects it, the initial write throws, it
 * aborts at commit time, or destroy() cuts it short.
 *
 * Every later update from the same client has clocks above that initial
 * content, so if it is lost the rows that WERE written cannot integrate on
 * reload either: they sit in `doc.store.pendingStructs` and the user sees
 * an empty document.
 */

import * as Y from 'yjs'
import * as t from 'lib0/testing.js'
import { IndexeddbPersistence, clearDocument } from '../src/y-idb.js'

/**
 * Reopen database `name` in a fresh doc, exactly like a page reload would,
 * and report what the user sees.
 *
 * @param {string} name
 * @return {Promise<{ content: Array<any>, pending: boolean }>}
 */
const reload = async name => {
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(name, doc)
  await persistence.whenSynced
  const content = doc.getArray('t').toArray()
  const pending = doc.store.pendingStructs !== null
  await persistence.destroy()
  doc.destroy()
  return { content, pending }
}

/**
 * A doc that already holds ['A'] before any provider is attached.
 *
 * @return {Y.Doc}
 */
const docWithInitialContent = () => {
  const doc = new Y.Doc()
  doc.getArray('t').insert(0, ['A'])
  return doc
}

/**
 * @param {IndexeddbPersistence} persistence
 * @param {string} eventName
 * @return {Promise<any>}
 */
const nextEvent = (persistence, eventName) =>
  new Promise(resolve => {
    /**
     * @param {any} arg
     */
    const handler = arg => {
      persistence.off(eventName, handler)
      resolve(arg)
    }
    persistence.on(eventName, handler)
  })

/**
 * Make the FIRST readwrite transaction opened on database `name` fail at
 * commit time, the way a browser aborts a transaction whose writes cannot be
 * made durable (QuotaExceededError on a large row, WebKit background abort):
 * every request in it succeeds, then the transaction aborts instead of
 * firing 'complete'. Implemented on top of fake-indexeddb's transaction
 * scheduler (`_start` runs once per queued request and finally commits).
 *
 * @param {string} name
 * @return {{ aborted: Promise<void>, restore: () => void }}
 */
const failFirstReadwriteCommit = name => {
  const realTransaction = IDBDatabase.prototype.transaction
  let armed = true
  /** @type {() => void} */
  let markAborted = () => {}
  /** @type {Promise<void>} */
  const aborted = new Promise(resolve => { markAborted = resolve })
  // @ts-ignore - override the prototype to inject a commit-time failure
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    if (armed && this.name === name && tx.mode === 'readwrite') {
      armed = false
      tx.addEventListener('abort', () => markAborted())
      const fakeTx = /** @type {any} */ (tx)
      const realStart = fakeTx._start
      fakeTx._start = function () {
        const allRequestsDone = this._requests.every(/** @param {any} r */ r => r.request.readyState === 'done')
        if (allRequestsDone && this._state !== 'finished') {
          // Commit point reached: abort instead of committing.
          this._abort('QuotaExceededError')
          return
        }
        return realStart.call(this)
      }
    }
    return tx
  }
  return {
    aborted,
    restore: () => { IDBDatabase.prototype.transaction = realTransaction }
  }
}

/**
 * The transactionRunner rejects the hydration call (the first call it gets).
 * The pre-existing ['A'] must still reach the database; the edit made after
 * the failure must reload together with it.
 *
 * @param {t.TestCase} tc
 */
export const testInitialStatePersistedWhenRunnerRejectsHydration = async tc => {
  await clearDocument(tc.testName)
  const doc = docWithInitialContent()

  let calls = 0
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = async work => {
    if (calls++ === 0) {
      throw new Error('runner rejects the hydration transaction')
    }
    return work()
  }

  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
  await nextEvent(persistence, 'error')
  t.assert(!persistence.synced, 'precondition: hydration failed')

  doc.getArray('t').insert(1, ['B'])
  await persistence.flush()
  await persistence.destroy()

  const { content, pending } = await reload(tc.testName)
  t.compare(content, ['A', 'B'], 'pre-existing content and the later edit must both reload')
  t.assert(!pending, 'no persisted row may be left dangling in pendingStructs')
}

/**
 * The hydration transaction aborts at commit (QuotaExceededError). That is a
 * failed write: it must be reported through 'error', and the pre-existing
 * ['A'] must be written later instead of silently dropped.
 *
 * @param {t.TestCase} tc
 */
export const testInitialStatePersistedWhenHydrationAbortsAtCommit = async tc => {
  await clearDocument(tc.testName)
  const hook = failFirstReadwriteCommit(tc.testName)
  try {
    const doc = docWithInitialContent()
    const persistence = new IndexeddbPersistence(tc.testName, doc)
    /** @type {Array<any>} */
    const errors = []
    persistence.on('error', /** @param {any} err */ err => { errors.push(err) })

    await hook.aborted
    hook.restore()
    // Let the provider observe the abort (its handlers run in the same
    // event dispatch, before ours resumes; settle any follow-up microtasks).
    await new Promise(resolve => setTimeout(resolve, 0))
    // Count only what the abort itself reported, so later errors cannot
    // stand in for it.
    const abortErrors = errors.length

    doc.getArray('t').insert(1, ['B'])
    await persistence.flush()
    await persistence.destroy()

    const { content, pending } = await reload(tc.testName)
    t.compare(content, ['A', 'B'], 'pre-existing content and the later edit must both reload')
    t.assert(!pending, 'no persisted row may be left dangling in pendingStructs')
    t.assert(abortErrors > 0, 'a hydration transaction that aborts at commit must emit "error"')
  } finally {
    hook.restore()
  }
}

/**
 * destroy() runs after the database opened but before the hydration read
 * resolved. destroy() still writes the buffered edit ['B'], so it must not
 * leave behind the pre-existing ['A'] that edit depends on.
 *
 * @param {t.TestCase} tc
 */
export const testInitialStatePersistedWhenDestroyedDuringHydration = async tc => {
  await clearDocument(tc.testName)
  const doc = docWithInitialContent()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  // The constructor registered its _db.then() (which starts hydration)
  // before this await did, so it runs first; the hydration read is still
  // pending when this await resumes.
  await persistence._db
  t.assert(!persistence.synced, 'precondition: hydration still in flight')

  doc.getArray('t').insert(1, ['B'])
  await persistence.destroy()

  const { content, pending } = await reload(tc.testName)
  t.compare(content, ['A', 'B'], 'pre-existing content and the edit flushed by destroy() must both reload')
  t.assert(!pending, 'no persisted row may be left dangling in pendingStructs')
}

/**
 * The initial-state write itself fails (the add request throws). That
 * rejects the hydration chain without any transactionRunner involved; the
 * pre-existing ['A'] must still reach the database.
 *
 * @param {t.TestCase} tc
 */
export const testInitialStatePersistedWhenInitialWriteThrows = async tc => {
  await clearDocument(tc.testName)
  const realAdd = IDBObjectStore.prototype.add
  // @ts-ignore - override the prototype to make the first add on this db throw
  IDBObjectStore.prototype.add = function (value, key) {
    if (this.transaction.db.name !== tc.testName) {
      return key === undefined ? realAdd.call(this, value) : realAdd.call(this, value, key)
    }
    IDBObjectStore.prototype.add = realAdd
    throw new DOMException('initial-state write rejected', 'QuotaExceededError')
  }
  try {
    const doc = docWithInitialContent()
    const persistence = new IndexeddbPersistence(tc.testName, doc)
    await nextEvent(persistence, 'error')
    t.assert(!persistence.synced, 'precondition: hydration failed')

    doc.getArray('t').insert(1, ['B'])
    await persistence.flush()
    await persistence.destroy()

    const { content, pending } = await reload(tc.testName)
    t.compare(content, ['A', 'B'], 'pre-existing content and the later edit must both reload')
    t.assert(!pending, 'no persisted row may be left dangling in pendingStructs')
  } finally {
    IDBObjectStore.prototype.add = realAdd
  }
}
