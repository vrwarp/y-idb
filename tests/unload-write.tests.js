/* eslint-env browser */

/**
 * Regression tests: the write issued when the page becomes hidden
 * (visibilitychange -> hidden / pagehide) must be tracked like any other
 * flush. flush() and destroy() must wait for it, and when it fails the
 * failure must be reported through 'error', the batch kept and retried
 * (and included in destroy()'s final write) instead of silently parked in
 * memory or dropped.
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, readSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'
import * as promise from 'lib0/promise.js'

/**
 * Swaps in listener-capturing lifecycle globals for the duration of `fn`, so
 * the test can make the page hidden the way a browser does (the node harness
 * installs no-op stubs). Providers must be constructed inside `fn`.
 *
 * @param {(hidePage: () => void) => Promise<void>} fn
 */
const withPageLifecycle = async fn => {
  const g = /** @type {any} */ (globalThis)
  const originalAdd = g.addEventListener
  const originalRemove = g.removeEventListener
  const originalDocument = g.document
  /** @type {Map<string, Array<Function>>} */
  const listeners = new Map()
  const add = (/** @type {string} */ type, /** @type {Function} */ handler) => {
    const existing = listeners.get(type) || []
    existing.push(handler)
    listeners.set(type, existing)
  }
  const remove = (/** @type {string} */ type, /** @type {Function} */ handler) => {
    listeners.set(type, (listeners.get(type) || []).filter(h => h !== handler))
  }
  const fakeDocument = { addEventListener: add, removeEventListener: remove, visibilityState: 'visible' }
  g.addEventListener = add
  g.removeEventListener = remove
  g.document = fakeDocument
  const hidePage = () => {
    fakeDocument.visibilityState = 'hidden'
    for (const handler of (listeners.get('visibilitychange') || []).slice()) handler()
  }
  try {
    await fn(hidePage)
  } finally {
    g.addEventListener = originalAdd
    g.removeEventListener = originalRemove
    g.document = originalDocument
  }
}

/**
 * Records every readwrite transaction the provider opens on the `updates`
 * store from now on, and whether it has committed. With `abortFirst`, the
 * first such transaction is aborted on a microtask (after its add() requests
 * were queued, before they ran) — a transient write failure such as a quota
 * error or a connection that is closing.
 *
 * @param {IndexeddbPersistence} persistence
 * @param {{ abortFirst?: boolean }} [opts]
 * @return {Array<{ tx: IDBTransaction, committed: boolean }>}
 */
const trackUpdateWrites = (persistence, { abortFirst = false } = {}) => {
  const db = /** @type {IDBDatabase} */ (persistence.db)
  const originalTransaction = db.transaction
  /** @type {Array<{ tx: IDBTransaction, committed: boolean }>} */
  const writes = []
  // @ts-ignore
  db.transaction = function (storeNames, mode, options) {
    const tx = originalTransaction.call(this, storeNames, mode, options)
    const names = typeof storeNames === 'string' ? [storeNames] : Array.from(storeNames)
    if (mode === 'readwrite' && names.includes('updates')) {
      const entry = { tx, committed: false }
      // Registered before the provider sets tx.oncomplete, so it runs first.
      tx.addEventListener('complete', () => { entry.committed = true })
      writes.push(entry)
      if (abortFirst && writes.length === 1) {
        queueMicrotask(() => {
          try { tx.abort() } catch (e) {}
        })
      }
    }
    return tx
  }
  return writes
}

/**
 * The persisted content of the `t` array, read straight from the database.
 *
 * @param {string} name
 * @return {Promise<Array<any>>}
 */
const readPersisted = async name => {
  const update = await readSnapshot(name)
  const doc = new Y.Doc()
  if (update !== null) Y.applyUpdate(doc, update)
  return doc.getArray('t').toArray()
}

/**
 * Polls `cond` until it holds or `timeoutMs` passes. A bound, not a race:
 * the condition only depends on what the provider does on its own.
 *
 * @param {() => Promise<boolean>} cond
 * @param {number} timeoutMs
 * @return {Promise<boolean>}
 */
const eventually = async (cond, timeoutMs) => {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    if (await cond()) return true
    if (Date.now() >= deadline) return false
    await promise.wait(50)
  }
}

/**
 * README: flush() "resolves once every pending update ... has been
 * committed". An app typically awaits provider.flush() in its own
 * visibilitychange handler, which runs after the provider's. The provider's
 * handler has already moved the buffered edit into its own page-hide
 * transaction, so flush() saw an empty queue and resolved while that write
 * was still uncommitted.
 *
 * @param {t.TestCase} tc
 */
export const testFlushWaitsForPageHideWrite = async tc => {
  await clearDocument(tc.testName)
  await withPageLifecycle(async hidePage => {
    const doc = new Y.Doc()
    // A long debounce: only the page-hide path and flush() can write.
    const persistence = new IndexeddbPersistence(tc.testName, doc, { writeDebounceMs: 1000 })
    await persistence.whenSynced
    const writes = trackUpdateWrites(persistence)

    doc.getArray('t').insert(0, ['edit'])
    hidePage()
    await persistence.flush()

    t.assert(writes.length > 0, 'the edit should have been written')
    const uncommitted = writes.filter(w => !w.committed).length
    t.assert(uncommitted === 0, `flush() resolved while ${uncommitted} of ${writes.length} update write(s) (the page-hide write) had not committed`)
    t.assert((await readPersisted(tc.testName)).includes('edit'), 'the edit must be persisted once flush() resolved')

    await persistence.destroy()
  })
  await clearDocument(tc.testName)
}

/**
 * When the page-hide write fails and no further edit follows (the user has
 * left the tab), the failure must be reported through 'error' and the batch
 * retried on its own, like any failed flush. Before the fix it was only put
 * back into the in-memory queue: no 'error', no retry, and the edit was
 * never persisted.
 *
 * @param {t.TestCase} tc
 */
export const testFailedPageHideWriteIsReportedAndRetried = async tc => {
  await clearDocument(tc.testName)
  await withPageLifecycle(async hidePage => {
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc)
    await persistence.whenSynced
    // Let the initial-sync chain (and its trailing _scheduleFlush) settle.
    await promise.wait(50)
    /** @type {Array<any>} */
    const errors = []
    persistence.on('error', /** @param {any} err */ err => { errors.push(err) })
    const writes = trackUpdateWrites(persistence, { abortFirst: true })

    doc.getArray('t').insert(0, ['edit'])
    // Synchronously after the edit, before the microtask flush runs: the
    // page-hide write takes the batch and is the transaction that fails.
    hidePage()

    const persisted = await eventually(async () => (await readPersisted(tc.testName)).includes('edit'), 2500)
    t.assert(writes.length > 0 && writes[0].committed === false, 'the first update write should have been aborted')
    t.assert(errors.length > 0, 'a failed page-hide write must emit \'error\'')
    t.assert(persisted, 'a failed page-hide write must be retried until the edit is persisted (it stayed in memory only)')

    await persistence.destroy()
  })
  await clearDocument(tc.testName)
}

/**
 * The same failure in the ordering a browser produces: the microtask flush
 * of a first edit is already in flight when a second edit arrives and the
 * page is hidden. The page-hide write queues behind that flush and fails
 * only after it committed, so the flusher is idle with nothing scheduled
 * when the batch comes back — it must still be reported and retried.
 *
 * @param {t.TestCase} tc
 */
export const testPageHideWriteFailingAfterInFlightFlushIsRetried = async tc => {
  await clearDocument(tc.testName)
  await withPageLifecycle(async hidePage => {
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc)
    await persistence.whenSynced
    await promise.wait(50)
    /** @type {Array<any>} */
    const errors = []
    persistence.on('error', /** @param {any} err */ err => { errors.push(err) })
    const writes = trackUpdateWrites(persistence)

    doc.getArray('t').insert(0, ['first'])
    // Let the microtask flush of the first edit start its transaction.
    await Promise.resolve()
    t.assert(persistence._writing && writes.length === 1, 'the flush of the first edit should be in flight')
    doc.getArray('t').insert(1, ['second'])
    hidePage()
    t.assert(writes.length === 2, 'the page-hide write should open its own transaction')
    const [flushWrite, pageHideWrite] = writes
    // Registered after the provider's oncomplete, so the flusher has already
    // gone idle when the page-hide write fails.
    flushWrite.tx.addEventListener('complete', () => {
      try { pageHideWrite.tx.abort() } catch (e) {}
    })

    const persisted = await eventually(async () => (await readPersisted(tc.testName)).includes('second'), 2500)
    t.assert(flushWrite.committed && !pageHideWrite.committed, 'the page-hide write should have failed after the in-flight flush committed')
    t.assert(errors.length > 0, 'a failed page-hide write must emit \'error\'')
    t.assert(persisted, 'a failed page-hide write must be retried until the edit is persisted (it stayed in memory only)')

    await persistence.destroy()
  })
  await clearDocument(tc.testName)
}

/**
 * destroy() while the page-hide write is in flight, and that write then
 * fails: destroy() must wait for it and include the batch in its final
 * write. Before the fix destroy() found an empty queue, and the failing
 * page-hide transaction then saw the provider destroyed and dropped the
 * batch — the edit was lost with no 'error'.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyDuringFailedPageHideWriteKeepsEdit = async tc => {
  await clearDocument(tc.testName)
  await withPageLifecycle(async hidePage => {
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc)
    await persistence.whenSynced
    await promise.wait(50)
    const writes = trackUpdateWrites(persistence, { abortFirst: true })

    doc.getArray('t').insert(0, ['edit'])
    hidePage()
    // Teardown starts while the page-hide write is in flight; that write is
    // aborted right after (on a microtask).
    await persistence.destroy()

    t.assert(writes.length > 0 && writes[0].committed === false, 'the first update write should have been aborted')
    t.compareArrays(await readPersisted(tc.testName), ['edit'], 'destroy() must persist the batch of the failed page-hide write')
  })
  await clearDocument(tc.testName)
}
