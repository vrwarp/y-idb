/* eslint-env browser */

/**
 * Regression tests: README promises that the write on `pagehide` "opens its
 * transaction immediately instead of waiting for the runner", because the
 * page may be gone right after that event. With a serializing
 * transactionRunner that is busy (a trim, hydration, or another database's
 * write behind a shared lock), the flusher has already moved the buffered
 * edits into a flush that sits queued in the runner. pagehide must still
 * write those edits: once the page is gone the queued flush never runs.
 *
 * (lifecycle-runner's testPagehideWritesWhileHiddenFlushWaitsForBusyRunner
 * does not cover this: its long writeDebounceMs and synchronously fired
 * events keep the flusher from taking the edit before pagehide.)
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, readSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'
import * as promise from 'lib0/promise.js'

/**
 * Swaps in listener-capturing lifecycle globals for the duration of `fn`, so
 * the test can hide and unload the page the way a browser does (the node
 * harness installs no-op stubs). Providers must be constructed inside `fn`.
 *
 * `leavePage()` fires visibilitychange (to hidden) and then pagehide, as a
 * browser does when the tab is closed, and resolves once whatever the
 * pagehide listeners returned has settled. It never waits for the runner.
 *
 * @param {(leavePage: () => Promise<void>) => Promise<void>} fn
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
  const leavePage = async () => {
    fakeDocument.visibilityState = 'hidden'
    for (const handler of (listeners.get('visibilitychange') || []).slice()) handler()
    const results = (listeners.get('pagehide') || []).slice().map(handler => handler())
    await Promise.all(results.map(r => Promise.resolve(r).catch(() => {})))
    // Let the page-hide transaction commit if it has not yet.
    await promise.wait(50)
  }
  try {
    await fn(leavePage)
  } finally {
    g.addEventListener = originalAdd
    g.removeEventListener = originalRemove
    g.document = originalDocument
  }
}

/**
 * A serializing lock, as README recommends for WebKit, plus a way to hold it
 * (standing in for a long trim, a cold-start hydration, or another
 * database's write behind the same global lock).
 */
const createLockRunner = () => {
  /** @type {Promise<any>} */
  let tail = Promise.resolve()
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    const result = tail.then(() => work())
    tail = result.catch(() => {})
    return result
  }
  /**
   * Takes the lock until the returned function is called.
   * @return {() => void}
   */
  const hold = () => {
    /** @type {() => void} */
    let release = () => {}
    runner(() => new Promise(resolve => { release = () => resolve(undefined) }))
    return () => release()
  }
  return { runner, hold }
}

/**
 * The persisted content of the `t` array, read straight from the database
 * (as the next page load would see it).
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
 * The user types while the runner is held and closes the tab before it is
 * released. The flush of the edit is queued in the runner, so the edit is no
 * longer in the provider's buffer; the pagehide write found nothing to write
 * and the edit was lost.
 *
 * @param {t.TestCase} tc
 */
export const testPageHideWritesEditOfFlushQueuedInBusyRunner = async tc => {
  await clearDocument(tc.testName)
  await withPageLifecycle(async leavePage => {
    const { runner, hold } = createLockRunner()
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
    await persistence.whenSynced
    // Let the initial-sync chain (and its trailing _scheduleFlush) settle.
    await promise.wait(50)
    doc.getArray('t').insert(0, ['committed'])
    await persistence.flush()
    t.compareArrays(await readPersisted(tc.testName), ['committed'])

    const release = hold()
    doc.getArray('t').insert(1, ['typed while the runner was busy'])
    // The microtask flush takes the edit and queues behind the held lock.
    await promise.wait(20)
    // The tab is closed while the lock is still held: the queued flush
    // never gets to run.
    await leavePage()

    const persisted = await readPersisted(tc.testName)
    release()
    await persistence.destroy()
    t.compareArrays(persisted, ['committed', 'typed while the runner was busy'], 'the pagehide write must persist the edit whose flush was still queued in the busy runner')
    t.compareArrays(await readPersisted(tc.testName), ['committed', 'typed while the runner was busy'])
  })
  await clearDocument(tc.testName)
}

/**
 * Same, with a second edit made after the flush was queued. pagehide wrote
 * only the later edit, which cannot be applied on reload without the earlier
 * one (its clocks build on it), so neither edit survived.
 *
 * @param {t.TestCase} tc
 */
export const testPageHideWritesQueuedFlushBatchAlongWithLaterEdits = async tc => {
  await clearDocument(tc.testName)
  await withPageLifecycle(async leavePage => {
    const { runner, hold } = createLockRunner()
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
    await persistence.whenSynced
    await promise.wait(50)
    doc.getArray('t').insert(0, ['committed'])
    await persistence.flush()
    t.compareArrays(await readPersisted(tc.testName), ['committed'])

    const release = hold()
    doc.getArray('t').insert(1, ['first'])
    // The flush of 'first' is now queued behind the held lock.
    await promise.wait(20)
    doc.getArray('t').insert(2, ['second'])
    await leavePage()

    const persisted = await readPersisted(tc.testName)
    release()
    await persistence.destroy()
    t.compareArrays(persisted, ['committed', 'first', 'second'], 'the pagehide write must persist both the queued flush batch and the later edit')
    t.compareArrays(await readPersisted(tc.testName), ['committed', 'first', 'second'])
  })
  await clearDocument(tc.testName)
}

/**
 * Number of stored update rows (raw read, no provider, no runner).
 *
 * @param {string} name
 * @return {Promise<number>}
 */
const countRows = name => new Promise((resolve, reject) => {
  const req = indexedDB.open(name)
  req.onerror = () => reject(req.error)
  req.onsuccess = () => {
    const db = req.result
    const tx = db.transaction(['updates'], 'readonly')
    const count = tx.objectStore('updates').count()
    tx.oncomplete = () => {
      db.close()
      resolve(count.result)
    }
    tx.onerror = () => {
      db.close()
      reject(tx.error)
    }
  }
})

/**
 * The page survives pagehide (back/forward cache) and the runner later gets
 * to the flush whose batch the pagehide write took over. That flush must
 * open no second transaction for the batch, and must leave the flusher
 * idle: flush() resolves and later edits are written as usual.
 *
 * @param {t.TestCase} tc
 */
export const testFlushTakenOverByPageHideOpensNoTransactionOnceRunnerFrees = async tc => {
  await clearDocument(tc.testName)
  await withPageLifecycle(async leavePage => {
    const { runner, hold } = createLockRunner()
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
    await persistence.whenSynced
    await promise.wait(50)
    doc.getArray('t').insert(0, ['committed'])
    await persistence.flush()

    const release = hold()
    doc.getArray('t').insert(1, ['typed while the runner was busy'])
    await promise.wait(20)
    // Waits for the queued flush.
    const flushed = persistence.flush()
    await leavePage()
    const rowsAfterPageHide = await countRows(tc.testName)
    t.compareArrays(await readPersisted(tc.testName), ['committed', 'typed while the runner was busy'])

    // The runner now runs the queued flush work.
    release()
    await promise.wait(50)
    t.assert(await countRows(tc.testName) === rowsAfterPageHide, 'the flush whose batch pagehide wrote must not write it again')
    t.assert(!persistence._writing && persistence._flushPromise === null, 'the flusher must be idle')
    await Promise.race([flushed, promise.wait(2000).then(() => { throw new Error('flush() hung') })])

    doc.getArray('t').insert(2, ['after the page came back'])
    await Promise.race([persistence.flush(), promise.wait(2000).then(() => { throw new Error('flush() hung') })])
    await persistence.destroy()
    t.compareArrays(await readPersisted(tc.testName), ['committed', 'typed while the runner was busy', 'after the page came back'])
  })
  await clearDocument(tc.testName)
}
