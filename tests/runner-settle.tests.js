/* eslint-env browser */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'
import * as promise from 'lib0/promise.js'

/**
 * Observes every transaction opened on the database `dbName` and records a
 * promise that resolves once that transaction has finished (committed or
 * aborted). The listeners are attached at creation time, so they observe the
 * real commit regardless of when (or whether) the library assigns its own
 * handlers.
 *
 * @param {string} dbName
 */
const trackTransactions = dbName => {
  /** @type {Array<Promise<void>>} */
  const finished = []
  const realTransaction = IDBDatabase.prototype.transaction
  // @ts-ignore - override the prototype to observe every transaction's end
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    if (this.name === dbName) {
      finished.push(new Promise(resolve => {
        tx.addEventListener('complete', () => resolve())
        tx.addEventListener('abort', () => resolve())
      }))
    }
    return tx
  }
  return {
    finished,
    restore: () => { IDBDatabase.prototype.transaction = realTransaction }
  }
}

/**
 * A transactionRunner modelled on a lock that is released only after the
 * work's transactions have committed (or, equivalently, any runner whose
 * returned promise settles in a later task than the one in which work()
 * resolved: a lock released via setTimeout/postMessage, a scheduler.yield
 * between transactions, ...). The returned promise therefore settles strictly
 * AFTER the IndexedDB `complete` event of the transaction(s) the work opened.
 *
 * `released` resolves the first time the runner hands a result back, i.e.
 * once the constructor's hydration fetch has been released to the library.
 *
 * @param {Array<Promise<void>>} finished
 */
const createLockReleasedAfterCommitRunner = finished => {
  /** @type {function(): void} */
  let markReleased = () => {}
  /** @type {Promise<void>} */
  const released = new Promise(resolve => { markReleased = resolve })
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = async work => {
    const before = finished.length
    const result = await work()
    // Hold the "lock" until every transaction the work opened has finished.
    await Promise.all(finished.slice(before))
    markReleased()
    return result
  }
  return { runner, released }
}

/**
 * Generous bound used only to detect a hang. With a correct implementation
 * 'synced' fires no later than a few microtasks after the runner releases
 * the hydration result, so this is never reached unless the emit was lost.
 */
const HANG_TIMEOUT_MS = 1000

/**
 * whenSynced must resolve (and 'synced' must fire, with `synced === true`)
 * once the hydration transaction has committed, regardless of WHEN the
 * transactionRunner settles its returned promise. Here the runner settles
 * only after the hydration transaction's `complete` event has already been
 * dispatched — previously the library attached its emit handler to that
 * transaction only after the runner settled, so it never ran and whenSynced
 * hung forever.
 *
 * @param {t.TestCase} tc
 */
export const testWhenSyncedResolvesWhenRunnerSettlesAfterCommit = async tc => {
  await clearDocument(tc.testName)
  const tracker = trackTransactions(tc.testName)
  /** @type {IndexeddbPersistence|null} */
  let persistence = null
  try {
    const { runner, released } = createLockReleasedAfterCommitRunner(tracker.finished)
    const doc = new Y.Doc()
    doc.getMap('m').set('initial', 'state')
    persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
    let syncedEvents = 0
    persistence.on('synced', () => { syncedEvents++ })

    // The runner has released the hydration result, strictly after the
    // hydration transaction committed.
    await released
    t.assert(tracker.finished.length > 0, 'hydration opened a transaction')

    const outcome = await Promise.race([
      persistence.whenSynced.then(() => 'synced'),
      promise.wait(HANG_TIMEOUT_MS).then(() => 'timeout')
    ])
    t.assert(outcome === 'synced', `whenSynced must resolve once hydration committed (got ${outcome})`)
    t.assert(persistence.synced === true, 'persistence.synced must be true after hydration')
    t.assert(syncedEvents === 1, `'synced' must be emitted exactly once (got ${syncedEvents})`)
  } finally {
    tracker.restore()
    if (persistence) await persistence.destroy()
  }
}

/**
 * The everyday form of the trigger: a runner that adds one task-level hop
 * (here a setTimeout) after the work resolves. Under fake-indexeddb the
 * hydration transaction always commits before that hop ends, so this is
 * deterministic; in browsers it is a race against commit latency.
 *
 * Also covers the post-sync flush: an edit made before the database opened
 * is buffered, and only the 'synced' path schedules the flush that drains it.
 *
 * @param {t.TestCase} tc
 */
export const testWhenSyncedResolvesWithSetTimeoutRunner = async tc => {
  await clearDocument(tc.testName)
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = async work => {
    const result = await work()
    await new Promise(resolve => setTimeout(resolve, 0))
    return result
  }
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
  try {
    doc.getMap('m').set('early', 'edit')
    t.assert(persistence._pendingUpdates.length === 1, 'pre-open edit is buffered')

    const outcome = await Promise.race([
      persistence.whenSynced.then(() => 'synced'),
      promise.wait(HANG_TIMEOUT_MS).then(() => 'timeout')
    ])
    t.assert(outcome === 'synced', `whenSynced must resolve once hydration committed (got ${outcome})`)
    t.assert(persistence.synced === true, 'persistence.synced must be true after hydration')

    await promise.until(HANG_TIMEOUT_MS, () => persistence._pendingUpdates.length === 0 && !persistence._writing)
  } finally {
    await persistence.destroy()
  }
}
