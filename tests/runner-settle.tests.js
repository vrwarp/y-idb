/* eslint-env browser */

/**
 * Regression tests: a `transactionRunner` whose promise does not settle in
 * step with the transaction it wraps.
 *
 * - Settling AFTER the hydration transaction has committed (a lock released
 *   after commit, a setTimeout hop) must not keep whenSynced from resolving.
 * - Settling BEFORE the flush transaction it wraps (a deadline that rejects,
 *   or a watchdog that resolves, while the transaction is still stalled —
 *   the WebKit-hang mitigation the README describes) must not corrupt the
 *   flusher. Each flush attempt is concluded exactly once, from the
 *   transaction's own outcome; at most one flush transaction is in flight;
 *   flush() and destroy() wait for it.
 */

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

/**
 * Open `name` WITHOUT creating stores, on a connection independent of the
 * provider under test.
 *
 * @param {string} name
 * @return {Promise<IDBDatabase>}
 */
const openRaw = name => new Promise((resolve, reject) => {
  const request = indexedDB.open(name)
  request.onsuccess = () => resolve(request.result)
  request.onerror = () => reject(request.error)
})

/**
 * Number of rows in the `updates` store, read through an independent
 * connection.
 *
 * @param {string} name
 * @return {Promise<number>}
 */
const countUpdateRows = async name => {
  const db = await openRaw(name)
  try {
    return await new Promise((resolve, reject) => {
      const request = db.transaction(['updates'], 'readonly').objectStore('updates').count()
      request.onsuccess = () => resolve(request.result)
      request.onerror = () => reject(request.error)
    })
  } finally {
    db.close()
  }
}

/**
 * Hold a readwrite transaction on the `updates` store (from another
 * connection, like a long transaction of another tab) until `release()` —
 * a stalled store: every provider transaction on it stays pending until
 * then, so the test decides exactly when the flush transaction can settle.
 *
 * @param {string} name
 * @return {Promise<{ release: () => Promise<void> }>}
 */
const holdUpdatesStore = async name => {
  const db = await openRaw(name)
  const tx = db.transaction(['updates'], 'readwrite')
  const store = tx.objectStore('updates')
  let holding = true
  const keepBusy = () => {
    if (holding) {
      store.count().onsuccess = keepBusy
    }
  }
  keepBusy()
  const done = new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve(undefined)
    tx.onabort = () => reject(tx.error)
  })
  return {
    release: async () => {
      holding = false
      await done
      db.close()
    }
  }
}

/**
 * Observe the provider's flush transactions (readwrite on `updates` only)
 * opened from now on: how many are open at once, and when each settles.
 *
 * @param {IndexeddbPersistence} persistence
 */
const trackFlushTransactions = persistence => {
  const db = /** @type {any} */ (persistence.db)
  const originalTransaction = db.transaction
  const tracker = {
    /** @type {Array<IDBTransaction>} */
    txs: [],
    /** @type {Array<Promise<void>>} resolves when the i-th tx completed or aborted */
    settled: [],
    inFlight: 0,
    maxInFlight: 0,
    committed: 0
  }
  /**
   * @param {string|Array<string>} storeNames
   * @param {IDBTransactionMode} [mode]
   * @param {any} [options]
   */
  db.transaction = function (storeNames, mode, options) {
    const tx = originalTransaction.call(this, storeNames, mode, options)
    const names = typeof storeNames === 'string' ? [storeNames] : Array.from(storeNames)
    if (mode === 'readwrite' && names.length === 1 && names[0] === 'updates') {
      tracker.txs.push(tx)
      tracker.inFlight++
      tracker.maxInFlight = Math.max(tracker.maxInFlight, tracker.inFlight)
      tracker.settled.push(new Promise(resolve => {
        tx.addEventListener('complete', () => {
          tracker.committed++
          tracker.inFlight--
          resolve(undefined)
        })
        tx.addEventListener('abort', () => {
          tracker.inFlight--
          resolve(undefined)
        })
      }))
    }
    return tx
  }
  return tracker
}

/**
 * A runner with a deadline, like `Promise.race([work(), rejectAfter(ms)])`,
 * armed for the next write only. The test fires the deadline explicitly via
 * `expire()` while the wrapped transaction is still pending.
 */
const createDeadlineRunner = () => {
  const ctl = {
    armed: false,
    /** @type {null|((err: Error) => void)} */
    expire: null
  }
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    if (!ctl.armed) return work()
    ctl.armed = false
    return new Promise((resolve, reject) => {
      ctl.expire = reject
      work().then(resolve, reject)
    })
  }
  return { ctl, runner }
}

/**
 * The runner's deadline rejects while the flush transaction is stalled, and
 * that transaction then aborts. That is ONE failed flush attempt: it must
 * count as one retry (with maxRetries: 1 it must not exhaust the retries)
 * and the batch must be re-buffered once, i.e. persisted without duplicate
 * rows by the backoff retry.
 *
 * @param {t.TestCase} tc
 */
export const testRunnerDeadlineThenFlushAbortCountsAsOneFailure = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const { ctl, runner } = createDeadlineRunner()
  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner, maxRetries: 1 })
  await persistence.whenSynced
  await promise.wait(50)
  const rowsBefore = await countUpdateRows(tc.testName)

  let retryExhausted = 0
  persistence.on('retry-exhausted', () => { retryExhausted++ })
  const flushTxs = trackFlushTransactions(persistence)
  const blocker = await holdUpdatesStore(tc.testName)

  ctl.armed = true
  doc.getArray('t').insert(0, [1])
  // The flush microtask queued by the insert runs first: the flush
  // transaction is open, stuck behind the blocker.
  await Promise.resolve()
  t.assert(flushTxs.txs.length === 1 && ctl.expire !== null, 'flush transaction should be pending behind the blocker')

  const expireDeadline = /** @type {(err: Error) => void} */ (ctl.expire)
  // The deadline expires while the transaction is still stalled ...
  expireDeadline(new Error('transactionRunner deadline exceeded'))
  await promise.wait(0)
  // ... and the stalled transaction then fails.
  try { flushTxs.txs[0].abort() } catch (e) { /* already concluded */ }
  await flushTxs.settled[0]
  await blocker.release()

  // Let the backoff retry write the batch.
  await persistence.flush()
  const rowsWritten = (await countUpdateRows(tc.testName)) - rowsBefore

  t.assert(retryExhausted === 0, `one failed flush attempt exhausted maxRetries: 1 ('retry-exhausted' emitted ${retryExhausted}x; the failure was counted twice)`)
  t.assert(rowsWritten === 1, `one update must be persisted as exactly one row (got ${rowsWritten}: the failed batch was re-buffered twice)`)

  await persistence.destroy()
}

/**
 * The runner's deadline rejects while the flush transaction T1 is stalled;
 * a second update arrives, and T1 later commits. The flusher must keep at
 * most one flush transaction in flight throughout (no retry transaction
 * opened beside the still-pending T1, no new flush beside a pending retry),
 * and flush() must not resolve while the second update is still uncommitted.
 *
 * @param {t.TestCase} tc
 */
export const testRunnerDeadlineThenLateCommitKeepsOneFlushInFlight = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const { ctl, runner } = createDeadlineRunner()
  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
  await persistence.whenSynced
  await promise.wait(50)

  const flushTxs = trackFlushTransactions(persistence)
  const blocker = await holdUpdatesStore(tc.testName)

  ctl.armed = true
  doc.getArray('t').insert(0, [1])
  await Promise.resolve()
  t.assert(flushTxs.txs.length === 1 && ctl.expire !== null, 'flush transaction should be pending behind the blocker')

  const expireDeadline = /** @type {(err: Error) => void} */ (ctl.expire)
  expireDeadline(new Error('transactionRunner deadline exceeded'))
  // An update T1 does not carry arrives before the documented first backoff
  // (200ms) ends ...
  await promise.wait(100)
  doc.getArray('t').insert(1, [2])
  // ... and T1 stays stalled past that backoff. Timers fire in expiry order,
  // so a backoff retry armed by the rejection above has run before this
  // wait ends.
  await promise.wait(200)

  // Unblock the store: T1 now settles (commits).
  const releasing = blocker.release()
  await flushTxs.settled[0]
  // Right after T1's complete event: no other flush transaction can have
  // committed yet, so the second update must still be awaited. (Reading the
  // store cannot show this: a reader queues behind any pending readwrite
  // transaction and so always sees its rows.)
  await persistence.flush()
  const uncommittedWhenFlushResolved = flushTxs.inFlight
  // An edit now must not open a flush transaction beside a pending one.
  doc.getArray('t').insert(2, [3])
  await persistence.flush()
  await releasing
  await Promise.all(flushTxs.settled)

  t.assert(uncommittedWhenFlushResolved === 0, `flush() resolved while ${uncommittedWhenFlushResolved} flush transaction(s), carrying the second update, were still uncommitted`)
  t.assert(flushTxs.maxInFlight <= 1, `at most 1 flush transaction may be in flight, but ${flushTxs.maxInFlight} were open at once (${flushTxs.txs.length} opened in total)`)

  await persistence.destroy()
  const doc2 = new Y.Doc()
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced
  t.compareArrays(doc2.getArray('t').toArray(), [1, 2, 3])
  await persistence2.destroy()
}

/**
 * More `then` calls than this on promises derived from the runner's result
 * within one event-loop turn means something is re-awaiting an
 * already-settled promise in a loop (healthy code makes a handful).
 */
const SPIN_LIMIT = 1000

/**
 * The flush transaction is stalled and the runner's watchdog gives up and
 * RESOLVES, like `Promise.race([work(), resolveAfter(ms)])`. flush() must
 * still wait for the transaction to commit — by yielding to the event loop,
 * not by re-awaiting the already-resolved runner promise in a microtask loop,
 * which starves the event loop so the IDB `complete` event can never be
 * dispatched (a frozen page).
 *
 * The runner returns a Promise subclass so such a spin is observable: every
 * `await` of a promise derived from it calls its `then`. Past SPIN_LIMIT
 * calls in one turn, the probe settles the await on a macrotask instead, so
 * the spinning code can make progress and the test reports a failure rather
 * than hanging the runner.
 *
 * @param {t.TestCase} tc
 */
export const testWatchdogRunnerResolvingEarlyFlushYieldsUntilCommit = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()

  const probe = { callsThisTurn: 0, maxCallsPerTurn: 0 }
  /**
   * @template T
   * @extends {Promise<T>}
   */
  class SpinProbePromise extends Promise {
    /**
     * @param {any} [onFulfilled]
     * @param {any} [onRejected]
     * @return {Promise<any>}
     */
    then (onFulfilled, onRejected) {
      if (probe.callsThisTurn++ === 0) {
        setTimeout(() => { probe.callsThisTurn = 0 }, 0)
      }
      probe.maxCallsPerTurn = Math.max(probe.maxCallsPerTurn, probe.callsThisTurn)
      if (probe.callsThisTurn > SPIN_LIMIT) {
        return super.then(
          /** @param {any} value */
          value => new Promise(resolve => setTimeout(resolve, 0))
            .then(() => onFulfilled ? onFulfilled(value) : value),
          onRejected
        )
      }
      return super.then(onFulfilled, onRejected)
    }
  }

  let watchdogNextWrite = false
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    if (!watchdogNextWrite) return work()
    watchdogNextWrite = false
    // The watchdog's deadline has already passed: it resolves before the
    // stalled transaction settles.
    return /** @type {Promise<any>} */ (SpinProbePromise.race([work(), SpinProbePromise.resolve(undefined)]))
  }

  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
  await persistence.whenSynced
  await promise.wait(50)
  const flushTxs = trackFlushTransactions(persistence)

  watchdogNextWrite = true
  doc.getArray('t').insert(0, [1])
  await persistence.flush()
  const committedWhenFlushResolved = flushTxs.committed

  t.assert(probe.maxCallsPerTurn <= SPIN_LIMIT, `flush() re-awaited an already-settled promise more than ${SPIN_LIMIT} times without yielding to the event loop (it spins forever: the page would freeze)`)
  t.assert(flushTxs.txs.length === 1 && committedWhenFlushResolved === 1, 'flush() must resolve only after the flush transaction committed')

  await persistence.destroy()
}

/**
 * Same early-resolving watchdog runner; destroy() is called while the flush
 * transaction is in flight and that transaction then fails. destroy() must
 * wait for it to settle so the re-buffered batch is included in its final
 * write — not snapshot the queue early and lose the batch.
 *
 * @param {t.TestCase} tc
 */
export const testWatchdogRunnerResolvingEarlyDestroyPersistsFailedFlush = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  let watchdogNextWrite = false
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    if (!watchdogNextWrite) return work()
    watchdogNextWrite = false
    return /** @type {Promise<any>} */ (Promise.race([work(), Promise.resolve(undefined)]))
  }
  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
  await persistence.whenSynced
  await promise.wait(50)
  const flushTxs = trackFlushTransactions(persistence)

  watchdogNextWrite = true
  doc.getArray('t').insert(0, [1])
  await Promise.resolve()
  t.assert(flushTxs.txs.length === 1, 'flush transaction should be in flight')
  const destroyed = persistence.destroy()
  // The in-flight flush fails after destroy() was called.
  try { flushTxs.txs[0].abort() } catch (e) { /* already concluded */ }
  await destroyed

  const doc2 = new Y.Doc()
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced
  const persisted = doc2.getArray('t').toArray()
  t.compareArrays(persisted, [1], `the failed in-flight batch must be persisted by destroy() (persisted: ${JSON.stringify(persisted)})`)
  await persistence2.destroy()
}

/**
 * A runner that resolves WITHOUT running the work (e.g. a lock requested
 * with `ifAvailable` while it is taken) also settles before any flush
 * transaction does — there is none. The attempt must still conclude as a
 * failure: 'error' is emitted, the batch is re-buffered, and the backoff
 * retry persists it, instead of the flusher waiting forever for a
 * transaction that will never be opened.
 *
 * @param {t.TestCase} tc
 */
export const testRunnerResolvingWithoutRunningFlushRetriesBatch = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  let skipNextWrite = false
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    if (!skipNextWrite) return work()
    skipNextWrite = false
    return /** @type {Promise<any>} */ (Promise.resolve(undefined))
  }
  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
  await persistence.whenSynced
  await promise.wait(50)
  const rowsBefore = await countUpdateRows(tc.testName)
  let errors = 0
  persistence.on('error', () => { errors++ })

  skipNextWrite = true
  doc.getArray('t').insert(0, [1])
  // Past the first backoff (200ms): the retry has opened its transaction,
  // which the row count below queues behind.
  await promise.wait(300)
  const rowsWritten = (await countUpdateRows(tc.testName)) - rowsBefore

  t.assert(errors === 1, `the attempt the runner skipped must be reported once ('error' emitted ${errors}x)`)
  t.assert(rowsWritten === 1, `the skipped batch must be re-buffered and persisted by the retry (got ${rowsWritten} rows)`)

  await persistence.destroy()
}

/**
 * The runner rejects before running the work (a lock queue's timeout) but
 * still runs it later (the timeout does not dequeue it). The rejection
 * concluded that attempt and re-buffered its batch, so the late work must
 * not open a stale flush transaction: the batch is written once, by the
 * backoff retry.
 *
 * @param {t.TestCase} tc
 */
export const testRunnerRejectingBeforeRunningFlushOpensNoStaleTransaction = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const deferred = {
    armed: false,
    /** @type {null|(() => Promise<any>)} */
    work: null
  }
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    if (!deferred.armed) return work()
    deferred.armed = false
    deferred.work = work
    return Promise.reject(new Error('transactionRunner lock timeout'))
  }
  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
  await persistence.whenSynced
  await promise.wait(50)
  const rowsBefore = await countUpdateRows(tc.testName)
  const flushTxs = trackFlushTransactions(persistence)

  deferred.armed = true
  doc.getArray('t').insert(0, [1])
  await promise.wait(0)
  t.assert(deferred.work !== null, 'the flush should have gone through the runner')
  // The lock frees up: the queued work runs after all.
  await /** @type {() => Promise<any>} */ (deferred.work)()
  await persistence.flush()
  const rowsWritten = (await countUpdateRows(tc.testName)) - rowsBefore

  t.assert(flushTxs.txs.length === 1, `the late work of a concluded attempt opened a stale flush transaction (${flushTxs.txs.length} opened for one update)`)
  t.assert(rowsWritten === 1, `one update must be persisted as exactly one row (got ${rowsWritten})`)

  await persistence.destroy()
}
