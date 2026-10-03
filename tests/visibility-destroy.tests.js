/* eslint-env browser */

/**
 * Regression tests: with a transactionRunner, the write issued on
 * visibilitychange -> hidden waits for the runner. If destroy() runs while
 * that write is still queued (a component unmounts or the route changes
 * right after a tab switch, while another write holds the lock), destroy()
 * must still write every buffered update, wait for that write to commit,
 * and report a failure via 'error', as it does when its own final write
 * fails. The queued hide write used to start after destroy(), take the
 * buffered batch away from destroy()'s final write, and drop it silently if
 * its transaction failed. destroy() also resolved before it committed.
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, readSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

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
 * A FIFO lock used as the transactionRunner (the global lock the README
 * describes). Work runs at once when the lock is free and is queued
 * otherwise. `hold()` takes the lock for some other client (another
 * provider's write); the `release` it resolves with starts the next queued
 * work synchronously, so the test decides exactly when that work starts
 * relative to the provider's own calls, independent of microtask counts.
 */
const createLock = () => {
  /** @type {Array<() => void>} */
  const waiting = []
  let busy = false
  const next = () => {
    const start = waiting.shift()
    if (start) {
      start()
    } else {
      busy = false
    }
  }
  /**
   * @param {() => void} start
   */
  const acquire = start => {
    if (busy) {
      waiting.push(start)
    } else {
      busy = true
      start()
    }
  }
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => new Promise((resolve, reject) => {
    acquire(() => {
      /** @type {Promise<T>} */
      let run
      try {
        run = Promise.resolve(work())
      } catch (e) {
        run = Promise.reject(e)
      }
      run.then(resolve, reject).then(next, next)
    })
  })
  /**
   * @return {Promise<() => void>} resolves with `release` once the lock is held
   */
  const hold = () => new Promise(resolve => {
    acquire(() => resolve(next))
  })
  return { runner, hold }
}

/**
 * Records every readwrite transaction the provider opens on the `updates`
 * store from now on, and whether it has settled. With `abortNth`, the nth
 * such transaction (counting from 1) is aborted on a microtask (after its
 * add() requests were queued, before they ran): a transient write failure
 * such as a quota error.
 *
 * @param {IndexeddbPersistence} persistence
 * @param {{ abortNth?: number }} [opts]
 * @return {Array<{ committed: boolean, settled: boolean }>}
 */
const trackUpdateWrites = (persistence, { abortNth = 0 } = {}) => {
  const db = /** @type {IDBDatabase} */ (persistence.db)
  const originalTransaction = db.transaction
  /** @type {Array<{ committed: boolean, settled: boolean }>} */
  const writes = []
  // @ts-ignore
  db.transaction = function (storeNames, mode, options) {
    const tx = originalTransaction.call(this, storeNames, mode, options)
    const names = typeof storeNames === 'string' ? [storeNames] : Array.from(storeNames)
    if (mode === 'readwrite' && names.includes('updates')) {
      const entry = { committed: false, settled: false }
      // Registered before the provider sets its handlers, so they run first.
      tx.addEventListener('complete', () => { entry.committed = true; entry.settled = true })
      tx.addEventListener('abort', () => { entry.settled = true })
      writes.push(entry)
      if (writes.length === abortNth) {
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
  const content = doc.getArray('t').toArray()
  doc.destroy()
  return content
}

/**
 * Sets up the race: an edit is persisted, then another client holds the
 * lock, the user edits and switches tabs (the hide write queues behind the
 * lock), and the provider is destroyed right before the lock frees.
 *
 * @param {t.TestCase} tc
 * @param {(hidePage: () => void) => Promise<void>} fn
 */
const withDestroyRacingQueuedHideWrite = async (tc, fn) => {
  await clearDocument(tc.testName)
  await withPageLifecycle(fn)
  await clearDocument(tc.testName)
}

/**
 * The queued hide write must not take the last edit away from destroy()
 * and lose it without a report. Its transaction fails here (as a
 * QuotaExceededError would): the edit must still be persisted (destroy()'s
 * final write includes it) or, if nothing could write it, the failure must
 * be reported through 'error', as destroy() does when its own final write
 * fails. Previously the edit was dropped and no 'error' fired.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyDoesNotSilentlyDropBatchTakenByQueuedHideWrite = async tc => {
  await withDestroyRacingQueuedHideWrite(tc, async hidePage => {
    const { runner, hold } = createLock()
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc, {
      transactionRunner: runner,
      // Far longer than the test: only flush(), the hide write and destroy()
      // can write.
      writeDebounceMs: 60_000
    })
    await persistence.whenSynced
    /** @type {Array<any>} */
    const errors = []
    persistence.on('error', (/** @type {any} */ err) => { errors.push(err) })
    doc.getArray('t').push(['a'])
    await persistence.flush()

    // Another client's write holds the lock.
    const release = await hold()
    doc.getArray('t').push(['b'])
    // The next write transaction fails.
    trackUpdateWrites(persistence, { abortNth: 1 })
    // Tab switch: the hide write queues behind the lock.
    hidePage()
    // Unmount right after the tab switch, just before the lock frees.
    const destroyed = persistence.destroy()
    release()
    await destroyed

    const persisted = await readPersisted(tc.testName)
    t.assert(persisted.includes('a'), `the flushed edit must be persisted, got ${JSON.stringify(persisted)}`)
    t.assert(
      persisted.includes('b') || errors.length > 0,
      `the last edit must be persisted or its loss reported via 'error'; persisted ${JSON.stringify(persisted)}, ${errors.length} error(s) emitted`
    )
  })
}

/**
 * `await provider.destroy()` must mean the buffered updates are committed,
 * also when the queued hide write starts after destroy() and takes them.
 * Previously destroy() resolved while that write's transaction was still
 * pending.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyWaitsForQueuedHideWriteToCommit = async tc => {
  await withDestroyRacingQueuedHideWrite(tc, async hidePage => {
    const { runner, hold } = createLock()
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc, {
      transactionRunner: runner,
      writeDebounceMs: 60_000
    })
    await persistence.whenSynced
    /** @type {Array<any>} */
    const errors = []
    persistence.on('error', (/** @type {any} */ err) => { errors.push(err) })
    doc.getArray('t').push(['a'])
    await persistence.flush()

    const release = await hold()
    doc.getArray('t').push(['b'])
    const writes = trackUpdateWrites(persistence)
    hidePage()
    const destroyed = persistence.destroy()
    release()
    await destroyed

    const pending = writes.filter(w => !w.settled).length
    t.assert(
      pending === 0,
      `destroy() must resolve only after the write holding the last edit settled: ${pending} of ${writes.length} write transaction(s) still pending`
    )
    t.assert(writes.some(w => w.committed), 'the last edit must be written by a committed transaction')
    t.assert(errors.length === 0, `no write failed, yet ${errors.length} error(s) were emitted`)
    const persisted = await readPersisted(tc.testName)
    t.compareArrays(persisted, ['a', 'b'])
  })
}

/**
 * The same race with an ordinary promise-chain mutex as the runner and the
 * provider's own in-flight flush holding the lock: when that flush commits,
 * the mutex starts the queued hide write a few microtasks later, before
 * destroy() (waiting for the same flush) reaches its final write. The race
 * does not depend on the lock above starting queued work synchronously.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyKeepsBatchWhenOwnFlushHoldsMutex = async tc => {
  await withDestroyRacingQueuedHideWrite(tc, async hidePage => {
    let tail = Promise.resolve()
    /**
     * @template T
     * @param {() => Promise<T>} work
     * @return {Promise<T>}
     */
    const runner = work => {
      const run = tail.then(() => work())
      tail = run.then(() => {}, () => {})
      return run
    }
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc, {
      transactionRunner: runner,
      writeDebounceMs: 0
    })
    await persistence.whenSynced
    /** @type {Array<any>} */
    const errors = []
    persistence.on('error', (/** @type {any} */ err) => { errors.push(err) })
    // The write after the flush fails.
    const writes = trackUpdateWrites(persistence, { abortNth: 2 })
    doc.getArray('t').push(['b'])
    // Wait (in microtasks only, so it cannot commit yet) until the flush of
    // 'b' has opened its transaction and holds the mutex.
    for (let i = 0; i < 100 && writes.length === 0; i++) {
      await Promise.resolve()
    }
    t.assert(writes.length === 1, 'precondition: the flush of the first edit holds the mutex')
    doc.getArray('t').push(['c'])
    hidePage()
    await persistence.destroy()

    const persisted = await readPersisted(tc.testName)
    t.assert(persisted.includes('b'), `the flushed edit must be persisted, got ${JSON.stringify(persisted)}`)
    t.assert(
      persisted.includes('c') || errors.length > 0,
      `the last edit must be persisted or its loss reported via 'error'; persisted ${JSON.stringify(persisted)}, ${errors.length} error(s) emitted`
    )
  })
}
