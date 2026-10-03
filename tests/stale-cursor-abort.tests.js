/* eslint-env browser */

/**
 * Regression tests: a trim or fetch queued behind another transaction of the
 * same provider must not read the rows above a read cursor that transaction
 * has not committed yet.
 *
 * A trim (storeState) and hydration move the provider's read cursor past
 * their own row from a request callback, before their transaction commits;
 * an abort restores it (see dbref-abort.tests.js). A serializing
 * transactionRunner releases its lock once the work's promise resolves:
 * for hydration on the transaction's last request callback, for a trim
 * once it has committed, unless the runner gives up on it earlier (a
 * watchdog releasing the lock while the commit stalls). So the next queued
 * trim or fetch can start before the earlier transaction's outcome is
 * known. If that transaction then aborts (a QuotaExceededError at commit),
 * IndexedDB reverts the autoIncrement key generator and another tab's next
 * row re-uses the key of the aborted row. The queued trim or fetch must
 * still read and apply that row, and a full consolidation must not delete
 * it.
 */

import * as Y from 'yjs'
import * as idb from 'lib0/indexeddb'
import { IndexeddbPersistence, clearDocument, fetchUpdates, storeState, readSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

const requestMethods = /** @type {const} */ ([
  'add', 'put', 'delete', 'clear', 'get', 'getKey', 'getAll', 'getAllKeys', 'count', 'openCursor', 'openKeyCursor'
])

/**
 * A FIFO lock, the usual shape of a serializing transactionRunner: the next
 * work starts once the previous work's promise has settled, or once
 * `release()` gives up on the oldest work still holding the lock (what a
 * watchdog runner does when a transaction stalls before commit).
 */
const createFifoRunner = () => {
  /** @type {Promise<any>} */
  let chain = Promise.resolve()
  /** @type {Array<function(): void>} */
  const holders = []
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    const p = chain.then(work)
    /** @type {function(): void} */
    let release = () => {}
    const released = new Promise(resolve => { release = () => resolve(undefined) })
    holders.push(release)
    const settled = p.catch(() => {}).then(() => {
      const i = holders.indexOf(release)
      if (i >= 0) holders.splice(i, 1)
    })
    chain = Promise.race([settled, released])
    return p
  }
  runner.release = () => {
    const release = holders.shift()
    if (release) release()
  }
  return runner
}

/**
 * Open a second connection to `name` (another tab), with y-idb's layout.
 *
 * @param {string} name
 * @return {Promise<IDBDatabase>}
 */
const openOtherTab = name => idb.openDB(name, db =>
  idb.createStores(db, [
    ['updates', { autoIncrement: true }],
    ['custom']
  ])
)

/**
 * Fault injection on database `name`, one-shot:
 *
 * - The first readwrite transaction a connection other than `otherTab`
 *   opens (the provider's) is aborted when it would otherwise commit: every
 *   request it issued has succeeded and no further request was issued once
 *   their callbacks had run. That is how a commit-time QuotaExceededError
 *   or a background-tab kill shows up. `onCommitPoint`, if given, runs
 *   right before (the abort's event is dispatched later).
 * - As soon as that transaction is created, `otherTab` opens its own
 *   readwrite transaction adding `otherUpdate` (tab B flushes while tab A's
 *   transaction is running). IndexedDB runs it after the provider's
 *   transaction has settled, and before any transaction the provider
 *   creates later.
 *
 * Every transaction the provider's connection creates meanwhile is tracked,
 * so the test can wait for all of them to settle. Only standard IDB API is
 * patched.
 *
 * @param {string} name
 * @param {IDBDatabase} otherTab
 * @param {Uint8Array} otherUpdate
 * @param {function(): void} [onCommitPoint]
 */
const injectAbortWithOtherTabWrite = (name, otherTab, otherUpdate, onCommitPoint = () => {}) => {
  const realTransaction = IDBDatabase.prototype.transaction
  const proto = /** @type {any} */ (IDBObjectStore.prototype)
  const realMethods = requestMethods.map(m => proto[m])
  /** @type {IDBTransaction|null} */
  let target = null
  /** @type {Array<IDBRequest>} */
  const requests = []
  /** @type {Array<Promise<void>>} */
  const providerTxs = []
  const state = { aborted: false, otherTabCommitted: false }
  /** @type {function(): void} */
  let markTargetCreated = () => {}
  /** @type {Promise<void>} */
  const targetCreated = new Promise(resolve => { markTargetCreated = resolve })
  /** @type {function(): void} */
  let markOtherTabDone = () => {}
  /** @type {Promise<void>} */
  const otherTabDone = new Promise(resolve => { markOtherTabDone = resolve })
  const abort = () => {
    if (state.aborted || target === null) return
    try {
      target.abort()
      state.aborted = true
    } catch (e) {
      // Already committing/finished: the injection missed.
    }
  }
  /**
   * @param {IDBTransaction} tx
   * @return {Promise<void>}
   */
  const settled = tx => new Promise(resolve => {
    tx.addEventListener('complete', () => resolve())
    tx.addEventListener('abort', () => resolve())
  })
  // @ts-ignore - override the prototype to pick the target transaction
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    if (this.name === name && this !== otherTab) {
      providerTxs.push(settled(tx))
      if (target === null && tx.mode === 'readwrite') {
        target = tx
        // Tab B's write, created while tab A's transaction is running.
        const otherTx = realTransaction.call(otherTab, ['updates'], 'readwrite')
        realMethods[requestMethods.indexOf('add')].call(otherTx.objectStore('updates'), otherUpdate)
        otherTx.addEventListener('complete', () => {
          state.otherTabCommitted = true
          markOtherTabDone()
        })
        otherTx.addEventListener('abort', () => markOtherTabDone())
        markTargetCreated()
      }
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
          // Runs after the success callbacks' microtasks have drained and
          // before the transaction's next step (commit, if idle). Relies on
          // fake-indexeddb queueing that step with setImmediate after the
          // event dispatch (an implementation detail); the tests'
          // `state.aborted` precondition catches a silent miss.
          setImmediate(() => {
            if (!state.aborted && requests.every(r => r.readyState === 'done')) {
              onCommitPoint()
              abort()
            }
          })
        })
      }
      return req
    }
  })
  const restore = () => {
    IDBDatabase.prototype.transaction = realTransaction
    requestMethods.forEach((m, i) => { proto[m] = realMethods[i] })
  }
  /**
   * Resolves once tab B's write and every transaction the provider created
   * so far have settled.
   * @return {Promise<void>}
   */
  const allSettled = async () => {
    await otherTabDone
    let n = 0
    while (n < providerTxs.length) {
      n = providerTxs.length
      await Promise.all(providerTxs)
    }
  }
  return { state, targetCreated, allSettled, restore }
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
 * Tab B's edit, as the single row its flush writes.
 */
const otherTabUpdate = () => {
  const docB = new Y.Doc()
  docB.getMap('m').set('fromB', 'x')
  const update = Y.encodeStateAsUpdate(docB)
  docB.destroy()
  return update
}

/**
 * Tab A runs two trims through a FIFO transactionRunner. The first one's
 * full consolidation aborts at commit; the second starts as soon as the
 * runner gives up on the first one (at its commit point), before that abort.
 * Tab B flushes while the first trim's transaction is running, so its row
 * gets the key the aborted base row had. The second trim must read and keep
 * it.
 *
 * @param {t.TestCase} tc
 */
export const testTrimQueuedBehindAbortedTrimKeepsOtherTabsEdit = async tc => {
  await clearDocument(tc.testName)
  const docA = new Y.Doc()
  const runner = createFifoRunner()
  const pA = new IndexeddbPersistence(tc.testName, docA, { transactionRunner: runner })
  const otherTab = await openOtherTab(tc.testName)
  try {
    await pA.whenSynced
    docA.getMap('m').set('a', 1)
    await pA.flush()
    await storeState(pA, true)
    t.compare(await reloadMap(tc.testName), { a: 1 }, 'seeded')

    const fault = injectAbortWithOtherTabWrite(tc.testName, otherTab, otherTabUpdate(), runner.release)
    try {
      const first = storeState(pA, true)
      const second = storeState(pA, true)
      await Promise.allSettled([first, second])
      await fault.allSettled()
    } finally {
      fault.restore()
    }
    t.assert(fault.state.aborted, "precondition: the first trim's transaction was aborted")
    t.assert(fault.state.otherTabCommitted, "precondition: tab B's write committed")

    t.compare(await reloadMap(tc.testName), { a: 1, fromB: 'x' }, "tab A's trims must not delete tab B's committed row")
    t.compare(docA.getMap('m').toJSON(), { a: 1, fromB: 'x' }, "tab A must have applied tab B's row")
  } finally {
    otherTab.close()
    await pA.destroy()
  }
}

/**
 * Same with a fetch queued behind the aborted trim instead of a second
 * trim. It must apply tab B's row rather than move the read cursor past it,
 * or tab A's next full consolidation deletes the row.
 *
 * @param {t.TestCase} tc
 */
export const testFetchQueuedBehindAbortedTrimKeepsOtherTabsEdit = async tc => {
  await clearDocument(tc.testName)
  const docA = new Y.Doc()
  const runner = createFifoRunner()
  const pA = new IndexeddbPersistence(tc.testName, docA, { transactionRunner: runner })
  const otherTab = await openOtherTab(tc.testName)
  try {
    await pA.whenSynced
    docA.getMap('m').set('a', 1)
    await pA.flush()
    await storeState(pA, true)
    t.compare(await reloadMap(tc.testName), { a: 1 }, 'seeded')

    const fault = injectAbortWithOtherTabWrite(tc.testName, otherTab, otherTabUpdate(), runner.release)
    try {
      const trim = storeState(pA, true)
      const fetch = fetchUpdates(pA)
      await Promise.allSettled([trim, fetch])
      await fault.allSettled()
    } finally {
      fault.restore()
    }
    t.assert(fault.state.aborted, "precondition: the trim's transaction was aborted")
    t.assert(fault.state.otherTabCommitted, "precondition: tab B's write committed")
    t.compare(docA.getMap('m').toJSON(), { a: 1, fromB: 'x' }, "tab A's fetch must have applied tab B's row")

    await storeState(pA, true)
    t.compare(await reloadMap(tc.testName), { a: 1, fromB: 'x' }, "tab A's next consolidation must not delete tab B's committed row")
  } finally {
    otherTab.close()
    await pA.destroy()
  }
}

/**
 * Same, behind a hydration transaction that carries the initial-state row
 * of a doc that already had content, and aborts at commit. A trim the app
 * requests while hydration is running is queued behind it in the
 * transactionRunner and starts before the abort.
 *
 * @param {t.TestCase} tc
 */
export const testTrimQueuedBehindAbortedHydrationKeepsOtherTabsEdit = async tc => {
  await clearDocument(tc.testName)
  const otherTab = await openOtherTab(tc.testName)
  const docA = new Y.Doc()
  docA.getMap('m').set('a', 1)
  const fault = injectAbortWithOtherTabWrite(tc.testName, otherTab, otherTabUpdate())
  /** @type {IndexeddbPersistence|null} */
  let pA = null
  try {
    try {
      pA = new IndexeddbPersistence(tc.testName, docA, { transactionRunner: createFifoRunner() })
      // Hydration's transaction is under way: request a trim now.
      await fault.targetCreated
      await storeState(pA, true).catch(() => {})
      await pA.whenSynced
      await fault.allSettled()
    } finally {
      fault.restore()
    }
    t.assert(fault.state.aborted, 'precondition: the hydration transaction was aborted')
    t.assert(fault.state.otherTabCommitted, "precondition: tab B's write committed")
    // The aborted hydration re-buffered tab A's initial state.
    await pA.flush()

    t.compare(await reloadMap(tc.testName), { a: 1, fromB: 'x' }, "tab A's trim must not delete tab B's committed row")
    t.compare(docA.getMap('m').toJSON(), { a: 1, fromB: 'x' }, "tab A must have applied tab B's row")
  } finally {
    otherTab.close()
    if (pA !== null) await pA.destroy()
  }
}
