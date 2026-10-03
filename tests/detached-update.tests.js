/* eslint-env browser */

/**
 * Regression tests: a queued update whose IDBObjectStore.add() throws
 * synchronously must not wedge the provider or lose the edits queued after
 * it.
 *
 * Yjs hands the same Uint8Array to every 'update' listener. Another listener
 * on the doc (one that posts updates to a worker with
 * `postMessage(update, [update.buffer])`, or a `structuredClone` with
 * transfer) can detach its buffer while the provider still holds it in its
 * queue. add() then throws a DataCloneError synchronously. That does not
 * abort the transaction, so the adds issued before it commit on their own,
 * and the update itself, plus every one queued after it, was never written:
 *
 * - the flusher re-buffered the whole batch, poisoned entry included, and
 *   every retry (endless since retries keep going at the capped backoff)
 *   committed the same prefix again as duplicate rows without getting past
 *   it;
 * - the page-hide write and destroy()'s final write stopped at it, and the
 *   page-hide write handed the batch back while its partial transaction
 *   still committed the prefix.
 *
 * Reloading lost every edit from the poisoned update on. The doc itself still
 * holds all of them, so the provider can always recover the content (e.g.
 * from `Y.encodeStateAsUpdate(doc)`).
 *
 * The flusher test runs `setTimeout` on a virtual clock (fake-indexeddb runs
 * on setImmediate and is let settle before every clock step), so the retries
 * of two simulated minutes run deterministically in milliseconds.
 */

import * as Y from 'yjs'
import * as idb from 'lib0/indexeddb'
import { IndexeddbPersistence, clearDocument, readSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * Registers an 'update' listener (after the provider's own) that detaches
 * the buffer of the next update once `arm()` was called, the way a sibling
 * listener transferring the update to a worker does.
 *
 * @param {Y.Doc} doc
 * @return {() => void} arm
 */
const detachNextUpdate = doc => {
  let armed = false
  doc.on('update', (/** @type {Uint8Array} */ update) => {
    if (!armed) return
    armed = false
    structuredClone(update.buffer, { transfer: [update.buffer] })
    t.assert(update.byteLength === 0, 'precondition: the update buffer is detached')
  })
  return () => { armed = true }
}

/**
 * The edits every test makes: two plain updates, one whose buffer a sibling
 * listener detaches while it is queued, and one queued after it.
 *
 * @param {Y.Doc} doc
 * @param {() => void} armDetach
 */
const editWithDetachedUpdate = (doc, armDetach) => {
  const arr = doc.getArray('a')
  arr.push(['b1'])
  arr.push(['b2'])
  armDetach()
  arr.push(['detached'])
  arr.push(['a1'])
}

/**
 * Content of database `name` as a fresh provider-less reader sees it.
 *
 * @param {string} name
 * @return {Promise<Array<any>>}
 */
const persistedArray = async name => {
  const persisted = await readSnapshot(name)
  const doc = new Y.Doc()
  if (persisted !== null) Y.applyUpdate(doc, persisted)
  return doc.getArray('a').toArray()
}

/**
 * Number of rows in the updates store of database `name`.
 *
 * @param {string} name
 * @return {Promise<number>}
 */
const countRows = async name => {
  const db = await idb.openDB(name, () => {})
  try {
    const [store] = idb.transact(db, ['updates'], 'readonly')
    return await idb.count(store)
  } finally {
    db.close()
  }
}

/**
 * Number of rows in the updates store of database `name` that hold the
 * insertion of array element `value`. An edit stored in more than one row
 * means rows the failed add() left behind committed beside the rows that
 * recovered them.
 *
 * @param {string} name
 * @param {any} value
 * @return {Promise<number>}
 */
const countRowsHolding = async (name, value) => {
  const db = await idb.openDB(name, () => {})
  try {
    const [store] = idb.transact(db, ['updates'], 'readonly')
    const rows = /** @type {Array<Uint8Array>} */ (await idb.getAll(store))
    return rows.filter(row => Y.decodeUpdate(row).structs.some(struct =>
      struct instanceof Y.Item && struct.content.getContent().includes(value)
    )).length
  } finally {
    db.close()
  }
}

/**
 * @typedef {Object} VirtualClock
 * @property {(ms: number) => Promise<void>} advanceBy run simulated time forward, firing due timers in order
 */

/**
 * Runs `fn` with `setTimeout` on a virtual clock. Before every clock step
 * fake-indexeddb (setImmediate) is let settle until no transaction is open.
 * The real timers and prototype are restored afterwards.
 *
 * @template T
 * @param {(clock: VirtualClock) => Promise<T>} fn
 * @return {Promise<T>}
 */
const withVirtualClock = async fn => {
  const g = /** @type {any} */ (globalThis)
  const realSetTimeout = g.setTimeout
  const realClearTimeout = g.clearTimeout
  const realTransaction = IDBDatabase.prototype.transaction
  /** @type {Map<number, { at: number, fn: Function, args: Array<any> }>} */
  const timers = new Map()
  let now = 0
  let seq = 0
  let openTx = 0
  g.setTimeout = (/** @type {Function} */ fn, /** @type {number} */ ms = 0, /** @type {Array<any>} */ ...args) => {
    const id = ++seq
    timers.set(id, { at: now + Math.max(0, Number(ms) || 0), fn, args })
    return id
  }
  g.clearTimeout = (/** @type {number} */ id) => { timers.delete(id) }
  // @ts-ignore
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    openTx++
    const done = () => { openTx-- }
    tx.addEventListener('complete', done)
    tx.addEventListener('abort', done)
    return tx
  }
  const tick = () => new Promise(resolve => setImmediate(resolve))
  const settle = async () => {
    let idle = 0
    while (idle < 3) {
      await tick()
      idle = openTx === 0 ? idle + 1 : 0
    }
  }
  /** @type {VirtualClock} */
  const clock = {
    advanceBy: async ms => {
      const until = now + ms
      for (;;) {
        await settle()
        let nextId = -1
        /** @type {{ at: number, fn: Function, args: Array<any> } | null} */
        let next = null
        for (const [id, timer] of timers) {
          if (timer.at <= until && (next === null || timer.at < next.at)) {
            nextId = id
            next = timer
          }
        }
        if (next === null) break
        timers.delete(nextId)
        now = Math.max(now, next.at)
        next.fn(...next.args)
      }
      now = until
    }
  }
  try {
    return await fn(clock)
  } finally {
    g.setTimeout = realSetTimeout
    g.clearTimeout = realClearTimeout
    IDBDatabase.prototype.transaction = realTransaction
  }
}

/**
 * The flusher must get past an update it cannot add(): the edits made
 * before, with and after it are persisted (an edit made later too), without
 * a destroy() or reload having to rescue them, and without the store
 * filling up with copies of the rows before it.
 *
 * @param {t.TestCase} tc
 */
export const testFlusherPersistsEditsAroundADetachedUpdate = async tc => {
  await clearDocument(tc.testName)
  await withVirtualClock(async clock => {
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc)
    persistence.on('error', () => {})
    await persistence.whenSynced
    const armDetach = detachNextUpdate(doc)

    try {
      editWithDetachedUpdate(doc, armDetach)
      // Ample time for the flusher's first attempt and its backoff retries.
      await clock.advanceBy(60_000)
      doc.getArray('a').push(['a2'])
      await clock.advanceBy(60_000)

      const persisted = await persistedArray(tc.testName)
      t.compareArrays(
        persisted,
        ['b1', 'b2', 'detached', 'a1', 'a2'],
        `every edit, including the detached update and those after it, must be persisted by the flusher (persisted: ${JSON.stringify(persisted)})`
      )
      const rows = await countRows(tc.testName)
      t.assert(rows <= 5, `5 updates must not leave more than 5 rows (got ${rows}): each retry re-added the rows before the detached update`)
      const copies = await countRowsHolding(tc.testName, 'b1')
      t.assert(copies === 1, `an edit made before the detached update must be stored once (in ${copies} rows): the failed attempt's adds must not commit`)
      t.assert(persistence._pendingUpdates.length === 0, 'nothing must be left in the queue')
    } finally {
      // Under the virtual clock too: its retry timers are dropped with it.
      await persistence.destroy()
    }
  })
}

/**
 * The page-hide write is the last chance to persist buffered edits (the page
 * may be gone right after): once it has settled, every edit made before the
 * page was hidden must be in the database, not only those queued before the
 * detached update.
 *
 * @param {t.TestCase} tc
 */
export const testPageHideWritePersistsEditsAroundADetachedUpdate = async tc => {
  await clearDocument(tc.testName)
  const g = /** @type {any} */ (globalThis)
  const originalAdd = g.addEventListener
  /** @type {Array<() => Promise<void>>} */
  const pagehideHandlers = []
  g.addEventListener = (/** @type {string} */ type, /** @type {() => Promise<void>} */ handler) => {
    if (type === 'pagehide') pagehideHandlers.push(handler)
  }
  /** @type {IndexeddbPersistence} */
  let persistence
  const doc = new Y.Doc()
  try {
    // The debounce keeps the flusher from taking the queue first.
    persistence = new IndexeddbPersistence(tc.testName, doc, { writeDebounceMs: 60_000 })
  } finally {
    g.addEventListener = originalAdd
  }
  persistence.on('error', () => {})
  await persistence.whenSynced
  t.assert(pagehideHandlers.length === 1, 'precondition: the provider listens for pagehide')
  const armDetach = detachNextUpdate(doc)

  editWithDetachedUpdate(doc, armDetach)
  await Promise.all(pagehideHandlers.map(handler => handler()))

  const persisted = await persistedArray(tc.testName)
  t.compareArrays(
    persisted,
    ['b1', 'b2', 'detached', 'a1'],
    `once the page-hide write has settled, every buffered edit must be persisted (persisted: ${JSON.stringify(persisted)})`
  )
  const copies = await countRowsHolding(tc.testName, 'b1')
  t.assert(copies === 1, `an edit made before the detached update must be stored once (in ${copies} rows): the failed attempt's adds must not commit`)
  await persistence.destroy()
}

/**
 * destroy()'s final write must persist every pending edit, not stop at the
 * detached update and drop the rest: they must all survive a reload.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyFinalWritePersistsEditsAroundADetachedUpdate = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  // The debounce keeps every edit pending until destroy()'s final write.
  const persistence = new IndexeddbPersistence(tc.testName, doc, { writeDebounceMs: 60_000 })
  persistence.on('error', () => {})
  await persistence.whenSynced
  const armDetach = detachNextUpdate(doc)

  editWithDetachedUpdate(doc, armDetach)
  await persistence.destroy()
  const copies = await countRowsHolding(tc.testName, 'b1')
  t.assert(copies === 1, `an edit made before the detached update must be stored once (in ${copies} rows): the failed attempt's adds must not commit`)

  const doc2 = new Y.Doc()
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced
  const reloaded = doc2.getArray('a').toArray()
  t.compareArrays(
    reloaded,
    ['b1', 'b2', 'detached', 'a1'],
    `every edit pending at destroy() must survive a reload (reloaded: ${JSON.stringify(reloaded)})`
  )
  await persistence2.destroy()
}
