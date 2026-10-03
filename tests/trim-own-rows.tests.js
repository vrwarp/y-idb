/* eslint-env browser */

/**
 * Performance regression tests: a trim must not re-read and re-apply the
 * rows this provider flushed itself.
 *
 * Every trim (storeState) starts with a catch-up read of every row at or
 * above the read cursor `_dbref` and applies those rows to the doc — rows
 * another tab may have written. When `_dbref` only moves in hydration and in
 * trims, the catch-up read also returns every row this provider's flusher
 * (or page-hide write) committed since: up to ~PREFERRED_TRIM_SIZE updates
 * that are already in the doc, cloned out of IndexedDB, decoded and run
 * through the struct store for nothing. The incremental trim then reads the
 * same rows a second time for its merge.
 *
 * The first two tests pin that cost with counters (no timing):
 *  - struct-store lookups on the doc during the trim: Y.applyUpdate looks up
 *    the struct list of each client in every update it applies; an
 *    incremental trim that applies no rows makes none
 *  - own row value reads: how often the trim reads an own row's value from
 *    IndexedDB with getAll (at most once, for the merge; a trim that reads
 *    the tail with a cursor makes none)
 * Their failure message also reports the own rows among the values of the
 * trim's first getAll, the catch-up read whose rows go to Y.applyUpdate
 * (not asserted: a trim that combines its reads would count differently).
 *
 * The remaining tests are guards for any fix that lets the trim skip own
 * rows: a row the provider has not seen (another tab's, or one written at a
 * key reverted by an aborted transaction) must still be read and applied.
 * They pass on the unfixed code too.
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, storeState, readSnapshot, PREFERRED_TRIM_SIZE } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * @param {Uint8Array} update
 * @return {string}
 */
const rowId = update => update.join(',')

/**
 * Collects the updates `provider` will write as rows (one row per update).
 *
 * @param {Y.Doc} doc
 * @param {IndexeddbPersistence} provider
 */
const trackOwnRows = (doc, provider) => {
  /** @type {Set<string>} */
  const rows = new Set()
  /**
   * @param {Uint8Array} update
   * @param {any} origin
   */
  const onUpdate = (update, origin) => {
    if (origin !== provider) rows.add(rowId(update))
  }
  doc.on('update', onUpdate)
  return { rows, stop: () => doc.off('update', onUpdate) }
}

/**
 * Runs `storeState(provider, false)` and counts what it reads and applies.
 *
 * @param {IndexeddbPersistence} provider
 * @param {Set<string>} ownRows
 */
const measureTrim = async (provider, ownRows) => {
  const proto = /** @type {any} */ (IDBObjectStore.prototype)
  const realGetAll = proto.getAll
  /** @type {Array<Array<Uint8Array>>} */
  const reads = []
  /**
   * @this {IDBObjectStore}
   * @param {...any} args
   */
  proto.getAll = function (...args) {
    const req = realGetAll.apply(this, args)
    if (this.name === 'updates' && this.transaction.db.name === provider.name) {
      /** @type {Array<Uint8Array>} */
      const read = []
      reads.push(read)
      req.addEventListener('success', () => { read.push(...req.result) })
    }
    return req
  }
  const clients = /** @type {any} */ (provider.doc.store.clients)
  const realGet = clients.get
  let lookups = 0
  /**
   * @this {Map<number, any>}
   * @param {number} client
   */
  clients.get = function (client) {
    lookups++
    return realGet.call(this, client)
  }
  try {
    await storeState(provider, false)
  } finally {
    proto.getAll = realGetAll
    delete clients.get
  }
  /**
   * @param {Uint8Array} v
   */
  const isOwn = v => ownRows.has(rowId(v))
  return {
    getAllCalls: reads.length,
    ownRowsApplied: reads.length > 0 ? reads[0].filter(isOwn).length : 0,
    ownRowValueReads: reads.reduce((n, read) => n + read.filter(isOwn).length, 0),
    structStoreLookups: lookups
  }
}

/**
 * A provider on a fresh database with trim bookkeeping (base row from a
 * full consolidation), so the next trim of a full store is incremental.
 *
 * @param {string} name
 * @param {object} [opts]
 * @param {number} [opts.writeDebounceMs]
 */
const openSeeded = async (name, opts) => {
  await clearDocument(name)
  const doc = new Y.Doc()
  const provider = new IndexeddbPersistence(name, doc, opts)
  await provider.whenSynced
  doc.getMap('m').set('base', 0)
  await provider.flush()
  await storeState(provider, true)
  return { doc, provider }
}

/**
 * `count` separate edits, each its own update (and row).
 *
 * @param {Y.Doc} doc
 * @param {number} from
 * @param {number} count
 */
const editRows = (doc, from, count) => {
  for (let i = from; i < from + count; i++) {
    doc.getArray('log').push([i])
  }
}

/**
 * The persisted content of `name`, as a fresh doc would load it after a
 * reload.
 *
 * @param {string} name
 */
const reload = async name => {
  const snapshot = await readSnapshot(name)
  const doc = new Y.Doc()
  if (snapshot !== null) Y.applyUpdate(doc, snapshot)
  const json = { m: doc.getMap('m').toJSON(), log: doc.getArray('log').length }
  doc.destroy()
  return json
}

/**
 * Tab B opens the same database, writes one edit, persists it and closes.
 *
 * @param {string} name
 */
const otherTabWritesAndCloses = async name => {
  const docB = new Y.Doc()
  const pB = new IndexeddbPersistence(name, docB)
  await pB.whenSynced
  docB.getMap('m').set('fromB', 'x')
  await pB.flush()
  await pB.destroy()
  docB.destroy()
}

/**
 * One-shot fault injection: the next readwrite transaction opened on
 * database `dbName` is aborted as soon as its first add() has succeeded
 * (the row was handed its auto-increment key, which the abort reverts).
 *
 * @param {string} dbName
 */
const abortNextWriteAtFirstAdd = dbName => {
  const realTransaction = IDBDatabase.prototype.transaction
  const proto = /** @type {any} */ (IDBObjectStore.prototype)
  const realAdd = proto.add
  /** @type {IDBTransaction|null} */
  let target = null
  const state = { aborted: false }
  /** @type {function(): void} */
  let markDone = () => {}
  /** @type {Promise<void>} */
  const whenDone = new Promise(resolve => { markDone = resolve })
  // @ts-ignore - override the prototype to pick the target transaction
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    if (target === null && this.name === dbName && tx.mode === 'readwrite') {
      target = tx
      tx.addEventListener('complete', () => markDone())
      tx.addEventListener('abort', () => markDone())
    }
    return tx
  }
  /**
   * @this {IDBObjectStore}
   * @param {...any} args
   */
  proto.add = function (...args) {
    const req = realAdd.apply(this, args)
    const tx = target
    if (tx !== null && this.transaction === tx && !state.aborted) {
      req.addEventListener('success', () => {
        if (state.aborted) return
        try {
          tx.abort()
          state.aborted = true
        } catch (e) {
          // Already committing/finished: the injection missed.
        }
      })
    }
    return req
  }
  const restore = () => {
    IDBDatabase.prototype.transaction = realTransaction
    proto.add = realAdd
  }
  return { state, whenDone, restore }
}

/**
 * Flusher path: PREFERRED_TRIM_SIZE own rows committed in 5 flushes, then
 * the incremental trim folds them. The trim must neither re-apply them nor
 * read their values more than once (for the merge).
 *
 * @param {t.TestCase} tc
 */
export const testIncrementalTrimSkipsOwnFlushedRows = async tc => {
  const { doc, provider } = await openSeeded(tc.testName)
  try {
    const own = trackOwnRows(doc, provider)
    for (let b = 0; b < 5; b++) {
      editRows(doc, b * 100, PREFERRED_TRIM_SIZE / 5)
      await provider.flush()
    }
    own.stop()
    t.assert(own.rows.size === PREFERRED_TRIM_SIZE, 'precondition: one row per edit')

    const stats = await measureTrim(provider, own.rows)
    t.assert(provider._dbsize === 2, 'precondition: the trim was incremental (base row + one delta row)')
    const got = JSON.stringify(stats)
    t.assert(stats.structStoreLookups === 0, `the trim must not re-apply rows this provider flushed itself (${PREFERRED_TRIM_SIZE} own rows); got ${got}`)
    t.assert(stats.ownRowValueReads <= PREFERRED_TRIM_SIZE, `each own row must be read at most once, for the merge; got ${got}`)

    t.compare(await reload(tc.testName), { m: { base: 0 }, log: PREFERRED_TRIM_SIZE }, 'the trim kept every edit')
  } finally {
    await provider.destroy()
  }
}

/**
 * Same for rows committed by the page-hide write, which bypasses the
 * flusher.
 *
 * @param {t.TestCase} tc
 */
export const testIncrementalTrimSkipsOwnPageHideRows = async tc => {
  // Long debounce: the page-hide write, not the flusher, takes each batch.
  const { doc, provider } = await openSeeded(tc.testName, { writeDebounceMs: 60000 })
  try {
    const own = trackOwnRows(doc, provider)
    for (let b = 0; b < 5; b++) {
      editRows(doc, b * 100, PREFERRED_TRIM_SIZE / 5)
      await provider._unloadListener()
    }
    own.stop()
    t.assert(provider._pendingUpdates.length === 0, 'precondition: the page-hide writes took every edit')

    const stats = await measureTrim(provider, own.rows)
    t.assert(provider._dbsize === 2, 'precondition: the trim was incremental (base row + one delta row)')
    const got = JSON.stringify(stats)
    t.assert(stats.structStoreLookups === 0, `the trim must not re-apply rows this provider's page-hide write committed (${PREFERRED_TRIM_SIZE} own rows); got ${got}`)
    t.assert(stats.ownRowValueReads <= PREFERRED_TRIM_SIZE, `each own row must be read at most once, for the merge; got ${got}`)

    t.compare(await reload(tc.testName), { m: { base: 0 }, log: PREFERRED_TRIM_SIZE }, 'the trim kept every edit')
  } finally {
    await provider.destroy()
  }
}

/**
 * Guard: another tab's row committed between two own flushes must still be
 * read and applied by the trim, and survive the next full consolidation.
 *
 * @param {t.TestCase} tc
 */
export const testTrimAppliesForeignRowBetweenOwnFlushes = async tc => {
  const { doc, provider } = await openSeeded(tc.testName)
  try {
    editRows(doc, 0, PREFERRED_TRIM_SIZE / 2)
    await provider.flush()
    await otherTabWritesAndCloses(tc.testName)
    editRows(doc, PREFERRED_TRIM_SIZE / 2, PREFERRED_TRIM_SIZE / 2)
    await provider.flush()

    await storeState(provider, false)
    t.assert(provider._dbsize === 2, 'precondition: the trim was incremental (base row + one delta row)')
    t.compare(doc.getMap('m').toJSON(), { base: 0, fromB: 'x' }, "the trim applied the other tab's row")
    const expected = { m: { base: 0, fromB: 'x' }, log: PREFERRED_TRIM_SIZE }
    t.compare(await reload(tc.testName), expected, 'the incremental trim kept every row')
    await storeState(provider, true)
    t.compare(await reload(tc.testName), expected, "a full consolidation keeps the other tab's row")
  } finally {
    await provider.destroy()
  }
}

/**
 * Guard: a flush queued behind a trim that aborts (the abort restores
 * `_dbref` and reverts the key generator) commits at the reverted key; a row
 * another tab writes afterwards must still be read by the next trim.
 *
 * @param {t.TestCase} tc
 */
export const testTrimAppliesForeignRowAfterFlushQueuedBehindAbortedTrim = async tc => {
  const { doc, provider } = await openSeeded(tc.testName)
  try {
    const fault = abortNextWriteAtFirstAdd(tc.testName)
    try {
      // The edit schedules a flush on a microtask: its transaction is
      // created after the trim's and runs once the trim has aborted.
      doc.getMap('m').set('a', 1)
      await storeState(provider, true).catch(() => {})
      await fault.whenDone
    } finally {
      fault.restore()
    }
    t.assert(fault.state.aborted, 'precondition: the trim transaction was aborted')
    await provider.flush()
    t.compare(await reload(tc.testName), { m: { base: 0, a: 1 }, log: 0 }, 'the queued flush committed')

    await otherTabWritesAndCloses(tc.testName)
    await storeState(provider, false)
    t.compare(doc.getMap('m').toJSON(), { base: 0, a: 1, fromB: 'x' }, "the trim applied the other tab's row")
    await storeState(provider, true)
    t.compare(await reload(tc.testName), { m: { base: 0, a: 1, fromB: 'x' }, log: 0 }, "a full consolidation keeps the other tab's row")
  } finally {
    await provider.destroy()
  }
}

/**
 * Guard: a flush that aborts after its row was handed a key must not count
 * that key as seen — another tab's next row re-uses it.
 *
 * @param {t.TestCase} tc
 */
export const testTrimAppliesForeignRowAtKeyOfAbortedFlush = async tc => {
  const { doc, provider } = await openSeeded(tc.testName)
  try {
    const fault = abortNextWriteAtFirstAdd(tc.testName)
    try {
      doc.getMap('m').set('a', 1)
      await fault.whenDone
    } finally {
      fault.restore()
    }
    t.assert(fault.state.aborted, 'precondition: the flush transaction was aborted')
    // Hold the backoff retry until the other tab has written its row.
    clearTimeout(provider._retryTimeoutId)
    provider._retryTimeoutId = null
    await otherTabWritesAndCloses(tc.testName)
    await provider.flush()

    await storeState(provider, false)
    t.compare(doc.getMap('m').toJSON(), { base: 0, a: 1, fromB: 'x' }, "the trim applied the other tab's row")
    await storeState(provider, true)
    t.compare(await reload(tc.testName), { m: { base: 0, a: 1, fromB: 'x' }, log: 0 }, "a full consolidation keeps the other tab's row")
  } finally {
    await provider.destroy()
  }
}
