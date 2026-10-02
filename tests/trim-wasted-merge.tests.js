/* eslint-env browser */

/**
 * Perf regression: a trim that ends in a full consolidation must not first
 * read and Y.mergeUpdates the tail it is about to throw away.
 *
 * The incremental path of `_storeState` reads the tail (getAllKeys + getAll
 * over (lastSegKey, inf)) and merges it before checking
 * `segRows + 1 >= trimSegmentRows` / `segBytes >= budget`; when either
 * fires, `fullConsolidation()` re-encodes the doc and the merge result is
 * discarded, in the same synchronous task as the full encode. The row check
 * needs neither the tail nor the merge; the byte check can use the raw tail
 * bytes as an upper bound.
 *
 * Costs are pinned with counters, never time:
 *  - tail reads: getAllKeys requests (the tail read is the trim's only one)
 *  - rows/bytes returned by getAll requests during the trim
 *  - strings Yjs decodes during the trim (TextDecoder.prototype.decode
 *    calls). The trimming provider has hydrated every row beforehand, so its
 *    trim fetches no new rows to apply, and `fullConsolidation()` only
 *    encodes: every decode in the trim belongs to the tail merge.
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, storeState, PREFERRED_TRIM_SIZE } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'
import * as idb from 'lib0/indexeddb.js'

/**
 * @typedef {object} TrimWork
 * @property {number} tailReads getAllKeys requests issued
 * @property {number} rowsRead rows returned by getAll requests
 * @property {number} bytesRead bytes of those rows
 * @property {number} decodes strings decoded by Yjs (TextDecoder.decode calls)
 */

/**
 * Counts TextDecoder.decode calls made while `fn` runs synchronously.
 *
 * @param {() => void} fn
 * @return {number}
 */
const countDecodes = fn => {
  const realDecode = TextDecoder.prototype.decode
  let decodes = 0
  /**
   * @this {TextDecoder}
   * @param {BufferSource} [input]
   * @param {TextDecodeOptions} [options]
   */
  const patchedDecode = function (input, options) {
    decodes++
    return realDecode.call(this, input, options)
  }
  TextDecoder.prototype.decode = patchedDecode
  try {
    fn()
  } finally {
    TextDecoder.prototype.decode = realDecode
  }
  return decodes
}

/**
 * Counts the IndexedDB reads and Yjs string decodes `fn` performs.
 *
 * @param {() => Promise<any>} fn
 * @return {Promise<TrimWork>}
 */
const countTrimWork = async fn => {
  const work = { tailReads: 0, rowsRead: 0, bytesRead: 0, decodes: 0 }
  const realGetAll = IDBObjectStore.prototype.getAll
  const realGetAllKeys = IDBObjectStore.prototype.getAllKeys
  const realDecode = TextDecoder.prototype.decode
  /**
   * @this {IDBObjectStore}
   * @param {IDBValidKey|IDBKeyRange|null} [query]
   * @param {number} [count]
   */
  const patchedGetAll = function (query, count) {
    const request = realGetAll.call(this, query, count)
    request.addEventListener('success', () => {
      request.result.forEach(/** @param {Uint8Array} row */ row => {
        work.rowsRead++
        work.bytesRead += row.byteLength
      })
    })
    return request
  }
  /**
   * @this {IDBObjectStore}
   * @param {IDBValidKey|IDBKeyRange|null} [query]
   * @param {number} [count]
   */
  const patchedGetAllKeys = function (query, count) {
    work.tailReads++
    return realGetAllKeys.call(this, query, count)
  }
  /**
   * @this {TextDecoder}
   * @param {BufferSource} [input]
   * @param {TextDecodeOptions} [options]
   */
  const patchedDecode = function (input, options) {
    work.decodes++
    return realDecode.call(this, input, options)
  }
  IDBObjectStore.prototype.getAll = patchedGetAll
  IDBObjectStore.prototype.getAllKeys = patchedGetAllKeys
  TextDecoder.prototype.decode = patchedDecode
  try {
    await fn()
  } finally {
    IDBObjectStore.prototype.getAll = realGetAll
    IDBObjectStore.prototype.getAllKeys = realGetAllKeys
    TextDecoder.prototype.decode = realDecode
  }
  return work
}

/**
 * Leaves `name` holding a small base row, `deltaWaves` delta rows and a
 * fresh tail of PREFERRED_TRIM_SIZE + 5 single-update rows (map overwrites
 * with string keys and values, so a merge of them decodes strings), then
 * binds a FRESH provider that hydrates all of it — its trim therefore has no
 * new rows to apply, and the tail predates its session (the leftover-tail
 * case the trim already handles).
 *
 * @param {string} name
 * @param {{ trimSegmentRows?: number, trimFullCompactBytes?: number }} opts
 * @param {number} deltaWaves
 * @return {Promise<{ provider: IndexeddbPersistence, expected: any }>}
 */
const buildTrimLayout = async (name, opts, deltaWaves) => {
  await clearDocument(name)
  const writerDoc = new Y.Doc()
  const map = writerDoc.getMap('m')
  const writer = new IndexeddbPersistence(name, writerDoc, opts)
  writer._storeTimeout = 1e9 // trims are driven explicitly
  await writer.whenSynced
  map.set('seed', 'x')
  await writer.flush()
  await storeState(writer, true) // base row + trim bookkeeping
  let n = 0
  const wave = () => {
    for (let i = 0; i < PREFERRED_TRIM_SIZE + 5; i++) {
      map.set('k' + (n % 20), 'value-' + n)
      n++
    }
  }
  for (let w = 0; w < deltaWaves; w++) {
    wave()
    await writer.flush()
    await storeState(writer, false)
  }
  wave()
  await writer.flush()
  const expected = map.toJSON()
  await writer.destroy()

  const provider = new IndexeddbPersistence(name, new Y.Doc(), opts)
  provider._storeTimeout = 1e9
  await provider.whenSynced
  t.assert(provider._dbsize === 1 + deltaWaves + PREFERRED_TRIM_SIZE + 5, `layout: base + ${deltaWaves} delta row(s) + tail (got ${provider._dbsize} rows)`)
  t.compare(provider.doc.getMap('m').toJSON(), expected)
  return { provider, expected }
}

/**
 * Strings a Y.mergeUpdates of the provider's current tail (rows after
 * lastSegKey) decodes — proves the decode counter sees a tail merge.
 *
 * @param {IndexeddbPersistence} provider
 * @return {Promise<{ rows: number, decodes: number }>}
 */
const tailMergeDecodes = async provider => {
  const [updates, custom] = idb.transact(/** @type {IDBDatabase} */ (provider.db), ['updates', 'custom'], 'readonly')
  const trimState = /** @type {any} */ (await idb.get(custom, '__yidb_trim_v1'))
  const tail = /** @type {Array<Uint8Array>} */ (await idb.getAll(updates, idb.createIDBKeyRangeLowerBound(trimState.lastSegKey, true)))
  return { rows: tail.length, decodes: countDecodes(() => { Y.mergeUpdates(tail) }) }
}

/**
 * The trim must still land on the same layout and data.
 *
 * @param {string} name
 * @param {IndexeddbPersistence} provider
 * @param {any} expected
 */
const assertFullyConsolidated = async (name, provider, expected) => {
  t.assert(provider._dbsize === 1, `the trim consolidated fully (got ${provider._dbsize} rows)`)
  const doc = new Y.Doc()
  const reader = new IndexeddbPersistence(name, doc)
  await reader.whenSynced
  t.compare(doc.getMap('m').toJSON(), expected)
  await reader.destroy()
}

/**
 * A forced full consolidation of the same doc: the floor a policy-chosen
 * full consolidation should match (no tail read, no merge).
 *
 * @param {IndexeddbPersistence} provider
 * @return {Promise<TrimWork>}
 */
const forcedFloor = async provider => {
  const floor = await countTrimWork(() => storeState(provider, true))
  t.assert(floor.tailReads === 0 && floor.rowsRead === 0 && floor.decodes === 0, `a forced full consolidation reads and decodes nothing (got ${JSON.stringify(floor)})`)
  return floor
}

/**
 * Row-count trigger: with trimSegmentRows = 2 and one delta row stored, the
 * next trim must consolidate fully (segRows + 1 >= 2). Deciding that needs
 * only the delta-row count, yet the trim reads all 505 tail rows and merges
 * them first.
 *
 * @param {t.TestCase} tc
 */
export const testRowTriggeredFullTrimSkipsTailReadAndMerge = async tc => {
  const { provider, expected } = await buildTrimLayout(tc.testName, { trimSegmentRows: 2 }, 1)
  const control = await tailMergeDecodes(provider)
  t.assert(control.rows === PREFERRED_TRIM_SIZE + 5 && control.decodes > 0, `merging this tail decodes strings (rows=${control.rows}, decodes=${control.decodes})`)

  const work = await countTrimWork(() => storeState(provider, false))
  t.info(`row-triggered full trim: ${JSON.stringify(work)}; merging its tail alone decodes ${control.decodes} strings`)
  await assertFullyConsolidated(tc.testName, provider, expected)
  await forcedFloor(provider)

  t.assert(work.tailReads === 0, `a row-triggered full consolidation does not read the tail (got ${work.tailReads} getAllKeys request(s))`)
  t.assert(work.rowsRead === 0, `a row-triggered full consolidation reads no rows (got ${work.rowsRead} rows, ${work.bytesRead} bytes)`)
  t.assert(work.decodes === 0, `a row-triggered full consolidation merges nothing (got ${work.decodes} strings decoded by a merge that was thrown away)`)
  await provider.destroy()
}

/**
 * Byte trigger: with a tiny base row and trimFullCompactBytes = 1, the raw
 * tail bytes alone cross the budget, so the trim must consolidate fully —
 * yet it merges the whole tail first and discards the result. Reading the
 * tail (for its byte sum) is allowed; merging it is not.
 *
 * @param {t.TestCase} tc
 */
export const testByteTriggeredFullTrimSkipsMerge = async tc => {
  const { provider, expected } = await buildTrimLayout(tc.testName, { trimFullCompactBytes: 1 }, 0)
  const control = await tailMergeDecodes(provider)
  t.assert(control.rows === PREFERRED_TRIM_SIZE + 5 && control.decodes > 0, `merging this tail decodes strings (rows=${control.rows}, decodes=${control.decodes})`)

  const work = await countTrimWork(() => storeState(provider, false))
  t.info(`byte-triggered full trim: ${JSON.stringify(work)}; merging its tail alone decodes ${control.decodes} strings`)
  await assertFullyConsolidated(tc.testName, provider, expected)
  await forcedFloor(provider)

  t.assert(work.decodes === 0, `a byte-triggered full consolidation merges nothing (got ${work.decodes} strings decoded by a merge that was thrown away)`)
  await provider.destroy()
}
