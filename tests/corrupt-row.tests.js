/* eslint-env browser */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, storeState, writeSnapshot, PREFERRED_TRIM_SIZE } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'
import * as idb from 'lib0/indexeddb.js'

/**
 * Resolves with the name of the first lifecycle event ('synced' or 'error')
 * the provider emits, so hydration can be awaited without racing timers
 * whether or not it succeeds.
 *
 * @param {IndexeddbPersistence} provider
 * @return {Promise<{ event: 'synced'|'error', err?: any }>}
 */
const firstLifecycleEvent = provider => new Promise(resolve => {
  provider.on('synced', () => resolve({ event: 'synced' }))
  provider.on('error', /** @param {any} err */ err => resolve({ event: 'error', err }))
})

/**
 * Opens a raw connection with the provider's store layout, to plant or
 * inspect rows behind the provider's back.
 *
 * @param {string} name
 * @return {Promise<IDBDatabase>}
 */
const openRaw = name => idb.openDB(name, db =>
  idb.createStores(db, [
    ['updates', { autoIncrement: true }],
    ['custom']
  ])
)

/**
 * Applies every stored row to a fresh doc, each on its own, and counts the
 * rows that fail to decode. Unlike readSnapshot (which merges inside the
 * transaction's complete handler) a lingering corrupt row cannot throw out
 * of here, so it fails an assertion instead of crashing the runner.
 *
 * @param {string} name
 * @return {Promise<{ doc: Y.Doc, keys: Array<number>, corrupt: number }>}
 */
const loadRawRows = async name => {
  const raw = await openRaw(name)
  const [store] = idb.transact(raw, ['updates'], 'readonly')
  const rows = await idb.getAllKeysValues(store)
  raw.close()
  const doc = new Y.Doc()
  let corrupt = 0
  rows.forEach(row => {
    try {
      Y.applyUpdate(doc, row.v)
    } catch (e) {
      corrupt++
    }
  })
  return { doc, keys: rows.map(row => row.k), corrupt }
}

/**
 * Persists [valid snapshot {a: 1}] [corrupt row] [valid update {b: 2}].
 *
 * @param {string} name
 */
const writeCorruptLayout = async name => {
  const source = new Y.Doc()
  source.getMap('m').set('a', 1)
  const updateA = Y.encodeStateAsUpdate(source)
  const svA = Y.encodeStateVector(source)
  source.getMap('m').set('b', 2)
  const updateB = Y.encodeStateAsUpdate(source, svA)
  await writeSnapshot(name, updateA)
  const raw = await openRaw(name)
  const [rawStore] = idb.transact(raw, ['updates'])
  await idb.addAutoKey(rawStore, new Uint8Array([1, 2, 3]))
  await idb.addAutoKey(rawStore, updateB)
  raw.close()
}

/**
 * One undecodable row in the updates store must not wedge the document: a
 * forced full consolidation (`storeState(provider, true)`) encodes the valid
 * in-memory state and replaces every older row, the corrupt one included,
 * and the next load then succeeds with all valid content (rows stored before
 * and after the bad row, plus edits made since).
 *
 * Previously the bad row made hydration throw half-way (rows after it were
 * never applied and the read cursor stayed at 0), so every later storeState
 * re-applied the corrupt row before reaching the corrupt-row fallback and
 * rejected; the row was never removed and every later load failed again.
 *
 * @param {t.TestCase} tc
 */
export const testForcedConsolidationRecoversFromCorruptRow = async tc => {
  await clearDocument(tc.testName)
  await writeCorruptLayout(tc.testName)

  const doc1 = new Y.Doc()
  const p1 = new IndexeddbPersistence(tc.testName, doc1)
  /** @type {IndexeddbPersistence|null} */
  let p2 = null
  try {
    // Hydration settles one way or the other (the corrupt row may surface
    // as an 'error'); edits made afterwards must be kept.
    await firstLifecycleEvent(p1)
    doc1.getMap('m').set('c', 3)
    await p1.flush()

    /** @type {any} */
    let storeErr = null
    try {
      await storeState(p1, true)
    } catch (e) {
      storeErr = e
    }
    t.assert(storeErr === null, `forced storeState must consolidate past the corrupt row instead of rejecting (got: ${storeErr && storeErr.message})`)
    t.compare(doc1.getMap('m').toJSON(), { a: 1, b: 2, c: 3 }, 'provider holds every valid row plus its own edit')

    // The corrupt row is gone: every stored row decodes, and together they
    // hold the full content.
    const persisted = await loadRawRows(tc.testName)
    t.assert(persisted.corrupt === 0, 'no undecodable row is left behind')
    t.compare(persisted.doc.getMap('m').toJSON(), { a: 1, b: 2, c: 3 }, 'persisted state is complete')

    await p1.destroy()

    // A later load succeeds cleanly with the full content.
    const doc2 = new Y.Doc()
    p2 = new IndexeddbPersistence(tc.testName, doc2)
    const outcome = await firstLifecycleEvent(p2)
    t.assert(outcome.event === 'synced', `reload after consolidation must sync (got '${outcome.event}': ${outcome.err && outcome.err.message})`)
    t.compare(doc2.getMap('m').toJSON(), { a: 1, b: 2, c: 3 }, 'reload sees all valid content')
  } finally {
    await p1.destroy()
    if (p2) await p2.destroy()
  }
}

/**
 * A corrupt row is skipped on load: the rows on both sides of it are
 * applied, the failure is reported once via 'error', 'synced' still fires,
 * and the read cursor moves past the bad row, so a later non-forced
 * storeState (below the trim threshold) does not trip over it again.
 *
 * @param {t.TestCase} tc
 */
export const testCorruptRowIsSkippedOnLoad = async tc => {
  await clearDocument(tc.testName)
  await writeCorruptLayout(tc.testName)

  const doc = new Y.Doc()
  const p = new IndexeddbPersistence(tc.testName, doc)
  p._storeTimeout = 1e9
  /** @type {Array<any>} */
  const errors = []
  p.on('error', /** @param {any} err */ err => { errors.push(err) })
  try {
    await firstLifecycleEvent(p)
    t.compare(doc.getMap('m').toJSON(), { a: 1, b: 2 }, 'rows before and after the corrupt row are loaded')

    doc.getMap('m').set('c', 3)
    await p.flush()
    // The flush transaction runs after the hydration transaction, so
    // hydration has committed by now.
    t.assert(p.synced, "hydration still completes with 'synced'")

    await storeState(p, false)
    const { keys } = await loadRawRows(tc.testName)
    t.assert(p._dbref === keys[keys.length - 1] + 1, 'read cursor is past the last row')
    t.assert(errors.length === 1, 'the corrupt row is reported once, not re-applied by storeState')
  } finally {
    await p.destroy()
  }
}

/**
 * A corrupt row that sits outside the tail an incremental trim folds (here
 * between the base row and an existing delta row) is still deleted by the
 * next automatic trim: once a row failed to decode, that trim consolidates
 * fully instead of writing another delta row next to the bad one.
 *
 * @param {t.TestCase} tc
 */
export const testAutomaticTrimDeletesCorruptRowOutsideTail = async tc => {
  await clearDocument(tc.testName)

  // Build [base row] [delta row] with real trim bookkeeping.
  const doc0 = new Y.Doc()
  const arr0 = doc0.getArray('t')
  const p0 = new IndexeddbPersistence(tc.testName, doc0)
  p0._storeTimeout = 1e9
  await p0.whenSynced
  arr0.insert(0, [-1])
  await p0.flush()
  await storeState(p0, true)
  for (let i = 0; i < PREFERRED_TRIM_SIZE + 5; i++) {
    arr0.insert(0, [i])
  }
  await p0.flush()
  await storeState(p0, false)
  t.assert(p0._dbsize === 2, 'base row + one delta row')
  const expected = arr0.toArray()
  await p0.destroy()

  // Plant a corrupt row between the base row and the delta row.
  const [baseKey, deltaKey] = (await loadRawRows(tc.testName)).keys
  t.assert(baseKey + 1 < deltaKey, 'a free key between base and delta row')
  const raw = await openRaw(tc.testName)
  const [rawStore] = idb.transact(raw, ['updates'])
  await idb.put(rawStore, new Uint8Array([1, 2, 3]), baseKey + 1)
  raw.close()

  const doc1 = new Y.Doc()
  const arr1 = doc1.getArray('t')
  const p1 = new IndexeddbPersistence(tc.testName, doc1)
  p1._storeTimeout = 1e9
  /** @type {IndexeddbPersistence|null} */
  let p2 = null
  try {
    await firstLifecycleEvent(p1)
    t.compareArrays(arr1.toArray(), expected, 'every valid row is loaded')
    for (let i = 0; i < PREFERRED_TRIM_SIZE + 5; i++) {
      arr1.insert(0, [10000 + i])
    }
    await p1.flush()
    await storeState(p1, false)
    t.assert(p1._dbsize === 1, 'the trim consolidated fully')

    const persisted = await loadRawRows(tc.testName)
    t.assert(persisted.corrupt === 0, 'the corrupt row is deleted')
    t.compareArrays(persisted.doc.getArray('t').toArray(), arr1.toArray(), 'persisted state is complete')
    await p1.destroy()

    const doc2 = new Y.Doc()
    p2 = new IndexeddbPersistence(tc.testName, doc2)
    const outcome = await firstLifecycleEvent(p2)
    t.assert(outcome.event === 'synced', `reload after the trim must sync (got '${outcome.event}': ${outcome.err && outcome.err.message})`)
    t.compareArrays(doc2.getArray('t').toArray(), arr1.toArray(), 'reload sees all valid content')
  } finally {
    await p1.destroy()
    if (p2) await p2.destroy()
  }
}
