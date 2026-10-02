/* eslint-env browser */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, fetchUpdates, storeState, readSnapshot, PREFERRED_TRIM_SIZE } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * A remote peer makes two dependent edits: U1 inserts 'x', U2 inserts 'y'
 * right after it (so U2 cannot integrate without U1).
 *
 * @return {{ u1: Uint8Array, u2: Uint8Array }}
 */
const makeDependentUpdates = () => {
  const remote = new Y.Doc()
  const arr = remote.getArray('t')
  const sv0 = Y.encodeStateVector(remote)
  arr.insert(0, ['x'])
  const u1 = Y.encodeStateAsUpdate(remote, sv0)
  const sv1 = Y.encodeStateVector(remote)
  arr.insert(1, ['y'])
  const u2 = Y.encodeStateAsUpdate(remote, sv1)
  remote.destroy()
  return { u1, u2 }
}

/**
 * Another tab (same database) that received U1 but not U2 persists U1,
 * followed by `extraRows` unrelated edits (one row each). Resolves once the
 * rows have committed and the tab is gone.
 *
 * @param {string} name
 * @param {Uint8Array} update
 * @param {number} [extraRows]
 */
const otherTabStores = async (name, update, extraRows = 0) => {
  const docB = new Y.Doc()
  Y.applyUpdate(docB, update, 'remote')
  const pB = new IndexeddbPersistence(name, docB)
  await pB.whenSynced
  for (let i = 0; i < extraRows; i++) {
    docB.getArray('pad').push([i])
  }
  await pB.flush()
  await pB.destroy()
  docB.destroy()
}

/**
 * Hydrate a fresh doc from everything persisted under `name`.
 *
 * @param {string} name
 * @return {Promise<Y.Doc>}
 */
const loadPersisted = async name => {
  const snapshot = await readSnapshot(name)
  const doc = new Y.Doc()
  if (snapshot) Y.applyUpdate(doc, snapshot)
  return doc
}

/**
 * Tab A receives U2 from the network before U1, so Yjs parks it in
 * pendingStructs (no 'update' event, nothing queued). Tab B persists U1.
 * fetchUpdates on tab A then reads U1 from IndexedDB, which integrates U1
 * AND the parked U2 under origin === provider. Everything that ended up in
 * the doc must be persisted — including U2, which no other tab stored.
 *
 * @param {t.TestCase} tc
 */
export const testPendingStructsIntegratedByFetchUpdatesArePersisted = async tc => {
  const name = tc.testName
  await clearDocument(name)
  const { u1, u2 } = makeDependentUpdates()

  const docA = new Y.Doc()
  const pA = new IndexeddbPersistence(name, docA)
  await pA.whenSynced

  Y.applyUpdate(docA, u2, 'remote')
  t.compare(docA.getArray('t').toJSON(), [], 'U2 is pending until U1 arrives')
  t.assert(docA.store.pendingStructs !== null, 'U2 is parked as pending structs')

  await otherTabStores(name, u1)

  await fetchUpdates(pA)
  t.compare(docA.getArray('t').toJSON(), ['x', 'y'], 'loading U1 integrated the pending U2')
  t.assert(docA.store.pendingStructs === null, 'nothing is left parked')

  await pA.flush()
  await pA.destroy()

  const reloaded = await loadPersisted(name)
  t.compare(reloaded.getArray('t').toJSON(), docA.getArray('t').toJSON(), 'persisted state matches the in-memory doc')
  reloaded.destroy()
  docA.destroy()
  await clearDocument(name)
}

/**
 * Same as above, but U1 is pulled in by the trim's own read of unseen rows
 * (storeState with forceStore = false, below the trim threshold).
 *
 * @param {t.TestCase} tc
 */
export const testPendingStructsIntegratedByTrimReadArePersisted = async tc => {
  const name = tc.testName
  await clearDocument(name)
  const { u1, u2 } = makeDependentUpdates()

  const docA = new Y.Doc()
  const pA = new IndexeddbPersistence(name, docA)
  await pA.whenSynced

  Y.applyUpdate(docA, u2, 'remote')
  t.assert(docA.store.pendingStructs !== null, 'U2 is parked as pending structs')

  await otherTabStores(name, u1)

  await storeState(pA, false)
  t.compare(docA.getArray('t').toJSON(), ['x', 'y'], 'loading U1 integrated the pending U2')
  t.assert(docA.store.pendingStructs === null, 'nothing is left parked')

  await pA.flush()
  await pA.destroy()

  const reloaded = await loadPersisted(name)
  t.compare(reloaded.getArray('t').toJSON(), docA.getArray('t').toJSON(), 'persisted state matches the in-memory doc')
  reloaded.destroy()
  docA.destroy()
  await clearDocument(name)
}

/**
 * Same again, through the real incremental trim: tab A already has trim
 * bookkeeping, and tab B writes U1 plus enough rows to push A's next trim
 * past the threshold. The trim reads (and applies) B's rows, integrating the
 * parked U2, then folds B's rows into one delta row.
 *
 * @param {t.TestCase} tc
 */
export const testPendingStructsIntegratedByIncrementalTrimArePersisted = async tc => {
  const name = tc.testName
  await clearDocument(name)
  const { u1, u2 } = makeDependentUpdates()

  const docA = new Y.Doc()
  const pA = new IndexeddbPersistence(name, docA)
  await pA.whenSynced
  docA.getArray('a').insert(0, ['base'])
  await pA.flush()
  await storeState(pA, true)

  Y.applyUpdate(docA, u2, 'remote')
  t.assert(docA.store.pendingStructs !== null, 'U2 is parked as pending structs')

  await otherTabStores(name, u1, PREFERRED_TRIM_SIZE)

  await storeState(pA, false)
  t.assert(pA._dbsize === 2, 'the trim folded the tail into one delta row')
  t.compare(docA.getArray('t').toJSON(), ['x', 'y'], 'loading U1 integrated the pending U2')
  t.assert(docA.store.pendingStructs === null, 'nothing is left parked')

  await pA.flush()
  await pA.destroy()

  const reloaded = await loadPersisted(name)
  t.compare(reloaded.getArray('t').toJSON(), docA.getArray('t').toJSON(), 'persisted state matches the in-memory doc')
  t.compare(reloaded.getArray('a').toJSON(), ['base'])
  t.assert(reloaded.getArray('pad').length === PREFERRED_TRIM_SIZE)
  reloaded.destroy()
  docA.destroy()
  await clearDocument(name)
}

/**
 * Pending delete sets behave the same way: tab A receives a remote delete
 * of an item it does not have yet (parked in pendingDs). Loading the item
 * from IndexedDB applies the delete under origin === provider; the deletion
 * must still be persisted.
 *
 * @param {t.TestCase} tc
 */
export const testPendingDeleteSetIntegratedByFetchUpdatesIsPersisted = async tc => {
  const name = tc.testName
  await clearDocument(name)
  const remote = new Y.Doc()
  const rarr = remote.getArray('t')
  rarr.insert(0, ['x', 'z'])
  const u1 = Y.encodeStateAsUpdate(remote)
  const sv1 = Y.encodeStateVector(remote)
  rarr.delete(1, 1)
  const del = Y.encodeStateAsUpdate(remote, sv1)
  remote.destroy()

  const docA = new Y.Doc()
  const pA = new IndexeddbPersistence(name, docA)
  await pA.whenSynced

  Y.applyUpdate(docA, del, 'remote')
  t.assert(docA.store.pendingDs !== null, 'the delete is parked as a pending delete set')

  await otherTabStores(name, u1)

  await fetchUpdates(pA)
  t.compare(docA.getArray('t').toJSON(), ['x'], 'loading U1 applied the pending delete')
  t.assert(docA.store.pendingDs === null, 'nothing is left parked')

  await pA.flush()
  await pA.destroy()

  const reloaded = await loadPersisted(name)
  t.compare(reloaded.getArray('t').toJSON(), docA.getArray('t').toJSON(), 'persisted state matches the in-memory doc')
  reloaded.destroy()
  docA.destroy()
  await clearDocument(name)
}
