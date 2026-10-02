/* eslint-env browser */

import * as Y from 'yjs'
import * as t from 'lib0/testing.js'
import { IndexeddbPersistence, clearDocument, readSnapshot } from '../src/y-idb.js'

/**
 * Regression tests for tearing a provider down before its IndexedDB
 * connection has opened (a short-lived import job, closing a just-opened
 * book, a quick unmount, a slow cold open on mobile/Safari).
 *
 * Unless noted otherwise, a test constructs the provider, edits the doc and
 * calls destroy() in the SAME synchronous tick: `indexedDB.open` always
 * completes in a later task, so the teardown deterministically starts before
 * the connection exists. The contract (README "destroy()",
 * examples/durability.js) is that destroy() persists every buffered update
 * before closing, so the edit must be readable from the database once
 * destroy() has resolved.
 */

/**
 * Load the complete persisted state of `name` into a fresh doc, without
 * binding a provider to it.
 *
 * @param {string} name
 * @return {Promise<{ snapshot: Uint8Array|null, doc: Y.Doc }>}
 */
const loadPersisted = async name => {
  const snapshot = await readSnapshot(name)
  const doc = new Y.Doc()
  if (snapshot !== null) {
    Y.applyUpdate(doc, snapshot)
  }
  return { snapshot, doc }
}

/**
 * @param {t.TestCase} tc
 */
export const testDestroyBeforeOpenPersistsBufferedUpdate = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  /** @type {any[]} */
  const errors = []
  persistence.on('error', (/** @type {any} */ err) => { errors.push(err) })
  let syncedFired = false
  persistence.on('synced', () => { syncedFired = true })
  doc.getMap('m').set('k', 'v')
  // Same tick as construction: the initial sync cannot have happened yet.
  t.assert(!persistence.synced)
  await persistence.destroy()

  const { snapshot, doc: reloaded } = await loadPersisted(tc.testName)
  t.assert(snapshot !== null, 'destroy() before the connection opened persisted nothing')
  t.compare(reloaded.getMap('m').toJSON(), { k: 'v' })
  // Persisted, not merely reported: nothing failed, so nothing is emitted.
  t.assert(errors.length === 0, 'the teardown write must not emit an error')
  t.assert(persistence._pendingUpdates.length === 0, 'the buffer must be drained')
  // The teardown write must not start the initial sync (testEarlyDestroy).
  t.assert(!syncedFired && !persistence.synced, "'synced' must not fire after destroy()")
}

/**
 * Same scenario through `doc.destroy()`, which tears the provider down via
 * its 'destroy' listener.
 *
 * @param {t.TestCase} tc
 */
export const testDocDestroyBeforeOpenPersistsBufferedUpdate = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  doc.getArray('a').push(['closed-right-away'])
  t.assert(!persistence.synced)
  doc.destroy()
  // destroy() is idempotent: this resolves when the teardown started by
  // doc.destroy() has finished.
  await persistence.destroy()

  const { snapshot, doc: reloaded } = await loadPersisted(tc.testName)
  t.assert(snapshot !== null, 'doc.destroy() before the connection opened persisted nothing')
  t.compare(reloaded.getArray('a').toArray(), ['closed-right-away'])
}

/**
 * The doc already holds content when the provider is bound. Normally the
 * initial sync persists that content; when the provider is destroyed before
 * the connection opens, the final write has to cover it as well — the new
 * edit was made by the same client on top of it, so persisting the edit
 * alone would leave it undecodable (pending on missing structs) on reload.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyBeforeOpenPersistsEditOnTopOfPreexistingContent = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  doc.getArray('a').insert(0, ['existing'])
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  doc.getArray('a').push(['edit'])
  t.assert(!persistence.synced)
  await persistence.destroy()

  const { snapshot, doc: reloaded } = await loadPersisted(tc.testName)
  t.assert(snapshot !== null, 'destroy() before the connection opened persisted nothing')
  t.compare(reloaded.getArray('a').toArray(), ['existing', 'edit'])
}

/**
 * flush() started alongside destroy() before the connection opens: flush()
 * returns early once it sees the provider destroyed, so the teardown write
 * must still persist the buffered edit.
 *
 * @param {t.TestCase} tc
 */
export const testFlushAlongsideDestroyBeforeOpenPersistsBufferedUpdate = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  doc.getMap('m').set('k', 'v')
  t.assert(!persistence.synced)
  await Promise.all([persistence.flush(), persistence.destroy()])

  const { snapshot, doc: reloaded } = await loadPersisted(tc.testName)
  t.assert(snapshot !== null, 'flush() + destroy() before the connection opened persisted nothing')
  t.compare(reloaded.getMap('m').toJSON(), { k: 'v' })
}

/**
 * Destroyed after the connection opened but while the initial sync is still
 * running, with a flush of the new edit already in flight. The flush writes
 * the edit, but the initial sync sees the provider destroyed and skips its
 * write of the doc's pre-existing state, so the teardown write has to supply
 * that state even though nothing is left in the buffer.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyDuringInitialSyncPersistsPreexistingContent = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  doc.getArray('a').insert(0, ['existing'])
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  // The constructor's own `_db.then` callback runs first and starts the
  // initial sync transaction.
  await persistence._db
  doc.getArray('a').push(['edit'])
  // One microtask hop: the flush queued by the push runs and opens its
  // transaction, which IndexedDB queues behind the initial sync's.
  await Promise.resolve()
  t.assert(persistence._writing, 'flush should be in flight')
  t.assert(!persistence.synced)
  await persistence.destroy()

  const { snapshot, doc: reloaded } = await loadPersisted(tc.testName)
  t.assert(snapshot !== null, 'destroy() during the initial sync persisted nothing')
  t.compare(reloaded.getArray('a').toArray(), ['existing', 'edit'])
}
