/* eslint-env browser */

import * as Y from 'yjs'
import { clearDocument, readSnapshot, writeSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * Runs `fn` and reports how it settled, treating a synchronous throw the same
 * as a rejection (a fix may validate its arguments eagerly).
 *
 * @param {() => Promise<any>} fn
 * @return {Promise<{ ok: boolean, error: any }>}
 */
const settle = async fn => {
  try {
    await fn()
    return { ok: true, error: null }
  } catch (error) {
    return { ok: false, error }
  }
}

/**
 * Detach `buffer` (as postMessage with a transfer list to a worker would),
 * leaving every view on it with length 0 and un-cloneable.
 *
 * @param {ArrayBuffer} buffer
 */
const detach = buffer => {
  structuredClone(buffer, { transfer: [buffer] })
  t.assert(buffer.byteLength === 0, 'buffer is detached')
}

/**
 * Stores a known snapshot in database `name` and returns its bytes.
 *
 * @param {string} name
 * @return {Promise<Uint8Array>}
 */
const storeGoodSnapshot = async name => {
  const good = new Y.Doc()
  good.getMap('library').set('book-1', { title: 'Persisted Book' })
  const goodUpdate = Y.encodeStateAsUpdate(good)
  good.destroy()
  await writeSnapshot(name, goodUpdate)
  const before = await readSnapshot(name)
  t.assert(before !== null, 'good snapshot persisted')
  t.compareArrays(Array.from(/** @type {Uint8Array} */ (before)), Array.from(goodUpdate))
  return goodUpdate
}

/**
 * A writeSnapshot() that fails (here: the update buffer is detached, so
 * structured clone rejects it with DataCloneError) must leave the previously
 * stored snapshot untouched. The README promises an atomic replacement of
 * "the complete content": either the new content is written or nothing is.
 *
 * Bug: store.clear() is issued before store.add(update); add throws
 * synchronously inside the Promise executor, the promise rejects, but the
 * transaction is never aborted and auto-commits the clear() alone, so the
 * database is left empty (readSnapshot() resolves null).
 *
 * @param {t.TestCase} tc
 */
export const testRejectedWriteSnapshotKeepsPreviousSnapshot = async tc => {
  await clearDocument(tc.testName)
  const goodUpdate = await storeGoodSnapshot(tc.testName)

  const next = new Y.Doc()
  next.getMap('library').set('book-2', { title: 'Replacement Book' })
  const badUpdate = Y.encodeStateAsUpdate(next)
  next.destroy()
  detach(badUpdate.buffer)

  const result = await settle(() => writeSnapshot(tc.testName, badUpdate))
  t.assert(!result.ok, 'writeSnapshot with a detached buffer must fail')

  const after = await readSnapshot(tc.testName)
  t.assert(after !== null, 'a failed writeSnapshot must not wipe the database (readSnapshot returned null)')
  t.compareArrays(Array.from(/** @type {Uint8Array} */ (after)), Array.from(goodUpdate), 'previous snapshot is intact')
}

/**
 * A caller that transfers the update buffer right after calling
 * writeSnapshot() (e.g. hands it to a worker) must never end up with an
 * empty database. Any atomic outcome is acceptable: either the call resolves
 * and the new content (as it was at call time) is stored, or it rejects and
 * the previous snapshot is left intact.
 *
 * Bug: writeSnapshot reads `update` only after openDB resolves, so the add
 * hits the detached buffer, throws DataCloneError after clear() was issued,
 * and the transaction commits the clear() alone.
 *
 * @param {t.TestCase} tc
 */
export const testWriteSnapshotBufferTransferredAfterCallNeverWipesDatabase = async tc => {
  await clearDocument(tc.testName)
  const goodUpdate = await storeGoodSnapshot(tc.testName)

  const next = new Y.Doc()
  next.getMap('library').set('book-2', { title: 'Replacement Book' })
  const nextUpdate = Y.encodeStateAsUpdate(next)
  const nextBytes = Array.from(nextUpdate)
  next.destroy()

  const result = await settle(() => {
    const pending = writeSnapshot(tc.testName, nextUpdate)
    // Synchronously after the call, before any IndexedDB work has run.
    detach(nextUpdate.buffer)
    return pending
  })

  const after = await readSnapshot(tc.testName)
  t.assert(after !== null, `writeSnapshot ${result.ok ? 'resolved' : 'rejected (' + (result.error && result.error.name) + ')'} but left the database empty`)
  t.compareArrays(
    Array.from(/** @type {Uint8Array} */ (after)),
    result.ok ? nextBytes : Array.from(goodUpdate),
    result.ok ? 'resolved: new snapshot fully stored' : 'rejected: previous snapshot intact'
  )
}
