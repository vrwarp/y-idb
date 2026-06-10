import * as Y from 'yjs'
import { IndexeddbPersistence } from '../src/y-idb.js'

/**
 * Durability example showing how to cleanly tear down the persistence
 * instance so that all pending memory changes are written.
 *
 * @param {string} roomName
 * @returns {Promise<void>}
 */
export async function runDurabilityExample (roomName) {
  const doc = new Y.Doc()
  const arr = doc.getArray('durability-store')
  const provider = new IndexeddbPersistence(roomName, doc)
  await provider.whenSynced

  // 1. Listen for write failures. The final flush performed by destroy() is
  // best-effort: destroy() always resolves, and a failure of the final write
  // is reported through the 'error' event rather than a rejection.
  provider.on('error', /** @param {any} err */ (err) => {
    console.error('Final flush failed:', err.message || err)
  })

  // 2. Perform a last-second modification.
  arr.push(['Final save state'])

  // 3. Tear down the provider.
  // destroy() waits for any in-flight write to settle and then flushes all
  // remaining buffered updates before closing the database connection.
  await provider.destroy()
  console.log('Provider destroyed and final changes flushed!')
}
