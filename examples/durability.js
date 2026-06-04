import * as Y from 'yjs'
import { IndexeddbPersistence } from '../src/y-idb.js'

/**
 * Durability example showing how to cleanly tear down the persistence
 * instance and guarantee that all pending memory changes are written.
 *
 * @param {string} roomName
 * @returns {Promise<void>}
 */
export async function runDurabilityExample (roomName) {
  const doc = new Y.Doc()
  const arr = doc.getArray('durability-store')
  const provider = new IndexeddbPersistence(roomName, doc)
  await provider.whenSynced

  // 1. Perform a last-second modification.
  arr.push(['Final save state'])

  // 2. Tear down the provider.
  // destroy() returns a Promise that resolves ONLY after any pending updates
  // are successfully flushed to IndexedDB. Awaiting this guarantees durability.
  await provider.destroy()
  console.log('Provider destroyed and final changes flushed successfully!')
}
