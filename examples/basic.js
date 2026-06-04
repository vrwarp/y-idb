import * as Y from 'yjs'
import { IndexeddbPersistence } from '../src/y-idb.js'

/**
 * Basic example demonstrating how to set up IndexeddbPersistence
 * and listen to the 'synced' event.
 *
 * @param {string} roomName
 * @returns {Promise<void>}
 */
export async function runBasicExample (roomName) {
  const doc = new Y.Doc()
  const arr = doc.getArray('tasks')

  // 1. Initialize the IndexedDB persistence provider.
  // By default, this provider uses microtask coalescing (writeDebounceMs = 0).
  const provider = new IndexeddbPersistence(roomName, doc)

  // 2. Wait for initial sync from IndexedDB.
  // The 'synced' event is emitted once existing data is loaded.
  await provider.whenSynced
  console.log('Database synced! Current list length:', arr.length)

  // 3. Make updates. They will be automatically coalesced and stored in IndexedDB.
  arr.push(['Buy milk'])
  arr.push(['Read a book'])

  // 4. Clean up the provider connection.
  await provider.destroy()
}
