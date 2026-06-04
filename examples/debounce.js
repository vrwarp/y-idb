import * as Y from 'yjs'
import { IndexeddbPersistence } from '../src/y-idb.js'

/**
 * Debounce example demonstrating how configuring `writeDebounceMs`
 * aggregates high-cadence edits into a single database transaction.
 *
 * @param {string} roomName
 * @returns {Promise<void>}
 */
export async function runDebounceExample (roomName) {
  const doc = new Y.Doc()
  const text = doc.getText('editor')

  // 1. Initialize persistence with a debounce delay (e.g. 100ms)
  const provider = new IndexeddbPersistence(roomName, doc, { writeDebounceMs: 100 })
  await provider.whenSynced

  // 2. Perform multiple synchronous or rapid updates.
  // Because they occur within the 100ms window, they will be batched
  // together in memory and flushed in a single transaction.
  text.insert(0, 'H')
  text.insert(1, 'e')
  text.insert(2, 'l')
  text.insert(3, 'l')
  text.insert(4, 'o')

  // Wait a short time (less than debounce) - updates are still in memory
  await new Promise(resolve => setTimeout(resolve, 30))

  // Perform another update within the same debounce window
  text.insert(5, ' World')

  // Wait for the debounce timer to expire and flush
  await new Promise(resolve => setTimeout(resolve, 120))

  await provider.destroy()
}
