import * as Y from 'yjs'
import { IndexeddbPersistence } from '../src/y-idb.js'

/**
 * Error handling example demonstrating how to listen to the 'error'
 * event to respond to database failures.
 *
 * @param {string} roomName
 * @returns {Promise<void>}
 */
export async function runErrorHandlingExample (roomName) {
  const doc = new Y.Doc()
  const arr = doc.getArray('data')
  const provider = new IndexeddbPersistence(roomName, doc)
  await provider.whenSynced

  // 1. Listen for the 'error' event to capture failures.
  // This event propagates issues like QuotaExceededError or transaction aborts.
  provider.on('error', /** @param {any} err */ (err) => {
    console.error('Persistence Error Captured:', err.message || err)
  })

  // 2. Perform updates. If the database connection encounters a failure,
  // the error listener will trigger, and the updates will remain in memory
  // for a safe retry.
  arr.push([1])

  await provider.destroy()
}
