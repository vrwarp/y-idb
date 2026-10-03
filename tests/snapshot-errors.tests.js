/* eslint-env browser */

import * as Y from 'yjs'
import * as t from 'lib0/testing.js'
import { isNode } from 'lib0/environment.js'
import { clearDocument, readSnapshot, writeSnapshot } from '../src/y-idb.js'

/**
 * Route every exception that escapes to the host's top level (an exception
 * thrown out of an IndexedDB event handler, for instance) to `onUncaught`
 * instead of the host's default handling, which for the node test runner is
 * `process.exit(1)`. Returns a function that restores the previous handlers.
 *
 * @param {function(any):void} onUncaught
 * @return {function():void}
 */
const captureUncaught = onUncaught => {
  if (isNode) {
    const previous = process.listeners('uncaughtException')
    process.removeAllListeners('uncaughtException')
    process.on('uncaughtException', onUncaught)
    return () => {
      process.removeListener('uncaughtException', onUncaught)
      previous.forEach(listener => process.on('uncaughtException', listener))
    }
  }
  /**
   * @param {ErrorEvent} event
   */
  const listener = event => {
    event.preventDefault()
    onUncaught(event.error)
  }
  window.addEventListener('error', listener)
  return () => window.removeEventListener('error', listener)
}

/**
 * Append `row` to the `updates` store of the existing database `name` through
 * a raw connection, resolving once the write transaction has committed.
 *
 * @param {string} name
 * @param {Uint8Array} row
 * @return {Promise<void>}
 */
const appendRawRow = (name, row) => new Promise((resolve, reject) => {
  const open = indexedDB.open(name)
  open.onerror = () => reject(open.error)
  open.onsuccess = () => {
    const db = open.result
    const tx = db.transaction(['updates'], 'readwrite')
    tx.objectStore('updates').add(row)
    tx.oncomplete = () => {
      db.close()
      resolve()
    }
    tx.onerror = tx.onabort = () => {
      db.close()
      reject(tx.error)
    }
  }
})

/**
 * readSnapshot() on a database whose rows cannot be merged (a valid snapshot
 * row followed by an undecodable row) must REJECT with the merge error.
 *
 * Bug: the rows are converted and merged inside the transaction's
 * `oncomplete` handler without a try/catch. `Y.mergeUpdates` throws there, the
 * exception escapes the IndexedDB event handler as an uncaught exception, and
 * neither resolve nor reject is ever called: the promise stays pending
 * forever, hanging every caller (backup, export, migration, boot).
 *
 * Verdict ordering is explicit rather than timing based: the outcome is
 * decided either by readSnapshot settling, or by an exception escaping to the
 * top level. In the latter case the only code path that could still settle
 * the promise has already thrown, so one further macrotask is enough to prove
 * it is stuck. (A long safety-net timer only turns an unexpected hang into a
 * failure instead of stalling the whole suite.)
 *
 * @param {t.TestCase} tc
 */
export const testReadSnapshotRejectsWhenStoredRowsCannotBeMerged = async tc => {
  await clearDocument(tc.testName)
  const source = new Y.Doc()
  source.getMap('m').set('k', 'v')
  await writeSnapshot(tc.testName, Y.encodeStateAsUpdate(source))
  await appendRawRow(tc.testName, new Uint8Array([1, 2, 3]))
  // Sanity check on the fixture: these rows really cannot be merged.
  t.fails(() => {
    Y.mergeUpdates([Y.encodeStateAsUpdate(source), new Uint8Array([1, 2, 3])])
  })

  /** @type {Array<any>} */
  const uncaught = []
  /** @type {any} */
  let safetyNet = null
  /** @type {function():void} */
  let restore = () => {}
  /**
   * @type {{ kind: 'resolved', value: any } | { kind: 'rejected', error: any } | { kind: 'pending', reason: string }}
   */
  let outcome
  try {
    outcome = await new Promise(resolve => {
      restore = captureUncaught(err => {
        uncaught.push(err)
        // Let any remaining settle path run before declaring the promise stuck.
        setTimeout(() => resolve({ kind: 'pending', reason: 'an exception escaped to the top level' }), 0)
      })
      safetyNet = setTimeout(() => resolve({ kind: 'pending', reason: 'safety-net timeout' }), 10000)
      readSnapshot(tc.testName).then(
        value => resolve({ kind: 'resolved', value }),
        error => resolve({ kind: 'rejected', error })
      )
    })
  } finally {
    clearTimeout(safetyNet)
    restore()
  }
  await clearDocument(tc.testName)

  t.assert(
    outcome.kind === 'rejected',
    outcome.kind === 'pending'
      ? `readSnapshot never settled (${outcome.reason}); uncaught: ${uncaught.map(e => String(e && (e.errors ? e.errors[0] : e))).join(', ')}`
      : 'readSnapshot must reject when the stored rows cannot be merged, not resolve'
  )
  t.assert(outcome.kind === 'rejected' && outcome.error instanceof Error, 'readSnapshot must reject with an Error')
  t.assert(uncaught.length === 0, 'the merge error must not escape as an uncaught exception')
}
