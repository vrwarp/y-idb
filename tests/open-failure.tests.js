/* eslint-env browser */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * Collect unhandled promise rejections while `fn` runs, instead of letting
 * the test harness (tests/node.js exits the process on the first one) or the
 * browser swallow them. The harness's own listeners are restored afterwards.
 *
 * @param {(reasons: Array<any>) => Promise<void>} fn
 */
const withUnhandledRejectionsCaptured = async fn => {
  /** @type {Array<any>} */
  const reasons = []
  if (typeof process !== 'undefined' && typeof process.on === 'function') {
    const saved = process.listeners('unhandledRejection')
    /**
     * @param {any} reason
     */
    const onRejection = reason => { reasons.push(reason) }
    process.removeAllListeners('unhandledRejection')
    process.on('unhandledRejection', onRejection)
    try {
      await fn(reasons)
    } finally {
      process.removeListener('unhandledRejection', onRejection)
      saved.forEach(listener => process.on('unhandledRejection', listener))
    }
  } else {
    /**
     * @param {PromiseRejectionEvent} event
     */
    const onRejection = event => {
      event.preventDefault()
      reasons.push(event.reason)
    }
    addEventListener('unhandledrejection', onRejection)
    try {
      await fn(reasons)
    } finally {
      removeEventListener('unhandledrejection', onRejection)
    }
  }
}

/**
 * Let pending microtasks and the runtime's unhandled-rejection tracking run
 * (Node reports unhandled rejections after the microtask queue of a
 * macrotask has drained).
 */
const drainTasks = async () => {
  for (let i = 0; i < 3; i++) {
    await new Promise(resolve => setTimeout(resolve, 0))
  }
}

/**
 * Resolve with the first 'error' event payload, or with `null` once
 * `deadlineMs` has passed without one. The deadline is only an upper bound
 * for the failure case; a correct implementation resolves via the event.
 *
 * @param {IndexeddbPersistence} persistence
 * @param {number} deadlineMs
 * @return {Promise<any>}
 */
const firstErrorOrNull = (persistence, deadlineMs) => new Promise(resolve => {
  const timer = setTimeout(() => resolve(null), deadlineMs)
  persistence.on('error', (/** @type {any} */ err) => {
    clearTimeout(timer)
    resolve(err === undefined ? new Error('error event without payload') : err)
  })
})

/**
 * When `indexedDB.open` fails for the document's database (Chrome's
 * "Internal error opening backing store", storage disabled, VersionError,
 * ...), the provider must report it through the 'error' event — the README
 * contract for failed database operations — and must not leak an unhandled
 * promise rejection, which crashes hosts that treat those as fatal and gives
 * the app no way to notice that persistence is unavailable.
 *
 * The open request is stubbed so the failure is fired explicitly at a
 * known point: no timing is involved.
 *
 * @param {t.TestCase} tc
 */
export const testIndexedDbOpenFailureEmitsErrorWithoutUnhandledRejection = async tc => {
  const name = tc.testName
  const factory = /** @type {any} */ (indexedDB)
  const hadOwnOpen = Object.prototype.hasOwnProperty.call(factory, 'open')
  const realOpen = factory.open
  /** @type {any} */
  let failingRequest = null
  factory.open = function (/** @type {string} */ dbName, /** @type {any[]} */ ...rest) {
    if (dbName === name) {
      failingRequest = {}
      return failingRequest
    }
    return realOpen.call(this, dbName, ...rest)
  }
  try {
    await withUnhandledRejectionsCaptured(async reasons => {
      const doc = new Y.Doc()
      const persistence = new IndexeddbPersistence(name, doc)
      // A consumer awaiting whenSynced: whatever a fix does with it, it is
      // not the source of an unhandled rejection here.
      persistence.whenSynced.then(() => {}, () => {})
      const errorEvent = firstErrorOrNull(persistence, 1000)

      t.assert(failingRequest !== null, 'the provider should have opened its database')
      const openError = new DOMException('Internal error opening backing store for indexedDB.open.', 'UnknownError')
      // Deliver the failure the way an IDBRequest does: the error is set on
      // the request, which is the event's target.
      failingRequest.error = openError
      failingRequest.onerror({ target: failingRequest })

      const err = await errorEvent
      // Edits made after the failure are only buffered; they must not cause
      // further unhandled rejections either.
      doc.getMap('m').set('k', 1)
      await drainTasks()
      await persistence.destroy()
      await drainTasks()

      t.assert(
        reasons.length === 0,
        `a failed indexedDB.open must not cause an unhandled rejection, got: ${reasons.map(String).join(' | ')}`
      )
      t.assert(err !== null, 'a failed indexedDB.open must be reported through the "error" event')
      t.assert(persistence.synced === false, 'the provider must not claim to be synced')
    })
  } finally {
    if (hadOwnOpen) {
      factory.open = realOpen
    } else {
      delete factory.open
    }
  }
}

/**
 * Create database `name` (version 1) holding only a store this module does
 * not use, so it lacks the 'updates' and 'custom' stores — e.g. a database
 * of the same name created by other code.
 *
 * @param {string} name
 * @return {Promise<void>}
 */
const createForeignDatabase = name => new Promise((resolve, reject) => {
  const request = indexedDB.open(name, 1)
  request.onupgradeneeded = () => { request.result.createObjectStore('foreign') }
  request.onsuccess = () => {
    request.result.close()
    resolve()
  }
  request.onerror = () => reject(request.error)
})

/**
 * When the database opens but hydration cannot even start — here the
 * database exists without the 'updates' store, so creating the hydration
 * transaction throws NotFoundError synchronously — the failure must be
 * reported through the 'error' event (as the existing initial-sync failure
 * path does) instead of escaping as an unhandled rejection.
 *
 * @param {t.TestCase} tc
 */
export const testMissingUpdatesStoreEmitsErrorWithoutUnhandledRejection = async tc => {
  const name = tc.testName
  await clearDocument(name)
  await createForeignDatabase(name)
  try {
    await withUnhandledRejectionsCaptured(async reasons => {
      const doc = new Y.Doc()
      const persistence = new IndexeddbPersistence(name, doc)
      persistence.whenSynced.then(() => {}, () => {})

      const err = await firstErrorOrNull(persistence, 2000)
      await drainTasks()
      await persistence.destroy()
      await drainTasks()

      t.assert(
        reasons.length === 0,
        `a hydration that cannot start must not cause an unhandled rejection, got: ${reasons.map(String).join(' | ')}`
      )
      t.assert(err !== null, 'a hydration that cannot start must be reported through the "error" event')
      t.assert(persistence.synced === false, 'the provider must not claim to be synced')
    })
  } finally {
    await clearDocument(name)
  }
}

/**
 * A transactionRunner that throws synchronously (instead of returning a
 * rejected promise) when asked to run the hydration transaction must be
 * reported through the 'error' event like any other initial-sync failure,
 * not escape as an unhandled rejection.
 *
 * @param {t.TestCase} tc
 */
export const testSyncThrowingTransactionRunnerEmitsErrorWithoutUnhandledRejection = async tc => {
  const name = tc.testName
  await clearDocument(name)
  try {
    await withUnhandledRejectionsCaptured(async reasons => {
      const runnerError = new Error('transaction runner unavailable')
      const doc = new Y.Doc()
      const persistence = new IndexeddbPersistence(name, doc, {
        transactionRunner: () => { throw runnerError }
      })
      persistence.whenSynced.then(() => {}, () => {})

      const err = await firstErrorOrNull(persistence, 2000)
      await drainTasks()
      await persistence.destroy()
      await drainTasks()

      t.assert(
        reasons.length === 0,
        `a throwing transactionRunner must not cause an unhandled rejection, got: ${reasons.map(String).join(' | ')}`
      )
      t.assert(err === runnerError, 'the runner failure must be reported through the "error" event')
      t.assert(persistence.synced === false, 'the provider must not claim to be synced')
    })
  } finally {
    await clearDocument(name)
  }
}
