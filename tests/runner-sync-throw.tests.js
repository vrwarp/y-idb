/* eslint-env browser */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, readSnapshot, writeSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'
import * as promise from 'lib0/promise.js'

/**
 * Settle `p` within `ms`, reporting how it ended instead of throwing, so a
 * wedged provider fails an assertion rather than hanging the test run.
 *
 * @param {Promise<any>} p
 * @param {number} ms
 * @return {Promise<string>}
 */
const settleWithin = (p, ms) => {
  /** @type {any} */
  let timer = null
  const deadline = new Promise(resolve => {
    timer = setTimeout(() => resolve('timed out'), ms)
  })
  return Promise.race([
    p.then(() => 'resolved', err => 'rejected: ' + (err && err.message)),
    deadline
  ]).then(outcome => {
    clearTimeout(timer)
    return /** @type {string} */ (outcome)
  })
}

/**
 * A transactionRunner that is not `async` and throws synchronously (e.g. a
 * sequencer/lock that was disposed) the next time `throwNext.value` is set.
 *
 * @param {{ value: boolean }} throwNext
 * @return {<T>(work: () => Promise<T>) => Promise<T>}
 */
const createSyncThrowingRunner = throwNext => work => {
  if (throwNext.value) {
    throwNext.value = false
    throw new Error('runner disposed')
  }
  return work()
}

/**
 * A transactionRunner that throws synchronously must be handled like one
 * that returns a rejected promise (see testTransactionRunnerFailureRecovery):
 * the batch is kept, 'error' is emitted, the write is retried, and flush()
 * keeps retrying until the data is persisted. Edits made afterwards must
 * still be written, and everything must survive a reload.
 *
 * The write is started by flush() rather than the automatic scheduler (a
 * debounce keeps the latter off the microtask queue), so with the bug the
 * synchronous throw surfaces as a flush() rejection instead of an uncaught
 * exception that would abort the whole test run.
 *
 * @param {t.TestCase} tc
 */
export const testRunnerSyncThrowDuringFlushIsRecovered = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const throwNext = { value: false }
  const persistence = new IndexeddbPersistence(tc.testName, doc, {
    transactionRunner: createSyncThrowingRunner(throwNext),
    writeDebounceMs: 50
  })
  await persistence.whenSynced

  /** @type {Array<any>} */
  const errors = []
  persistence.on('error', (/** @type {any} */ err) => { errors.push(err) })

  throwNext.value = true
  doc.getArray('t').insert(0, [1])
  const first = await settleWithin(persistence.flush(), 3000)
  t.compare(first, 'resolved', 'flush() must retry past a runner that throws synchronously, as it does for one that rejects')
  t.assert(errors.length === 1, `the runner failure must be emitted as one 'error' event (got ${errors.length})`)
  t.assert(String(errors[0]).includes('runner disposed'), 'the emitted error is the runner error')

  // The provider must not be wedged: a later edit is still persisted.
  doc.getArray('t').insert(1, [2])
  const second = await settleWithin(persistence.flush(), 3000)
  t.compare(second, 'resolved', 'edits after the runner failure must still be flushed')

  t.compare(await settleWithin(persistence.destroy(), 3000), 'resolved', 'destroy() must resolve')

  const doc2 = new Y.Doc()
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced
  t.compareArrays(doc2.getArray('t').toArray(), [1, 2], 'both edits must survive a reload')
  await persistence2.destroy()
}

/**
 * The same failure on the default path (writeDebounceMs 0), where the write
 * is started from a queueMicrotask callback: the synchronous throw must not
 * escape as an uncaught exception, and the backoff retry must persist the
 * batch.
 *
 * @param {t.TestCase} tc
 */
export const testRunnerSyncThrowDuringScheduledFlushIsRecovered = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const throwNext = { value: false }
  const persistence = new IndexeddbPersistence(tc.testName, doc, {
    transactionRunner: createSyncThrowingRunner(throwNext)
  })
  await persistence.whenSynced

  /** @type {Array<any>} */
  const errors = []
  persistence.on('error', (/** @type {any} */ err) => { errors.push(err) })

  throwNext.value = true
  doc.getArray('t').insert(0, [1])
  await promise.wait(50)
  t.assert(errors.length === 1, `the runner failure must be emitted as one 'error' event (got ${errors.length})`)
  t.assert(String(errors[0]).includes('runner disposed'), 'the emitted error is the runner error')

  doc.getArray('t').insert(1, [2])
  t.compare(await settleWithin(persistence.flush(), 3000), 'resolved', 'the failed batch and later edits must be flushed')
  t.compare(await settleWithin(persistence.destroy(), 3000), 'resolved', 'destroy() must resolve')

  const doc2 = new Y.Doc()
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced
  t.compareArrays(doc2.getArray('t').toArray(), [1, 2], 'both edits must survive a reload')
  await persistence2.destroy()
}

/**
 * destroy()'s final write of still-pending updates must treat a runner that
 * throws synchronously like one that rejects: surface the failure through
 * 'error' instead of rejecting (see testDestroyEmitsErrorInsteadOfRejecting),
 * and still close the database connection.
 *
 * @param {t.TestCase} tc
 */
export const testRunnerSyncThrowDuringDestroyEmitsError = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const throwNext = { value: false }
  const persistence = new IndexeddbPersistence(tc.testName, doc, {
    transactionRunner: createSyncThrowingRunner(throwNext)
  })
  await persistence.whenSynced
  const db = /** @type {IDBDatabase} */ (persistence.db)

  /** @type {Array<any>} */
  const errors = []
  persistence.on('error', (/** @type {any} */ err) => { errors.push(err) })

  // Edit and destroy in the same tick: the edit is still pending when
  // destroy() runs its final write, which is where the runner throws.
  throwNext.value = true
  doc.getArray('t').insert(0, [1])
  const outcome = await settleWithin(persistence.destroy(), 3000)

  t.compare(outcome, 'resolved', 'destroy() must not reject when the runner throws synchronously')
  t.assert(errors.length === 1, `the runner failure must be emitted as one 'error' event (got ${errors.length})`)
  t.assert(String(errors[0]).includes('runner disposed'), 'the emitted error is the runner error')
  // A transaction on a closed connection throws InvalidStateError.
  let closed = false
  try {
    db.transaction(['updates'], 'readonly')
  } catch (e) {
    closed = true
  }
  t.assert(closed, 'destroy() must close the database connection')
}

/**
 * readSnapshot() and writeSnapshot() return promises; a runner that throws
 * synchronously must reject them rather than throw out of the call.
 *
 * @param {t.TestCase} tc
 */
export const testRunnerSyncThrowRejectsSnapshotHelpers = async tc => {
  await clearDocument(tc.testName)
  /**
   * @param {() => Promise<any>} call
   * @return {Promise<string>}
   */
  const outcomeOf = async call => {
    /** @type {Promise<any>} */
    let p
    try {
      p = call()
    } catch (e) {
      return 'threw synchronously'
    }
    return settleWithin(p, 3000)
  }
  const update = Y.encodeStateAsUpdate(new Y.Doc())
  t.compare(
    await outcomeOf(() => writeSnapshot(tc.testName, update, { transactionRunner: createSyncThrowingRunner({ value: true }) })),
    'rejected: runner disposed',
    'writeSnapshot() must reject with the runner error'
  )
  t.compare(
    await outcomeOf(() => readSnapshot(tc.testName, { transactionRunner: createSyncThrowingRunner({ value: true }) })),
    'rejected: runner disposed',
    'readSnapshot() must reject with the runner error'
  )
}
