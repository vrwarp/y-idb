/* eslint-env browser */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, readSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * flush() must back off between attempts even once automatic retries are
 * exhausted. With `maxRetries: 0` every failed attempt exhausts immediately,
 * and `_onFlushFailed` then armed no backoff timer; flush() used to call
 * `_flush()` again straight away, looping through microtasks only. That
 * starved every timer — including the deadline the README tells callers to
 * race flush() against — for as long as the database kept failing.
 *
 * The runner here fails a bounded number of times, so the old microtask loop
 * eventually succeeds and resolves flush() before any timer fires: the test
 * fails on an assertion instead of hanging the run.
 *
 * @param {t.TestCase} tc
 */
export const testFlushBacksOffWhenRetriesAreExhausted = async tc => {
  const name = tc.testName
  await clearDocument(name)
  const doc = new Y.Doc()
  let failuresLeft = 0
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const transactionRunner = async work => {
    if (failuresLeft > 0) {
      failuresLeft--
      throw new Error('runner down')
    }
    return work()
  }
  const provider = new IndexeddbPersistence(name, doc, { maxRetries: 0, transactionRunner })
  await provider.whenSynced
  let errors = 0
  provider.on('error', () => { errors++ })

  failuresLeft = 1000
  doc.getMap('m').set('a', 1)
  let flushed = false
  const flushing = provider.flush().then(() => { flushed = true })

  // A timer must get to run while the database keeps failing.
  await new Promise(resolve => setTimeout(resolve, 20))
  t.assert(!flushed, 'flush() resolved only after spinning through every failure without yielding to timers')
  t.assert(errors <= 2, `flush() retried ${errors} times within 20ms instead of backing off`)

  // Once the database recovers, flush() still drains the queue.
  failuresLeft = 0
  await flushing
  const update = await readSnapshot(name)
  t.assert(update !== null)
  const reloaded = new Y.Doc()
  Y.applyUpdate(reloaded, /** @type {Uint8Array} */ (update))
  t.compare(reloaded.getMap('m').get('a'), 1)
  await provider.destroy()
}
