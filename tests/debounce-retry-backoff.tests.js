import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * Let IndexedDB request callbacks (fake-indexeddb schedules them with
 * setImmediate, not setTimeout) and the promise chains they settle run.
 */
const drain = async () => {
  for (let i = 0; i < 50; i++) {
    await new Promise(resolve => setImmediate(resolve))
  }
}

/**
 * Replace the global setTimeout/clearTimeout with a virtual clock while `fn`
 * runs, so the provider's debounce, retry and flush() timers fire only when
 * the test advances time, in deadline order. Whatever is still armed when
 * `fn` settles is simply discarded.
 *
 * @param {function({ advance: function(number):Promise<void>, advanceUntil: function(Promise<any>, number):Promise<boolean>, now: function():number, armed: function():Array<number> }):Promise<void>} fn
 */
const withVirtualClock = async fn => {
  const realSetTimeout = globalThis.setTimeout
  const realClearTimeout = globalThis.clearTimeout
  let now = 0
  let seq = 0
  /**
   * @type {Map<number, { at: number, id: number, ms: number, handler: function(...any):void, args: Array<any> }>}
   */
  const timers = new Map()
  /**
   * @param {function(...any):void} handler
   * @param {number} [ms]
   * @param {...any} args
   */
  const fakeSetTimeout = (handler, ms = 0, ...args) => {
    const id = ++seq
    const delay = Math.max(0, Number(ms) || 0)
    timers.set(id, { at: now + delay, id, ms: delay, handler, args })
    return id
  }
  /**
   * @param {any} id
   */
  const fakeClearTimeout = id => { timers.delete(id) }
  /**
   * Run every timer due within the next `ms` virtual milliseconds, earliest
   * deadline first, draining async work after each one.
   *
   * @param {number} ms
   */
  const advance = async ms => {
    const target = now + ms
    for (;;) {
      await drain()
      /**
       * @type {{ at: number, id: number, ms: number, handler: function(...any):void, args: Array<any> } | null}
       */
      let next = null
      for (const timer of timers.values()) {
        if (timer.at <= target && (next === null || timer.at < next.at || (timer.at === next.at && timer.id < next.id))) {
          next = timer
        }
      }
      if (next === null) break
      timers.delete(next.id)
      now = next.at
      next.handler(...next.args)
    }
    now = target
    await drain()
  }
  /**
   * Advance in 10ms steps until `promise` settles or `maxMs` have elapsed.
   *
   * @param {Promise<any>} promise
   * @param {number} maxMs
   * @return {Promise<boolean>} whether the promise settled
   */
  const advanceUntil = async (promise, maxMs) => {
    const state = { settled: false }
    promise.then(() => { state.settled = true }, () => { state.settled = true })
    const deadline = now + maxMs
    await drain()
    for (;;) {
      if (state.settled || now >= deadline) return state.settled
      await advance(10)
    }
  }
  globalThis.setTimeout = /** @type {any} */ (fakeSetTimeout)
  globalThis.clearTimeout = /** @type {any} */ (fakeClearTimeout)
  try {
    await fn({
      advance,
      advanceUntil,
      now: () => now,
      armed: () => Array.from(timers.values()).map(timer => timer.ms)
    })
  } finally {
    globalThis.setTimeout = realSetTimeout
    globalThis.clearTimeout = realClearTimeout
    timers.clear()
  }
}

/**
 * A provider whose transactionRunner rejects (as a broken WebKit connection
 * or a rejecting lock would) while `failuresLeft > 0`, and that records the
 * virtual time of every write attempt that reached the runner.
 *
 * @param {string} name
 * @param {function():number} now
 */
const createFailingProvider = (name, now) => {
  const doc = new Y.Doc()
  const control = {
    failuresLeft: 0,
    /**
     * @type {Array<number>}
     */
    attempts: []
  }
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const transactionRunner = async work => {
    if (control.failuresLeft > 0) {
      control.failuresLeft--
      control.attempts.push(now())
      throw new Error('runner down')
    }
    return work()
  }
  const provider = new IndexeddbPersistence(name, doc, { writeDebounceMs: 50, transactionRunner })
  /**
   * @type {Array<any>}
   */
  const errors = []
  provider.on('error', (/** @type {any} */ err) => { errors.push(err) })
  return { doc, provider, control, errors }
}

/**
 * destroy() must leave no provider timer armed, also after a failure episode
 * in which flush() was called while a writeDebounceMs timer was pending.
 *
 * flush() runs `_flush()` at once and that attempt fails, arming retry timer
 * R1. The debounce timer the edit armed then fires inside R1's backoff and
 * calls `_flush()` too; when that attempt fails as well, `_onFlushFailed`
 * stores the new retry timer R2 over R1 without clearing it. R1 later clears
 * the handle, so after the next successful write the provider no longer
 * tracks R2, and destroy() cannot clear it: it keeps the destroyed provider,
 * its doc and its backlog reachable (and a Node process alive) until it
 * fires.
 *
 * @param {t.TestCase} tc
 */
export const testNoRetryTimerOutlivesDestroyAfterDebouncedFailure = async tc => {
  await clearDocument(tc.testName)
  await withVirtualClock(async clock => {
    const { doc, provider, control, errors } = createFailingProvider(tc.testName, clock.now)
    await provider.whenSynced
    await clock.advance(0)

    // The next two write attempts fail.
    control.failuresLeft = 2
    doc.getMap('m').set('a', 1) // arms the writeDebounceMs timer
    const flushing = provider.flush() // attempts at once: failure #1
    t.assert(await clock.advanceUntil(flushing, 60_000), 'flush() did not resolve once the database recovered')
    t.compare(errors.length, 2, 'both injected failures were hit')
    t.compare(control.failuresLeft, 0)

    await provider.destroy()
    await clock.advance(0)
    const leaked = clock.armed()
    t.assert(
      leaked.length === 0,
      `destroy() resolved with ${leaked.length} provider timer(s) still armed (delays: ${leaked.join(', ')} ms)`
    )

    // The edit is persisted.
    const doc2 = new Y.Doc()
    const provider2 = new IndexeddbPersistence(tc.testName, doc2)
    await provider2.whenSynced
    t.compare(doc2.getMap('m').get('a'), 1)
    await provider2.destroy()
  })
}

/**
 * After a failed write, the next attempt waits for the retry backoff (200ms
 * after the first failure, see README `maxRetries`) on every path: the
 * pending writeDebounceMs timer must not attempt the write inside the
 * backoff either. flush() itself waits for the armed retry, and
 * `_scheduleFlush` refuses to schedule a flush while one is armed, but the
 * debounce timer the edit armed before flush() was called still calls
 * `_flush()` when it fires, 50ms into the 200ms backoff.
 *
 * @param {t.TestCase} tc
 */
export const testDebounceTimerRespectsRetryBackoff = async tc => {
  await clearDocument(tc.testName)
  await withVirtualClock(async clock => {
    const { doc, provider, control } = createFailingProvider(tc.testName, clock.now)
    await provider.whenSynced
    await clock.advance(0)

    control.failuresLeft = 1000
    doc.getMap('m').set('a', 1) // arms the writeDebounceMs timer (50ms)
    const flushing = provider.flush() // attempts at once: failure #1 at t0
    await clock.advance(0)
    t.compare(control.attempts.length, 1, 'flush() attempted the write at once')
    const firstFailure = control.attempts[0]

    // Just before the first retry backoff (200ms) elapses.
    await clock.advance(199)
    const early = control.attempts.slice(1).map(at => at - firstFailure)
    t.assert(
      early.length === 0,
      `write retried ${early.join(', ')}ms after the first failure, inside the 200ms retry backoff`
    )

    // Once the database recovers, flush() drains the queue.
    control.failuresLeft = 0
    t.assert(await clock.advanceUntil(flushing, 60_000), 'flush() did not resolve once the database recovered')
    await provider.destroy()
  })
}

/**
 * The same holds for the microtask that coalesces writes when
 * `writeDebounceMs` is 0 (the default). flush() is called while idle and an
 * edit follows in the same tick, so the edit's microtask is queued behind
 * flush()'s first attempt. That attempt fails synchronously (the
 * transaction cannot be opened, as on a closing connection) and arms the
 * retry; the microtask then must not attempt the write inside its backoff,
 * nor arm a second retry timer over the first.
 *
 * @param {t.TestCase} tc
 */
export const testMicrotaskFlushRespectsRetryBackoff = async tc => {
  await clearDocument(tc.testName)
  await withVirtualClock(async clock => {
    const doc = new Y.Doc()
    const provider = new IndexeddbPersistence(tc.testName, doc)
    await provider.whenSynced
    await clock.advance(0)
    const db = /** @type {IDBDatabase} */ (provider.db)
    const realTransaction = db.transaction
    const control = {
      failuresLeft: 2,
      /**
       * @type {Array<number>}
       */
      attempts: []
    }
    /**
     * The next two write attempts fail.
     *
     * @this {IDBDatabase}
     * @param {string | string[]} storeNames
     * @param {IDBTransactionMode} [mode]
     * @param {IDBTransactionOptions} [options]
     * @return {IDBTransaction}
     */
    const closingTransaction = function (storeNames, mode, options) {
      if (mode === 'readwrite' && control.failuresLeft > 0) {
        control.failuresLeft--
        control.attempts.push(clock.now())
        throw new DOMException('The database connection is closing.', 'InvalidStateError')
      }
      return realTransaction.call(this, storeNames, mode, options)
    }
    db.transaction = closingTransaction

    const flushing = provider.flush() // its first attempt runs on a later microtask
    doc.getMap('m').set('a', 1) // queues the coalescing microtask behind it
    await clock.advance(0)
    t.assert(control.attempts.length > 0, 'flush() attempted the write at once')
    const early = control.attempts.slice(1).map(at => at - control.attempts[0])
    t.assert(
      early.length === 0,
      `write retried ${early.join(', ')}ms after the first failure, inside the 200ms retry backoff`
    )

    t.assert(await clock.advanceUntil(flushing, 60_000), 'flush() did not resolve once the database recovered')
    await provider.destroy()
    await clock.advance(0)
    const leaked = clock.armed()
    t.assert(
      leaked.length === 0,
      `destroy() resolved with ${leaked.length} provider timer(s) still armed (delays: ${leaked.join(', ')} ms)`
    )

    const doc2 = new Y.Doc()
    const provider2 = new IndexeddbPersistence(tc.testName, doc2)
    await provider2.whenSynced
    t.compare(doc2.getMap('m').get('a'), 1)
    await provider2.destroy()
  })
}
