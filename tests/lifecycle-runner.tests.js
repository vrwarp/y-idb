/* eslint-env browser */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, readSnapshot } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * Install capturing stand-ins for `addEventListener` and `document` so the
 * lifecycle listeners IndexeddbPersistence registers can be fired for real
 * (the default harness installs no-op stubs).
 *
 * @param {(fire: (type: string) => void, setVisibility: (state: string) => void) => Promise<void>} fn
 */
const withCapturedLifecycle = async fn => {
  const originalAdd = globalThis.addEventListener
  const originalDocument = globalThis.document
  /** @type {Map<string, Function[]>} */
  const listeners = new Map()
  const record = (/** @type {string} */ type, /** @type {Function} */ handler) => {
    const existing = listeners.get(type) || []
    existing.push(handler)
    listeners.set(type, existing)
  }
  globalThis.addEventListener = /** @type {any} */ (record)
  globalThis.document = /** @type {any} */ ({
    addEventListener: record,
    removeEventListener: () => {},
    visibilityState: 'visible'
  })
  const fire = (/** @type {string} */ type) => {
    for (const handler of listeners.get(type) || []) handler()
  }
  const setVisibility = (/** @type {string} */ state) => {
    /** @type {any} */ (globalThis.document).visibilityState = state
  }
  try {
    await fn(fire, setVisibility)
  } finally {
    globalThis.addEventListener = originalAdd
    globalThis.document = originalDocument
  }
}

/**
 * README contract: the page-hide flush is one of the operations that open
 * their own transaction, and supplying `options.transactionRunner` makes ALL
 * writes strictly serialized (the runner is the global lock / sequencer that
 * prevents WebKit cross-transaction hangs).
 *
 * The visibilitychange -> hidden path (fired on every tab switch) does not
 * have to be synchronous, so with a runner configured the write it performs
 * for buffered edits must run inside the runner, not around it. Previously
 * the hide handler called `db.transaction(...)` directly: one readwrite
 * transaction opened outside the lock, zero runner calls.
 *
 * Time is controlled by a debounce far longer than the test, so the hide
 * event is the only thing that can write the buffered edit; async ordering
 * is controlled by awaiting every runner call and every transaction the
 * binding opened.
 *
 * @param {t.TestCase} tc
 */
export const testVisibilityHiddenFlushIsSerializedByTransactionRunner = async tc => {
  await clearDocument(tc.testName)

  // A strict sequencer, the kind of global lock the README describes.
  let inRunner = false
  let runnerTail = Promise.resolve()
  /** @type {Array<Promise<any>>} */
  const runnerCalls = []
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    const run = runnerTail.then(async () => {
      inRunner = true
      try {
        return await work()
      } finally {
        inRunner = false
      }
    })
    runnerTail = run.then(() => {}, () => {})
    runnerCalls.push(run)
    return run
  }

  // Observe every readwrite transaction opened on this database: was it
  // opened while the runner held the lock, and when does it finish?
  /** @type {Array<{ insideRunner: boolean, done: Promise<void> }>} */
  const writeTxs = []
  const realTransaction = IDBDatabase.prototype.transaction
  // @ts-ignore - override the prototype to observe every transaction
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    if (this.name === tc.testName && tx.mode === 'readwrite') {
      /** @type {Promise<void>} */
      const done = new Promise(resolve => {
        tx.addEventListener('complete', () => resolve())
        tx.addEventListener('error', () => resolve())
        tx.addEventListener('abort', () => resolve())
      })
      writeTxs.push({ insideRunner: inRunner, done })
    }
    return tx
  }

  /**
   * Wait until every runner call and every observed transaction has settled
   * and no new ones are started (including work deferred by a macrotask).
   */
  const settle = async () => {
    for (;;) {
      const nRunner = runnerCalls.length
      const nTx = writeTxs.length
      await Promise.all(runnerCalls.map(p => p.then(() => {}, () => {})).concat(writeTxs.map(w => w.done)))
      await new Promise(resolve => setTimeout(resolve, 0))
      if (runnerCalls.length === nRunner && writeTxs.length === nTx) return
    }
  }

  try {
    await withCapturedLifecycle(async (fire, setVisibility) => {
      const doc = new Y.Doc()
      const persistence = new IndexeddbPersistence(tc.testName, doc, {
        transactionRunner: runner,
        // Far longer than the test: only the hide event can write the edit.
        writeDebounceMs: 60_000
      })
      await persistence.whenSynced
      await settle()
      const txsBeforeHide = writeTxs.length
      const runnerCallsBeforeHide = runnerCalls.length

      // A buffered edit, then the user switches tabs.
      doc.getText('t').insert(0, 'buffered edit')
      setVisibility('hidden')
      fire('visibilitychange')
      await settle()

      const hideTxs = writeTxs.slice(txsBeforeHide)
      const outsideRunner = hideTxs.filter(w => !w.insideRunner).length
      t.assert(
        outsideRunner === 0,
        `visibilitychange(hidden) flush must run inside transactionRunner: ${outsideRunner} of ${hideTxs.length} readwrite transaction(s) opened outside the runner`
      )
      // Nothing else held the runner at hide time (settled above), so
      // "inside" must mean the hide write itself went through it.
      t.assert(runnerCalls.length > runnerCallsBeforeHide, 'the hide write must call transactionRunner')

      // The hide write must still persist the buffered edit eagerly (well
      // before the debounce timer would fire).
      const persisted = await readSnapshot(tc.testName)
      t.assert(persisted !== null, 'hide flush must persist the buffered edit')
      const fresh = new Y.Doc()
      Y.applyUpdate(fresh, /** @type {Uint8Array} */ (persisted))
      t.compareStrings(fresh.getText('t').toString(), 'buffered edit')
      fresh.destroy()

      await persistence.destroy()
    })
  } finally {
    IDBDatabase.prototype.transaction = realTransaction
  }
}

/**
 * Browsers fire visibilitychange(hidden) right before pagehide when a tab
 * closes. If another provider holds the runner (a global lock) at that
 * moment, the hide write has to wait, and the page may be gone before the
 * lock frees up. The buffered edits must therefore stay buffered until
 * that write actually starts, so the synchronous pagehide write that
 * follows still persists them without waiting for the runner.
 *
 * @param {t.TestCase} tc
 */
export const testPagehideWritesWhileHiddenFlushWaitsForBusyRunner = async tc => {
  await clearDocument(tc.testName)

  let runnerTail = Promise.resolve()
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    const run = runnerTail.then(() => work())
    runnerTail = run.then(() => {}, () => {})
    return run
  }

  await withCapturedLifecycle(async (fire, setVisibility) => {
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc, {
      transactionRunner: runner,
      // Far longer than the test: only the lifecycle events can write.
      writeDebounceMs: 60_000
    })
    await persistence.whenSynced

    // Some other work holds the lock while the tab closes.
    let release = () => {}
    const held = runner(() => new Promise(resolve => {
      release = () => resolve(undefined)
    }))
    try {
      doc.getText('t').insert(0, 'last edit')
      setVisibility('hidden')
      fire('visibilitychange')
      fire('pagehide')

      // Still holding the lock: the edit must already be persisted.
      const persisted = await readSnapshot(tc.testName)
      t.assert(persisted !== null, 'pagehide must persist the edit while the runner is busy')
      const fresh = new Y.Doc()
      Y.applyUpdate(fresh, /** @type {Uint8Array} */ (persisted))
      t.compareStrings(fresh.getText('t').toString(), 'last edit')
      fresh.destroy()
    } finally {
      release()
      await held
    }
    await persistence.destroy()
  })
}

/**
 * A transactionRunner that rejects the hide write must not lose the
 * buffered edits: they stay buffered for a later flush and the failure is
 * surfaced via 'error' instead of an unhandled rejection.
 *
 * @param {t.TestCase} tc
 */
export const testVisibilityHiddenRunnerFailureKeepsBuffer = async tc => {
  await clearDocument(tc.testName)
  let rejectNext = false
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = work => {
    if (rejectNext) {
      rejectNext = false
      return Promise.reject(new Error('runner rejected'))
    }
    return work()
  }

  await withCapturedLifecycle(async (fire, setVisibility) => {
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc, {
      transactionRunner: runner,
      writeDebounceMs: 60_000
    })
    await persistence.whenSynced
    /** @type {Array<any>} */
    const errors = []
    persistence.on('error', (/** @type {any} */ err) => { errors.push(err) })

    doc.getText('t').insert(0, 'kept')
    const buffered = persistence._pendingUpdates.length
    rejectNext = true
    setVisibility('hidden')
    fire('visibilitychange')
    await new Promise(resolve => setTimeout(resolve, 0))

    t.assert(errors.length === 1 && errors[0].message === 'runner rejected', 'runner failure must be emitted as error')
    t.assert(persistence._pendingUpdates.length === buffered, 'updates must stay buffered')

    await persistence.destroy()
    const persisted = await readSnapshot(tc.testName)
    const fresh = new Y.Doc()
    Y.applyUpdate(fresh, /** @type {Uint8Array} */ (persisted))
    t.compareStrings(fresh.getText('t').toString(), 'kept')
    fresh.destroy()
  })
}
