/* eslint-env browser */

/**
 * Regression tests: a trim whose transaction aborts at commit must be
 * reported, like every other failed write.
 *
 * README contract: "The 'error' event is fired when a database transaction
 * or operation fails (e.g. QuotaExceededError, aborted transaction)." The
 * realistic way a trim fails is exactly that: the origin is near its quota,
 * so the transaction holding the consolidated row aborts at commit with a
 * QuotaExceededError after every one of its requests succeeded.
 *
 * The storeState() chain resolves in the success callback of its last
 * request, before the commit, and the trim's 'abort' listener only counts
 * the failure for the backoff. So an automatic trim that aborts at commit
 * emits no 'error' at all, and an explicit `await storeState(provider, true)`
 * resolves as if the store had been consolidated, while nothing was. Apps
 * watching 'error' for quota problems never learn that compaction keeps
 * failing. (Flush, hydration, page-hide and destroy writes all report their
 * commit-time aborts.)
 */

import * as Y from 'yjs'
import * as t from 'lib0/testing.js'
import { IndexeddbPersistence, clearDocument, storeState, PREFERRED_TRIM_SIZE } from '../src/y-idb.js'

const tick = () => new Promise(resolve => setImmediate(resolve))

/**
 * Lets promise chains and IndexedDB events started by the abort settle.
 */
const settle = async () => {
  for (let i = 0; i < 5; i++) await tick()
}

/**
 * Resolves with `p`'s value, or with 'timeout' after `ms` — only a guard so
 * a missing abort fails the test instead of hanging the runner.
 *
 * @template T
 * @param {Promise<T>} p
 * @param {number} ms
 * @return {Promise<T|'timeout'>}
 */
const withDeadline = (p, ms) => {
  /** @type {any} */
  let id = null
  /** @type {Promise<'timeout'>} */
  const deadline = new Promise(resolve => { id = setTimeout(() => resolve('timeout'), ms) })
  return Promise.race([p, deadline]).finally(() => clearTimeout(id))
}

/**
 * Make the NEXT trim transaction (the readwrite transaction over both the
 * 'updates' and 'custom' stores) fail at commit time, the way a browser
 * aborts a transaction whose writes exceed the origin's quota: every request
 * in it succeeds, then the transaction aborts with a QuotaExceededError
 * instead of firing 'complete'. Implemented on top of fake-indexeddb's
 * transaction scheduler (`_start` runs once per queued request and finally
 * commits).
 *
 * @return {{ aborted: Promise<void>, restore: () => void }}
 */
const failNextTrimAtCommit = () => {
  const realTransaction = IDBDatabase.prototype.transaction
  let armed = true
  /** @type {() => void} */
  let markAborted = () => {}
  /** @type {Promise<void>} */
  const aborted = new Promise(resolve => { markAborted = resolve })
  // @ts-ignore - override the prototype to inject a commit-time failure
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    const names = typeof storeNames === 'string' ? [storeNames] : Array.from(storeNames)
    if (armed && mode === 'readwrite' && names.includes('updates') && names.includes('custom')) {
      armed = false
      tx.addEventListener('abort', () => markAborted())
      const fakeTx = /** @type {any} */ (tx)
      const realStart = fakeTx._start
      fakeTx._start = function () {
        const allRequestsDone = this._requests.every(/** @param {any} r */ r => r.request.readyState === 'done')
        if (allRequestsDone && this._state !== 'finished') {
          // Commit point reached: abort instead of committing.
          this._abort('QuotaExceededError')
          return
        }
        return realStart.call(this)
      }
    }
    return tx
  }
  return {
    aborted,
    restore: () => { IDBDatabase.prototype.transaction = realTransaction }
  }
}

/**
 * An explicit `storeState(provider, true)` whose transaction aborts at
 * commit must not report success silently: it must reject, or at least
 * surface the failure via 'error'.
 *
 * @param {t.TestCase} tc
 */
export const testExplicitStoreStateAbortedAtCommitIsReported = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const provider = new IndexeddbPersistence(tc.testName, doc)
  const fault = failNextTrimAtCommit()
  try {
    await provider.whenSynced
    for (let i = 0; i < 3; i++) {
      doc.getMap('m').set('k' + i, i)
      await provider.flush()
    }
    /** @type {Array<any>} */
    const errors = []
    provider.on('error', (/** @type {any} */ err) => { errors.push(err) })

    /** @type {{ rejected: boolean, reason: any }} */
    const outcome = await storeState(provider, true).then(
      () => ({ rejected: false, reason: null }),
      reason => ({ rejected: true, reason })
    )
    t.assert(await withDeadline(fault.aborted, 5000) !== 'timeout', 'precondition: the consolidation transaction aborted at commit')
    await settle()

    t.assert(
      outcome.rejected || errors.length > 0,
      "storeState(provider, true) aborted at commit (QuotaExceededError) and nothing was consolidated, yet it resolved and no 'error' was emitted"
    )
    const reported = outcome.rejected ? outcome.reason : errors[0]
    t.assert(
      reported instanceof Error && reported.name === 'QuotaExceededError',
      `the reported failure must carry the transaction's QuotaExceededError, got: ${String(reported)}`
    )
  } finally {
    fault.restore()
    await provider.destroy()
  }
}

/**
 * The automatic trim a flush arms once the store holds PREFERRED_TRIM_SIZE
 * rows: when its transaction aborts at commit (a full consolidation whose
 * row exceeds the quota), exactly one 'error' carrying the
 * QuotaExceededError must be emitted for the failed attempt.
 *
 * The trim timer is captured while the flush that arms it runs and fired by
 * hand, so no real time passes.
 *
 * @param {t.TestCase} tc
 */
export const testAutomaticTrimAbortedAtCommitEmitsError = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const provider = new IndexeddbPersistence(tc.testName, doc)
  /** @type {{ aborted: Promise<void>, restore: () => void } | null} */
  let fault = null
  try {
    await provider.whenSynced

    // One flush of PREFERRED_TRIM_SIZE + 20 rows arms the trim timer.
    const realSetTimeout = globalThis.setTimeout
    const realClearTimeout = globalThis.clearTimeout
    /** @type {Map<object, { fn: function(...any):void, args: Array<any> }>} */
    const captured = new Map()
    /**
     * @param {function(...any):void} fn
     * @param {number} [_ms]
     * @param {...any} args
     */
    const captureTimeout = (fn, _ms, ...args) => {
      const id = { capturedTimer: captured.size + 1 }
      captured.set(id, { fn, args })
      return id
    }
    /**
     * @param {any} id
     */
    const releaseTimeout = id => { captured.delete(id) }
    globalThis.setTimeout = /** @type {any} */ (captureTimeout)
    globalThis.clearTimeout = /** @type {any} */ (releaseTimeout)
    try {
      for (let i = 0; i < PREFERRED_TRIM_SIZE + 20; i++) {
        doc.getMap('m').set('k' + i, i)
      }
      await provider.flush()
    } finally {
      globalThis.setTimeout = realSetTimeout
      globalThis.clearTimeout = realClearTimeout
    }
    t.assert(captured.size === 1, `precondition: the flush that crossed PREFERRED_TRIM_SIZE armed one trim timer (got ${captured.size})`)

    /** @type {Array<any>} */
    const errors = []
    provider.on('error', (/** @type {any} */ err) => { errors.push(err) })
    fault = failNextTrimAtCommit()
    captured.forEach(({ fn, args }) => fn(...args))
    captured.clear()
    t.assert(await withDeadline(fault.aborted, 5000) !== 'timeout', 'precondition: the trim transaction aborted at commit')
    await settle()

    t.assert(
      errors.length === 1,
      `a trim aborted at commit (QuotaExceededError) must emit exactly one 'error', got ${errors.length}`
    )
    t.assert(
      errors[0] instanceof Error && errors[0].name === 'QuotaExceededError',
      `the 'error' must carry the transaction's QuotaExceededError, got: ${String(errors[0])}`
    )
    // The abort and the rejection report the same attempt: it backs off once.
    t.assert(provider._trimFailures === 1, `the failed trim must count once towards the backoff, got ${provider._trimFailures}`)
  } finally {
    if (fault) fault.restore()
    await provider.destroy()
  }
}
