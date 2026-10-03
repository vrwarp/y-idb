/* eslint-env browser */

/**
 * Perf regression: work done by the automatic flush retries while every
 * flush transaction keeps failing (a QuotaExceededError on every commit, a
 * WebKit connection that stays broken until reload, a runner that keeps
 * rejecting) and the app keeps editing, e.g. TTS saving progress every few
 * seconds.
 *
 * Once `maxRetries` attempts had failed, `_onFlushFailed` reset the retry
 * counter and armed no timer, so the next update flushed at once and started
 * another full cycle (attempts at 0, 0.2, 0.6, 1.4, 3.0, 6.2 s): about 40
 * attempts, 40 'error' and 6.7 'retry-exhausted' events per simulated
 * minute, forever. Each attempt add()s the whole, growing backlog (one row
 * per buffered update), so row adds grew quadratically with the length of
 * the failure with a large constant (x16 for x4 the time). The retries now
 * stay on a capped backoff until a write commits.
 *
 * `setTimeout` is replaced by a virtual clock for the duration of each
 * simulation; fake-indexeddb runs on setImmediate and is let settle (no
 * transaction open) before every clock step, so the counts are exact.
 */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, readSnapshot, MAX_RETRY_BACKOFF_MS } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/** Simulated time between edits (TTS progress cadence). */
const EDIT_MS = 3000

/**
 * @typedef {Object} Simulation
 * @property {() => number} now current simulated time (ms)
 * @property {(failing: boolean) => void} setFailing whether readwrite transactions on `updates` abort
 * @property {{ attempts: number, rowAdds: number }} counts readwrite transactions on `updates` and add() calls while failing
 * @property {(ms: number) => Promise<void>} advanceBy run simulated time forward, firing due timers in order
 * @property {<T>(p: Promise<T>) => Promise<T>} driveUntil run simulated time forward until `p` settles
 */

/**
 * Runs `fn` with `setTimeout` on a virtual clock and the IndexedDB
 * prototypes instrumented: while failing, every readwrite transaction on
 * the `updates` store is aborted right after the provider queued its add()
 * requests (a commit-time QuotaExceededError). The real timers and
 * prototypes are restored afterwards.
 *
 * @template T
 * @param {(sim: Simulation) => Promise<T>} fn
 * @return {Promise<T>}
 */
const withSimulation = async fn => {
  const g = /** @type {any} */ (globalThis)
  const realSetTimeout = g.setTimeout
  const realClearTimeout = g.clearTimeout
  const realTransaction = IDBDatabase.prototype.transaction
  const realAdd = IDBObjectStore.prototype.add

  /** @type {Map<number, { at: number, fn: Function, args: Array<any> }>} */
  const timers = new Map()
  let now = 0
  let seq = 0
  let openTx = 0
  let failing = false
  const counts = { attempts: 0, rowAdds: 0 }

  g.setTimeout = (/** @type {Function} */ fn, /** @type {number} */ ms = 0, /** @type {Array<any>} */ ...args) => {
    const id = ++seq
    timers.set(id, { at: now + Math.max(0, Number(ms) || 0), fn, args })
    return id
  }
  g.clearTimeout = (/** @type {number} */ id) => { timers.delete(id) }
  // @ts-ignore
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    openTx++
    const done = () => { openTx-- }
    tx.addEventListener('complete', done)
    tx.addEventListener('abort', done)
    const names = typeof storeNames === 'string' ? [storeNames] : Array.from(storeNames)
    if (failing && mode === 'readwrite' && names.includes('updates')) {
      counts.attempts++
      // Fails at commit, after the flush queued its add() requests.
      queueMicrotask(() => {
        try { tx.abort() } catch (e) { /* already finished */ }
      })
    }
    return tx
  }
  // @ts-ignore
  IDBObjectStore.prototype.add = function (value, key) {
    if (failing) counts.rowAdds++
    return realAdd.call(this, value, key)
  }

  const tick = () => new Promise(resolve => setImmediate(resolve))
  const settle = async () => {
    let idle = 0
    while (idle < 3) {
      await tick()
      idle = openTx === 0 ? idle + 1 : 0
    }
  }
  /**
   * @param {number} until
   * @return {boolean} whether a timer fired
   */
  const fireNextTimer = until => {
    let nextId = -1
    /** @type {{ at: number, fn: Function, args: Array<any> } | null} */
    let next = null
    for (const [id, timer] of timers) {
      if (timer.at <= until && (next === null || timer.at < next.at)) {
        nextId = id
        next = timer
      }
    }
    if (next === null) return false
    timers.delete(nextId)
    now = Math.max(now, next.at)
    next.fn(...next.args)
    return true
  }
  /** @type {Simulation} */
  const sim = {
    now: () => now,
    setFailing: f => { failing = f },
    counts,
    advanceBy: async ms => {
      const until = now + ms
      for (;;) {
        await settle()
        if (!fireNextTimer(until)) break
      }
      now = until
    },
    driveUntil: async p => {
      const state = { done: false }
      p.then(() => { state.done = true }, () => { state.done = true })
      while (!state.done) {
        await settle()
        if (state.done) break
        if (!fireNextTimer(Infinity)) await tick()
      }
      return p
    }
  }
  try {
    return await fn(sim)
  } finally {
    g.setTimeout = realSetTimeout
    g.clearTimeout = realClearTimeout
    IDBDatabase.prototype.transaction = realTransaction
    IDBObjectStore.prototype.add = realAdd
  }
}

/**
 * Edits the `progress` map the way TTS saves reading progress.
 *
 * @param {Y.Doc} doc
 * @param {number} i
 */
const saveProgress = (doc, i) => {
  const progress = doc.getMap('progress')
  doc.transact(() => {
    progress.set('currentCfi', `epubcfi(/6/${i}!/4/2/1:0)`)
    progress.set('lastRead', 1700000000000 + i * EDIT_MS)
  })
}

/**
 * Reloads database `name` and checks it holds the doc's progress.
 *
 * @param {Simulation} sim
 * @param {string} name
 * @param {Y.Doc} doc
 */
const assertPersisted = async (sim, name, doc) => {
  const persisted = await sim.driveUntil(readSnapshot(name))
  t.assert(persisted !== null)
  const reloaded = new Y.Doc()
  Y.applyUpdate(reloaded, /** @type {Uint8Array} */ (persisted))
  t.compare(reloaded.getMap('progress').toJSON(), doc.getMap('progress').toJSON())
}

/**
 * @typedef {Object} FailureRun
 * @property {number} attempts flush transactions opened while failing
 * @property {number} attemptsAfterExhaustion of those, after the first 'retry-exhausted'
 * @property {number} msAfterExhaustion simulated time from the first 'retry-exhausted' to the end
 * @property {number} rowAdds IDBObjectStore.add() calls while failing
 * @property {number} errors 'error' events while failing
 * @property {number} exhausted 'retry-exhausted' events while failing
 * @property {number} backlog updates buffered at the end of the window
 */

/**
 * Edit once every EDIT_MS for `minutes` of simulated time while every flush
 * transaction aborts right after its add() requests were queued, then let
 * writes recover and check that nothing was lost.
 *
 * @param {string} name
 * @param {number} minutes
 * @return {Promise<FailureRun>}
 */
const simulateFailingWrites = async (name, minutes) => {
  await clearDocument(name)
  return withSimulation(async sim => {
    const doc = new Y.Doc()
    const provider = new IndexeddbPersistence(name, doc)
    await sim.driveUntil(provider.whenSynced)
    let errors = 0
    let exhausted = 0
    let exhaustedAt = -1
    let attemptsAtExhaustion = 0
    provider.on('error', () => { errors++ })
    provider.on('retry-exhausted', () => {
      exhausted++
      if (exhaustedAt < 0) {
        exhaustedAt = sim.now()
        attemptsAtExhaustion = sim.counts.attempts
      }
    })

    sim.setFailing(true)
    const edits = Math.round(minutes * 60000 / EDIT_MS)
    for (let i = 0; i < edits; i++) {
      saveProgress(doc, i)
      await sim.advanceBy(EDIT_MS)
    }
    sim.setFailing(false)
    t.assert(exhaustedAt >= 0, 'retries were exhausted during the failure window')
    /** @type {FailureRun} */
    const run = {
      attempts: sim.counts.attempts,
      attemptsAfterExhaustion: sim.counts.attempts - attemptsAtExhaustion,
      msAfterExhaustion: sim.now() - exhaustedAt,
      rowAdds: sim.counts.rowAdds,
      errors,
      exhausted,
      backlog: provider._pendingUpdates.length
    }

    // Writes work again: everything buffered lands, nothing is lost.
    await sim.driveUntil(provider.flush())
    await sim.driveUntil(provider.destroy())
    await assertPersisted(sim, name, doc)
    await sim.driveUntil(clearDocument(name))
    return run
  })
}

/**
 * Attempts allowed per simulated window after retries were exhausted: at
 * most one per 10 simulated seconds on average.
 *
 * @param {FailureRun} run
 */
const attemptBound = run => Math.ceil(run.msAfterExhaustion / 10000) + 1

/**
 * Once retries are exhausted, a new update must not start a fresh retry
 * cycle on the spot. The bound — at most one attempt per 10 simulated
 * seconds on average after the first exhaustion — sits well between the old
 * behaviour (one every ~1.5 s: a 6-attempt cycle every 9 s at this edit
 * cadence) and staying on a capped 30-60 s backoff until a write succeeds
 * (1-2 per minute). A post-exhaustion timer that is followed by another
 * full 6-attempt cycle still averages one attempt every ~6 s, and fails it.
 * 'retry-exhausted' is emitted once for the whole failure, not once per
 * cycle.
 *
 * @param {t.TestCase} tc
 */
export const testExhaustedRetriesDoNotRestartOnEveryUpdate = async tc => {
  const run = await simulateFailingWrites(tc.testName, 8)
  const bound = attemptBound(run)
  t.info(`attempts=${run.attempts} afterExhaustion=${run.attemptsAfterExhaustion} in ${run.msAfterExhaustion / 1000}s errors=${run.errors} exhausted=${run.exhausted} backlog=${run.backlog}`)
  t.assert(
    run.attemptsAfterExhaustion <= bound,
    `${run.attemptsAfterExhaustion} flush attempts (and as many 'error' events) in the ${run.msAfterExhaustion / 1000} simulated seconds after retries were exhausted; expected at most ${bound}`
  )
  t.assert(run.exhausted === 1, `'retry-exhausted' must be emitted once per failure episode (got ${run.exhausted})`)
}

/**
 * Row adds while writes keep failing must stay within what the bounded
 * attempt rate allows: each attempt offers the backlog at most once, so over
 * the 8-minute window row adds are at most (the attempts before exhaustion
 * plus the bound of the test above) x the final backlog. The old behaviour
 * — a 6-attempt cycle every 9 s, each attempt re-adding the whole backlog —
 * needs about 3x that.
 *
 * (Row adds still grow quadratically with the length of the failure under
 * any fixed retry cadence, since every attempt must offer the whole, growing
 * backlog; writing it as one merged row instead would cost a decode of the
 * whole backlog per failure, more CPU than the structured clones it saves.)
 *
 * @param {t.TestCase} tc
 */
export const testFailedBacklogRowAddsStayBounded = async tc => {
  const run = await simulateFailingWrites(tc.testName, 8)
  const attemptsAtExhaustion = run.attempts - run.attemptsAfterExhaustion
  const bound = (attemptsAtExhaustion + attemptBound(run)) * run.backlog
  t.info(`rowAdds=${run.rowAdds} (attempts=${run.attempts}, backlog=${run.backlog}), bound=${bound}`)
  t.assert(
    run.rowAdds <= bound,
    `${run.rowAdds} row adds in the 8-minute failure window (${run.attempts} attempts, backlog ${run.backlog}); expected at most ${bound}`
  )
}

/**
 * A successful page-hide write proves the database works again: it must end
 * the failure episode like a flush commit does. Otherwise the capped
 * backoff keeps deferring the flush of every later edit for up to
 * MAX_RETRY_BACKOFF_MS, leaving them in memory only if the tab is then
 * killed without another pagehide.
 *
 * @param {t.TestCase} tc
 */
export const testPageHideWriteEndsSlowRetry = async tc => {
  await clearDocument(tc.testName)
  await withSimulation(async sim => {
    const doc = new Y.Doc()
    const provider = new IndexeddbPersistence(tc.testName, doc)
    await sim.driveUntil(provider.whenSynced)
    sim.setFailing(true)
    let i = 0
    for (; i < 40; i++) {
      saveProgress(doc, i)
      await sim.advanceBy(EDIT_MS)
    }
    t.assert(provider._retryCount > provider._maxRetries && provider._retryTimeoutId !== null, 'retries are exhausted and a slow retry is armed')

    sim.setFailing(false)
    // The page is hidden: the page-hide write takes the backlog. An edit
    // arrives while it is in flight.
    const write = provider._unloadListener()
    saveProgress(doc, i++)
    await sim.driveUntil(write)
    t.assert(provider._retryCount === 0, 'a committed page-hide write resets the retry count')
    // No simulated time passes from here on.
    await sim.advanceBy(0)
    t.assert(provider._pendingUpdates.length === 0 && !provider._writing, 'the edit made during the page-hide write must be flushed at once, not after the slow retry')
    saveProgress(doc, i++)
    await sim.advanceBy(0)
    t.assert(provider._pendingUpdates.length === 0 && !provider._writing, 'later edits must be flushed at once again')
    t.assert(provider._retryTimeoutId === null, 'no retry stays armed')
    await sim.driveUntil(provider.destroy())
    await assertPersisted(sim, tc.testName, doc)
    await sim.driveUntil(clearDocument(tc.testName))
  })
}

/**
 * An explicit flush() must not wait out the slow retry after exhaustion
 * (up to MAX_RETRY_BACKOFF_MS): it attempts at once and then on its own
 * short backoff (capped at 3.2 s), without hot-spinning, so a deadline
 * raced against it still fires, and it resolves soon after the database
 * recovers.
 *
 * @param {t.TestCase} tc
 */
export const testFlushDoesNotWaitOutSlowRetry = async tc => {
  await clearDocument(tc.testName)
  await withSimulation(async sim => {
    const doc = new Y.Doc()
    const provider = new IndexeddbPersistence(tc.testName, doc)
    await sim.driveUntil(provider.whenSynced)
    sim.setFailing(true)
    let i = 0
    for (; i < 40; i++) {
      saveProgress(doc, i)
      await sim.advanceBy(EDIT_MS)
    }
    t.assert(provider._retryCount > provider._maxRetries && provider._retryTimeoutId !== null, 'retries are exhausted and a slow retry is armed')

    // Still failing: flush() retries on its own backoff, and a deadline
    // raced against it fires.
    const attemptsBefore = sim.counts.attempts
    const flushed = provider.flush()
    const deadline = 20000
    const outcome = await sim.driveUntil(Promise.race([
      flushed.then(() => 'flushed'),
      new Promise(resolve => setTimeout(() => resolve('deadline'), deadline))
    ]))
    const attempts = sim.counts.attempts - attemptsBefore
    t.info(`flush() attempts in ${deadline / 1000} simulated seconds of failure: ${attempts}`)
    t.assert(outcome === 'deadline', 'flush() cannot resolve while writes fail')
    t.assert(attempts >= deadline / 3200, `flush() made ${attempts} attempts in ${deadline / 1000} s: it must not wait out the ${MAX_RETRY_BACKOFF_MS / 1000} s slow retry`)
    t.assert(attempts <= deadline / 1000, `flush() made ${attempts} attempts in ${deadline / 1000} s: it must back off between them`)

    // Writes recover: flush() resolves within its own backoff.
    sim.setFailing(false)
    const recoveredAt = sim.now()
    await sim.driveUntil(flushed)
    t.assert(sim.now() - recoveredAt <= 3200, `flush() resolved ${sim.now() - recoveredAt} ms after writes recovered`)
    t.assert(provider._pendingUpdates.length === 0 && provider._retryCount === 0 && provider._retryTimeoutId === null)
    await sim.driveUntil(provider.destroy())
    await assertPersisted(sim, tc.testName, doc)
    await sim.driveUntil(clearDocument(tc.testName))
  })
}
