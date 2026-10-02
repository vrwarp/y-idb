/* eslint-env browser */
/**
 * y-idb flush-failure benchmark: write work while IndexedDB keeps failing
 *
 * A versicle session keeps editing (TTS reading saves progress every few
 * seconds) while every flush transaction fails — a QuotaExceededError on
 * every commit, a WebKit IndexedDB connection that stays broken until
 * reload, or a transactionRunner that keeps rejecting. Failed batches are
 * kept in memory and retried; this measures how much work those retries
 * do as the failure window grows, then lets writes recover and checks that
 * the reloaded database matches the live doc.
 *
 * Time is simulated: `setTimeout` is replaced by a virtual clock that the
 * driver advances (one versicle event every FLUSH_FAIL_EDIT_MS), so a
 * 30-minute failure window runs in seconds and every count below is
 * deterministic. fake-indexeddb schedules its own work with setImmediate,
 * which keeps running for real; the driver lets it settle (no transaction
 * open) before each clock step.
 *
 * Failure modes (FLUSH_FAIL_MODE):
 *  - abort  (default): every readwrite transaction on the updates store is
 *    aborted right after the provider queued its add() requests — the
 *    stand-in for a commit-time QuotaExceededError. add() calls happen, so
 *    the per-row structured clones are counted.
 *  - runner: the transactionRunner rejects without running the work, so no
 *    transaction is opened at all; only attempts and 'error' events count.
 *
 * Reported per failure window (counters, not wall time):
 *  - backlog:      updates buffered in memory at the end of the window
 *  - attempts:     flush attempts (readwrite transactions on `updates` in
 *                  abort mode, runner calls in runner mode)
 *  - rowAdds:      IDBObjectStore.add() calls (each one structured-clones
 *                  its row)
 *  - offered:      bytes passed to add()
 *  - errors / exhausted: 'error' and 'retry-exhausted' events emitted
 *  - recovery:     rows the first successful flush writes once the database
 *                  works again, and whether a reload matches the live doc
 *
 * Run with: node benchmarks/flush-failure.mjs
 *   FLUSH_FAIL_MINUTES=7.5,15,30   simulated failure windows (minutes)
 *   FLUSH_FAIL_EDIT_MS=3000        simulated time between versicle events
 *   FLUSH_FAIL_PRE_SESSIONS=24     normal sessions aged in before the failure
 *   FLUSH_FAIL_MODE=abort|runner
 */
import 'fake-indexeddb/auto'
import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, readSnapshot } from '../src/y-idb.js'
import { createSim, runSession, clientIdForSession } from './versicle-workload.mjs'

const SEED = 20260820
const MINUTES = (process.env.FLUSH_FAIL_MINUTES || '7.5,15,30').split(',').map(Number)
const EDIT_MS = Number(process.env.FLUSH_FAIL_EDIT_MS || 3000)
const PRE_SESSIONS = Number(process.env.FLUSH_FAIL_PRE_SESSIONS || 24)
const MODE = process.env.FLUSH_FAIL_MODE || 'abort'
const DB = 'versicle-flush-failure-bench'

const fmtBytes = (n) => n >= 1048576 ? (n / 1048576).toFixed(2) + ' MB' : n >= 1024 ? (n / 1024).toFixed(1) + ' KB' : n + ' B'
const fmtInt = (n) => n.toLocaleString('en-US')

// --- Virtual clock ----------------------------------------------------------
// Installed before any provider exists: the provider's backoff, debounce and
// trim timers, and flush()'s own waits, all run on simulated time.
const realSetImmediate = globalThis.setImmediate
const clock = { now: 0, seq: 0, timers: new Map() }
globalThis.setTimeout = (fn, ms = 0, ...args) => {
  const id = ++clock.seq
  clock.timers.set(id, { at: clock.now + Math.max(0, Number(ms) || 0), fn, args })
  return id
}
globalThis.clearTimeout = (id) => { clock.timers.delete(id) }

// --- IndexedDB instrumentation and fault injection --------------------------
const counters = { attempts: 0, rowAdds: 0, offered: 0 }
let openTx = 0
let failing = false

const origTransaction = IDBDatabase.prototype.transaction
IDBDatabase.prototype.transaction = function (names, mode, options) {
  const tx = origTransaction.call(this, names, mode, options)
  openTx++
  const done = () => { openTx-- }
  tx.addEventListener('complete', done)
  tx.addEventListener('abort', done)
  const storeNames = typeof names === 'string' ? [names] : Array.from(names)
  if (mode === 'readwrite' && storeNames.includes('updates')) {
    if (MODE === 'abort') counters.attempts++
    if (failing && MODE === 'abort') {
      // After the provider queued its add() requests synchronously.
      queueMicrotask(() => {
        try { tx.abort() } catch (e) { /* already finished */ }
      })
    }
  }
  return tx
}
const origAdd = IDBObjectStore.prototype.add
IDBObjectStore.prototype.add = function (value, key) {
  counters.rowAdds++
  counters.offered += (value && value.byteLength) || 0
  return origAdd.call(this, value, key)
}
const transactionRunner = MODE === 'runner'
  ? async (work) => {
    counters.attempts++
    if (failing) throw new Error('transactionRunner down')
    return work()
  }
  : undefined

// --- Driver -----------------------------------------------------------------
const tick = () => new Promise(resolve => realSetImmediate(resolve))

/** Let real (setImmediate-scheduled) IndexedDB work finish. */
const settle = async () => {
  let idle = 0
  while (idle < 3) {
    await tick()
    idle = openTx === 0 ? idle + 1 : 0
  }
}

/** Fires the earliest virtual timer due at or before `until`; false if none. */
const fireNextTimer = (until) => {
  let nextId = null
  let next = null
  for (const [id, timer] of clock.timers) {
    if (timer.at <= until && (next === null || timer.at < next.at)) {
      nextId = id
      next = timer
    }
  }
  if (next === null) return false
  clock.timers.delete(nextId)
  clock.now = Math.max(clock.now, next.at)
  next.fn(...next.args)
  return true
}

/** Advance simulated time by `ms`, firing due timers in order. */
const advanceBy = async (ms) => {
  const until = clock.now + ms
  for (;;) {
    await settle()
    if (!fireNextTimer(until)) break
  }
  clock.now = until
}

/** Run simulated time forward until `p` settles; resolves to its value. */
const driveUntil = async (p) => {
  const state = { done: false }
  p.then(() => { state.done = true }, () => { state.done = true })
  while (!state.done) {
    await settle()
    if (state.done) break
    if (!fireNextTimer(Infinity)) await tick()
  }
  return p
}

/** Key-order-independent JSON (Y.Map iteration order depends on integration order). */
const canonical = (v) => Array.isArray(v)
  ? v.map(canonical)
  : v !== null && typeof v === 'object'
    ? Object.fromEntries(Object.keys(v).sort().map(k => [k, canonical(v[k])]))
    : v

const docJson = (doc) => {
  const out = {}
  for (const key of ['library', 'progress', 'annotations', 'reading-list', 'vocabulary', 'lexicon', 'contentAnalysis', 'devices', 'searchHistory', 'meta']) {
    out[key] = doc.getMap(key).toJSON()
  }
  return JSON.stringify(canonical(out))
}

const sum = (arr, sel) => arr.reduce((a, b) => a + sel(b), 0)

/**
 * Ages the database with PRE_SESSIONS normal sessions, then runs one long
 * session of `minutes` simulated minutes during which every flush fails.
 */
const runScenario = async (minutes) => {
  failing = false
  await driveUntil(clearDocument(DB))
  const sim = createSim({ seed: SEED })
  let session = 0
  for (; session < PRE_SESSIONS; session++) {
    const doc = new Y.Doc()
    doc.clientID = clientIdForSession(SEED, session)
    const provider = new IndexeddbPersistence(DB, doc, { transactionRunner })
    await driveUntil(provider.whenSynced)
    await runSession(sim, doc, () => advanceBy(EDIT_MS))
    // Let the provider's own trim timer fire.
    await advanceBy(2 * provider._storeTimeout)
    await driveUntil(provider.destroy())
    doc.destroy()
  }

  // --- The failure session ---
  const doc = new Y.Doc()
  doc.clientID = clientIdForSession(SEED, session)
  const provider = new IndexeddbPersistence(DB, doc, { transactionRunner })
  await driveUntil(provider.whenSynced)
  let errors = 0
  let exhausted = 0
  provider.on('error', () => { errors++ })
  provider.on('retry-exhausted', () => { exhausted++ })

  const events = Math.round(minutes * 60000 / EDIT_MS)
  sim.opts.eventsPerSession = events
  counters.attempts = counters.rowAdds = counters.offered = 0
  failing = true
  const t0 = clock.now
  await runSession(sim, doc, () => advanceBy(EDIT_MS))
  const window = {
    minutes,
    events,
    simulatedMs: clock.now - t0,
    backlog: provider._pendingUpdates.length,
    backlogBytes: sum(provider._pendingUpdates, u => u.byteLength),
    attempts: counters.attempts,
    rowAdds: counters.rowAdds,
    offered: counters.offered,
    errors,
    exhausted
  }

  // --- Recovery: writes work again ---
  failing = false
  counters.attempts = counters.rowAdds = counters.offered = 0
  await driveUntil(provider.flush())
  window.recoveryAttempts = counters.attempts
  window.recoveryRows = counters.rowAdds
  window.recoveryBytes = counters.offered
  await driveUntil(provider.destroy())

  const persisted = await driveUntil(readSnapshot(DB))
  const reloaded = new Y.Doc()
  Y.applyUpdate(reloaded, /** @type {Uint8Array} */ (persisted))
  window.reloadMatches = docJson(reloaded) === docJson(doc)
  doc.destroy()
  reloaded.destroy()
  await driveUntil(clearDocument(DB))
  return window
}

const main = async () => {
  const results = []
  for (const minutes of MINUTES) {
    results.push(await runScenario(minutes))
  }

  console.log('\n=== Writes failing during a long versicle session ===')
  console.log(`mode=${MODE} editEvery=${EDIT_MS}ms preSessions=${PRE_SESSIONS} maxRetries=5 (default)`)
  console.log('minutes | events | backlog (bytes) | attempts | attempts/min | rowAdds | offered | errors | exhausted | recovery rows (bytes) | reload ok')
  for (const r of results) {
    console.log([
      r.minutes, r.events, `${fmtInt(r.backlog)} (${fmtBytes(r.backlogBytes)})`, fmtInt(r.attempts),
      (r.attempts / (r.simulatedMs / 60000)).toFixed(1), fmtInt(r.rowAdds), fmtBytes(r.offered),
      fmtInt(r.errors), fmtInt(r.exhausted), `${fmtInt(r.recoveryRows)} (${fmtBytes(r.recoveryBytes)})`,
      r.reloadMatches ? 'yes' : 'NO'
    ].join(' | '))
  }
  if (results.length > 1) {
    const a = results[0]
    const b = results[results.length - 1]
    const k = b.minutes / a.minutes
    const ratio = (sel) => sel(a) === 0 ? '-' : 'x' + (sel(b) / sel(a)).toFixed(1)
    console.log(`\n--- scaling, ${b.minutes} min vs ${a.minutes} min (duration x${k.toFixed(1)}) ---`)
    console.log(`attempts ${ratio(r => r.attempts)}  rowAdds ${ratio(r => r.rowAdds)}  offered ${ratio(r => r.offered)}  backlog ${ratio(r => r.backlog)}`)
    console.log(`rowAdds per attempt: ${results.map(r => (r.rowAdds / Math.max(1, r.attempts)).toFixed(1)).join(' / ')}  (backlog at end: ${results.map(r => r.backlog).join(' / ')})`)
  }
  if (results.some(r => !r.reloadMatches)) {
    console.error('reloaded database does not match the live doc')
    process.exit(1)
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
