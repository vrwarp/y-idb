/* eslint-env browser */
/**
 * y-idb trim-failure benchmark: cost of a trim that keeps failing
 *
 * When a trim transaction aborts (realistically a QuotaExceededError when
 * the origin is near its quota, e.g. an EPUB reading app that keeps book
 * files in the same origin), restoreCursorOnAbort puts `_dbref`/`_dbsize`
 * back (`_dbsize` >= PREFERRED_TRIM_SIZE) and no failure state is kept. The
 * next flush's oncomplete therefore arms another trim `_storeTimeout` later,
 * and that retry redoes the whole trim: re-read and re-apply every row after
 * the restored cursor, re-read and merge the tail (incremental) or
 * re-encode the whole doc (full consolidation), and offer the big row again.
 * Nothing commits, so the backlog grows and every attempt costs more.
 *
 * Method (deterministic counters, no wall-clock dependence):
 *  1. Age one versicle-shaped doc through AGE_SESSIONS sessions with the
 *     real provider (as benchmarks/aging.mjs does), then dump the database.
 *  2. For each scenario, restore the dump into a fresh database, hydrate a
 *     provider on it and keep running versicle events, one flush per event,
 *     one event every INTERVAL_MS of VIRTUAL time: setTimeout/clearTimeout/
 *     Date.now are replaced by a virtual clock (fake-indexeddb schedules with
 *     setImmediate, so it is unaffected), so the provider's real 1 s
 *     `_storeTimeout` trim arming runs unmodified and deterministically.
 *  3. Quota pressure is simulated by aborting, at commit, every readwrite
 *     transaction that adds a row larger than LIMIT bytes (small flush rows
 *     still commit, the large trim row is rejected). The abort comes after
 *     every request has succeeded, exactly like a commit-time
 *     QuotaExceededError.
 *  4. Counting starts at the first trim attempt; cumulative counters are
 *     sampled after CHECKPOINTS write events of failure.
 *
 * Counters (IDBDatabase/IDBObjectStore prototype wrappers):
 *  - trims: transactions opened over ['custom', 'updates'] (only the trim
 *    opens that scope), split into committed / aborted / full
 *    (full = the trim deleted every row below its new row)
 *  - applied: rows the trim re-read with its first getAll and re-applied
 *    (Y.applyUpdate per row)
 *  - tail: rows re-read for the incremental merge (Y.mergeUpdates inputs;
 *    also paid by a full consolidation chosen by the row/byte budget, which
 *    is decided only after the merge)
 *  - read: total bytes returned by the trims' getAll requests
 *  - offered: bytes passed to add() by trims (one Y.encodeStateAsUpdate(doc)
 *    per full attempt)
 *  - errors: 'error' events emitted by the provider
 *  - trim ms: wall time from trim start to its transaction settling, and
 *    cpu: process CPU time since the first trim attempt (both indicative
 *    only: fake-indexeddb, shared machine)
 *
 * Scenarios:
 *  - control: no quota limit (trims commit; one per ~500 rows)
 *  - full: trimSegmentRows = 1 so every trim is a full consolidation, LIMIT
 *    between the largest flush row and the consolidated doc
 *  - incremental: default trim options, LIMIT below the merged-tail row
 *
 * Run with: node benchmarks/trim-failure.mjs
 *   env: TRIMFAIL_AGE_SESSIONS (72), TRIMFAIL_CHECKPOINTS ("50,100,200,400"),
 *        TRIMFAIL_INTERVAL_MS (5000), TRIMFAIL_FULL_LIMIT (65536),
 *        TRIMFAIL_INCR_LIMIT (4096), TRIMFAIL_SCENARIOS
 *        ("control,full,incremental")
 */
import 'fake-indexeddb/auto'
import * as Y from 'yjs'
import * as idb from 'lib0/indexeddb'
import { performance } from 'node:perf_hooks'
import {
  IndexeddbPersistence, storeState, clearDocument, PREFERRED_TRIM_SIZE
} from '../src/y-idb.js'
import { createSim, runSession, clientIdForSession, SeededRandom } from './versicle-workload.mjs'

const SEED = 20260820
const AGE_SESSIONS = Number(process.env.TRIMFAIL_AGE_SESSIONS || 72)
const CHECKPOINTS = (process.env.TRIMFAIL_CHECKPOINTS || '50,100,200,400').split(',').map(Number)
const INTERVAL_MS = Number(process.env.TRIMFAIL_INTERVAL_MS || 5000)
const FULL_LIMIT = Number(process.env.TRIMFAIL_FULL_LIMIT || 65536)
const INCR_LIMIT = Number(process.env.TRIMFAIL_INCR_LIMIT || 4096)
const SCENARIOS = (process.env.TRIMFAIL_SCENARIOS || 'control,full,incremental').split(',')
const AGE_DB = 'versicle-trimfail-age'
const RUN_DB = 'versicle-trimfail-run'

const fmtBytes = (n) => n >= 1048576 ? (n / 1048576).toFixed(2) + ' MB' : n >= 1024 ? (n / 1024).toFixed(1) + ' KB' : n + ' B'
const median = (xs) => {
  if (xs.length === 0) return 0
  const s = xs.slice().sort((a, b) => a - b)
  return s[Math.floor(s.length / 2)]
}
const byteLen = (v) => v ? (v.byteLength ?? v.length ?? 0) : 0
const tick = () => new Promise(resolve => setImmediate(resolve))

const openRaw = (name) => idb.openDB(name, db => idb.createStores(db, [
  ['updates', { autoIncrement: true }],
  ['custom']
]))

/** Copies every row (with its key) of both stores. */
const dumpDb = async (name) => {
  const db = await openRaw(name)
  const out = await new Promise((resolve, reject) => {
    const tx = db.transaction(['updates', 'custom'], 'readonly')
    const u = tx.objectStore('updates')
    const c = tx.objectStore('custom')
    const reqs = [u.getAllKeys(), u.getAll(), c.getAllKeys(), c.getAll()]
    tx.oncomplete = () => resolve({
      updates: reqs[0].result.map((k, i) => [k, reqs[1].result[i]]),
      custom: reqs[2].result.map((k, i) => [k, reqs[3].result[i]])
    })
    tx.onerror = () => reject(tx.error)
  })
  db.close()
  return out
}

const restoreDb = async (name, dump) => {
  await clearDocument(name)
  const db = await openRaw(name)
  await new Promise((resolve, reject) => {
    const tx = db.transaction(['updates', 'custom'], 'readwrite')
    const u = tx.objectStore('updates')
    const c = tx.objectStore('custom')
    dump.updates.forEach(([k, v]) => u.add(v, k))
    dump.custom.forEach(([k, v]) => c.put(v, k))
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error)
  })
  db.close()
}

/**
 * IndexedDB instrumentation: counts trim work, tracks in-flight
 * transactions (so the virtual clock can wait for them) and, when
 * `quota.limit` is set, aborts at commit every readwrite transaction that
 * added a row larger than the limit.
 */
const instrument = () => {
  const realTransaction = IDBDatabase.prototype.transaction
  const proto = IDBObjectStore.prototype
  const realGetAll = proto.getAll
  const realAdd = proto.add
  const realDelete = proto.delete
  const requestMethods = ['add', 'put', 'delete', 'clear', 'get', 'getKey', 'getAll', 'getAllKeys', 'count', 'openCursor', 'openKeyCursor']
  const realMethods = Object.fromEntries(requestMethods.map(m => [m, proto[m]]))
  const live = new Set()
  /** tx -> per-trim record */
  const trimTx = new Map()
  /** tx -> quota-abort bookkeeping (only for tx that added an oversize row) */
  const doomed = new Map()
  const quota = { limit: Infinity }
  const c = {
    trims: 0,
    committed: 0,
    aborted: 0,
    full: 0,
    applied: 0,
    tail: 0,
    readBytes: 0,
    offered: 0,
    trimMs: []
  }
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    live.add(tx)
    const names = Array.isArray(storeNames) ? storeNames : [storeNames]
    const isTrim = names.length === 2 && names.includes('custom') && names.includes('updates') && mode === 'readwrite'
    if (isTrim) {
      c.trims++
      trimTx.set(tx, { getAlls: 0, full: false, t0: performance.now() })
    }
    const done = (committed) => {
      live.delete(tx)
      doomed.delete(tx)
      const rec = trimTx.get(tx)
      if (rec) {
        trimTx.delete(tx)
        if (committed) c.committed++
        else c.aborted++
        if (rec.full) c.full++
        c.trimMs.push(performance.now() - rec.t0)
      }
    }
    tx.addEventListener('complete', () => done(true))
    tx.addEventListener('abort', () => done(false))
    return tx
  }
  const watchDoomed = (store, req) => {
    const d = doomed.get(store.transaction)
    if (!d) return
    d.requests.push(req)
    req.addEventListener('success', () => {
      // Runs after the success callbacks' microtasks (which may issue the
      // next request) and before fake-indexeddb's next transaction step:
      // abort only when the transaction would otherwise commit.
      setImmediate(() => {
        if (!d.aborted && doomed.get(store.transaction) === d && d.requests.every(r => r.readyState === 'done')) {
          d.aborted = true
          try { store.transaction.abort() } catch (e) {}
        }
      })
    })
  }
  requestMethods.forEach(m => {
    if (m === 'add' || m === 'getAll' || m === 'delete') return
    proto[m] = function (...args) {
      const req = realMethods[m].apply(this, args)
      watchDoomed(this, req)
      return req
    }
  })
  proto.add = function (value, key) {
    const req = realAdd.apply(this, arguments)
    const rec = trimTx.get(this.transaction)
    if (rec) c.offered += byteLen(value)
    if (byteLen(value) > quota.limit && !doomed.has(this.transaction)) {
      doomed.set(this.transaction, { requests: [], aborted: false })
    }
    watchDoomed(this, req)
    return req
  }
  proto.getAll = function (...args) {
    const req = realGetAll.apply(this, args)
    const rec = trimTx.get(this.transaction)
    if (rec && this.name === 'updates') {
      const first = rec.getAlls++ === 0
      req.addEventListener('success', () => {
        const rows = req.result
        if (first) c.applied += rows.length
        else c.tail += rows.length
        for (const r of rows) c.readBytes += byteLen(r)
      })
    }
    watchDoomed(this, req)
    return req
  }
  proto.delete = function (range) {
    const req = realDelete.apply(this, arguments)
    const rec = trimTx.get(this.transaction)
    // A full consolidation deletes every row below its new row.
    if (rec && this.name === 'updates' && range instanceof IDBKeyRange && range.lower === undefined) {
      rec.full = true
    }
    watchDoomed(this, req)
    return req
  }
  const restore = () => {
    IDBDatabase.prototype.transaction = realTransaction
    requestMethods.forEach(m => { proto[m] = realMethods[m] })
  }
  /** Waits until no transaction is in flight (and promise chains settled). */
  const quiesce = async () => {
    for (let idle = 0; idle < 3;) {
      await tick()
      idle = live.size === 0 ? idle + 1 : 0
    }
  }
  return { c, quota, restore, quiesce }
}

/**
 * Virtual clock for setTimeout/clearTimeout/Date.now. `advance(ms)` fires
 * due timers in order and lets the IndexedDB work each one starts settle.
 */
const installVirtualClock = (quiesce) => {
  const realSetTimeout = globalThis.setTimeout
  const realClearTimeout = globalThis.clearTimeout
  const realDateNow = Date.now
  const base = realDateNow()
  let now = 0
  let seq = 0
  const timers = new Map()
  globalThis.setTimeout = (fn, ms, ...args) => {
    const id = { virtualTimer: ++seq }
    timers.set(id, { due: now + Math.max(0, Number(ms) || 0), seq, fn, args })
    return id
  }
  globalThis.clearTimeout = (id) => { timers.delete(id) }
  Date.now = () => base + now
  const advance = async (ms) => {
    const target = now + ms
    for (;;) {
      let next = null
      for (const [id, t] of timers) {
        if (t.due <= target && (next === null || t.due < next[1].due || (t.due === next[1].due && t.seq < next[1].seq))) next = [id, t]
      }
      if (next === null) break
      timers.delete(next[0])
      now = next[1].due
      next[1].fn(...next[1].args)
      await quiesce()
    }
    now = target
  }
  const restore = () => {
    globalThis.setTimeout = realSetTimeout
    globalThis.clearTimeout = realClearTimeout
    Date.now = realDateNow
  }
  return { advance, restore }
}

const cloneSim = (sim) => {
  const rng = new SeededRandom(1)
  rng.seed = sim.rng.seed
  return { ...sim, rng, bookIds: sim.bookIds.slice() }
}

/** Ages the doc like benchmarks/aging.mjs; returns the sim state. */
const age = async () => {
  await clearDocument(AGE_DB)
  const sim = createSim({ seed: SEED })
  let doc = null
  for (let s = 0; s < AGE_SESSIONS; s++) {
    doc = new Y.Doc()
    doc.clientID = clientIdForSession(SEED, s)
    const provider = new IndexeddbPersistence(AGE_DB, doc)
    provider._storeTimeout = 1e9 // trims run explicitly at session end
    await provider.whenSynced
    await runSession(sim, doc, () => provider.flush())
    if (provider._dbsize >= PREFERRED_TRIM_SIZE) await storeState(provider, false)
    await provider.destroy()
    if (s < AGE_SESSIONS - 1) doc.destroy()
  }
  const docBytes = Y.encodeStateAsUpdate(doc).byteLength
  doc.destroy()
  return { sim, docBytes }
}

const runScenario = async (name, dump, simAfterAging) => {
  const limit = name === 'full' ? FULL_LIMIT : name === 'incremental' ? INCR_LIMIT : Infinity
  const opts = name === 'full' ? { trimSegmentRows: 1 } : {}
  await restoreDb(RUN_DB, dump)
  const ins = instrument()
  const clock = installVirtualClock(ins.quiesce)
  const sim = cloneSim(simAfterAging)
  const doc = new Y.Doc()
  doc.clientID = clientIdForSession(SEED, AGE_SESSIONS)
  let provider = null
  const samples = []
  let maxFlushRow = 0
  let errors = 0
  let started = false
  let failureEvents = 0
  const maxEvents = Math.max(...CHECKPOINTS)
  const cpuStart = process.cpuUsage()
  let cpuAtStart = null
  try {
    provider = new IndexeddbPersistence(RUN_DB, doc, opts)
    await provider.whenSynced
    const hydratedRows = provider._dbsize
    provider.on('error', () => { errors++ })
    doc.on('update', (u, origin) => {
      if (origin !== provider && u.byteLength > maxFlushRow) maxFlushRow = u.byteLength
    })
    ins.quota.limit = limit
    // Every counter only counts trim work, so before the first trim they are
    // all zero: no baseline subtraction is needed.
    const snapshot = () => ({ ...ins.c, trimMs: ins.c.trimMs.slice(), errors })
    const STOP = Symbol('stop')
    const onEvent = async () => {
      await provider.flush()
      const trimsBefore = ins.c.trims
      const cpuBefore = process.cpuUsage(cpuStart)
      await clock.advance(INTERVAL_MS)
      if (!started && ins.c.trims > trimsBefore) {
        // Count from the first trim attempt (it belongs to this event).
        started = true
        cpuAtStart = cpuBefore
      }
      if (started) {
        failureEvents++
        if (CHECKPOINTS.includes(failureEvents)) {
          const cpu = process.cpuUsage(cpuStart)
          samples.push({
            events: failureEvents,
            rows: provider._dbsize,
            cpuMs: (cpu.user + cpu.system - cpuAtStart.user - cpuAtStart.system) / 1000,
            ...snapshot()
          })
        }
        if (failureEvents >= maxEvents) throw STOP
      }
    }
    try {
      for (;;) await runSession(sim, doc, onEvent)
    } catch (e) {
      if (e !== STOP) throw e
    }
    return { name, limit, opts, hydratedRows, maxFlushRow, samples }
  } finally {
    ins.quota.limit = Infinity
    if (provider) await provider.destroy()
    clock.restore()
    ins.restore()
    doc.destroy()
    await clearDocument(RUN_DB)
  }
}

const main = async () => {
  const t0 = performance.now()
  const { sim, docBytes } = await age()
  const dump = await dumpDb(AGE_DB)
  await clearDocument(AGE_DB)
  const dbBytes = dump.updates.reduce((a, [, v]) => a + byteLen(v), 0)
  console.log('\n=== y-idb: a trim that keeps failing (virtual clock, simulated quota) ===')
  console.log(`aged ${AGE_SESSIONS} versicle sessions (${sim.totalEvents} events) in ${((performance.now() - t0) / 1000).toFixed(1)} s: doc=${fmtBytes(docBytes)} rows=${dump.updates.length} dbBytes=${fmtBytes(dbBytes)}`)
  console.log(`one write event (one flush) every ${INTERVAL_MS} ms of virtual time; provider _storeTimeout = 1000 ms (unmodified)`)
  console.log('counted from the first trim attempt; values are cumulative over the first N write events')

  for (const name of SCENARIOS) {
    const r = await runScenario(name, dump, sim)
    console.log(`\n--- ${name}: quota limit=${r.limit === Infinity ? 'none' : fmtBytes(r.limit)} opts=${JSON.stringify(r.opts)} hydratedRows=${r.hydratedRows} largestFlushRow=${fmtBytes(r.maxFlushRow)}`)
    console.log('events | rowsInStore | trims(committed/aborted, full) | applied rows | tail rows | read | offered by trims | errors | trim ms median (max) | cpu')
    for (const s of r.samples) {
      console.log([
        s.events, s.rows, `${s.trims}(${s.committed}/${s.aborted}, ${s.full})`, s.applied, s.tail,
        fmtBytes(s.readBytes), fmtBytes(s.offered), s.errors,
        `${median(s.trimMs).toFixed(1)} (${Math.max(0, ...s.trimMs).toFixed(1)})`,
        `${s.cpuMs.toFixed(0)} ms`
      ].join(' | '))
    }
    if (r.samples.length >= 2) {
      const a = r.samples[0]
      const b = r.samples[r.samples.length - 1]
      const k = b.events / a.events
      const ratio = (sel) => (sel(a) === 0 ? '-' : (sel(b) / sel(a)).toFixed(2) + 'x')
      console.log(`scaling ${a.events} -> ${b.events} events (${k}x): trims ${ratio(x => x.trims)}, applied ${ratio(x => x.applied)}, tail ${ratio(x => x.tail)}, read ${ratio(x => x.readBytes)}, offered ${ratio(x => x.offered)}`)
    }
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
