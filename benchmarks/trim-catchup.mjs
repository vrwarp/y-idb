/* eslint-env browser */
/**
 * y-idb trim catch-up benchmark: rows a trim re-reads and re-applies
 * although this provider flushed them itself
 *
 * Every trim (storeState) first reads every row at or above the provider's
 * read cursor `_dbref` and applies it with Y.applyUpdate — the catch-up for
 * rows another tab may have written. `_dbref` only moves in hydration and in
 * trims, never when the flusher (or the page-hide write) commits, so the
 * catch-up read also returns every row THIS provider flushed since it
 * hydrated or last trimmed. Those updates are already in the doc: applying
 * them changes nothing, but each row is still cloned out of IndexedDB,
 * decoded, and every struct goes through the struct-store lookups. The
 * incremental trim then reads the same rows a second time (getAllKeys +
 * getAll over the tail) to merge them into a delta row.
 *
 * Runs the versicle workload (./versicle-workload.mjs, deterministic seed)
 * with the REAL provider on fake-indexeddb and counts, per trim:
 *
 *  - catch-up rows/bytes: values returned by the trim's first getAll (the
 *    rows it applies), and how many of them this provider flushed itself
 *  - structs + delete-set ranges in those rows (what Y.applyUpdate walks)
 *  - tail rows/bytes: values read again for the merge
 *  - CPU (median of TRIM_REPEATS, interleaved): re-applying the catch-up
 *    rows exactly like applyStoredUpdates does, vs Y.mergeUpdates of the
 *    tail (the work an incremental trim actually needs). fake-indexeddb's
 *    own timing is not representative, so the IndexedDB side is reported
 *    as counts and bytes only.
 *
 * Scenarios (one fresh database each):
 *  - aging: the aging.mjs shape — 60-event sessions, trim at session end
 *    once the store holds PREFERRED_TRIM_SIZE rows
 *  - long:  600-event reading/TTS sessions; the trim fires mid-session right
 *    after the flush that crosses PREFERRED_TRIM_SIZE (as the provider's own
 *    trim timer would, without its 1 s delay)
 *
 * Run with: node benchmarks/trim-catchup.mjs
 * Env: TRIM_AGING_SESSIONS (240), TRIM_LONG_SESSIONS (30),
 *      TRIM_LONG_EVENTS (600), TRIM_REPEATS (7), TRIM_SCENARIOS (aging,long)
 */
import 'fake-indexeddb/auto'
import * as Y from 'yjs'
import { performance } from 'node:perf_hooks'
import {
  IndexeddbPersistence, storeState, clearDocument, PREFERRED_TRIM_SIZE
} from '../src/y-idb.js'
import { createSim, runSession, clientIdForSession } from './versicle-workload.mjs'

const SEED = 20260820
const AGING_SESSIONS = Number(process.env.TRIM_AGING_SESSIONS || 240)
const LONG_SESSIONS = Number(process.env.TRIM_LONG_SESSIONS || 30)
const LONG_EVENTS = Number(process.env.TRIM_LONG_EVENTS || 600)
const REPEATS = Number(process.env.TRIM_REPEATS || 7)
const SCENARIOS = (process.env.TRIM_SCENARIOS || 'aging,long').split(',')

const fmtBytes = (n) => n >= 1048576 ? (n / 1048576).toFixed(2) + ' MB' : n >= 1024 ? (n / 1024).toFixed(1) + ' KB' : n + ' B'
const fmtMs = (n) => n >= 100 ? n.toFixed(0) + ' ms' : n.toFixed(2) + ' ms'
const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b)
  const m = s.length >> 1
  return s.length % 2 === 1 ? s[m] : (s[m - 1] + s[m]) / 2
}
const sum = (arr, sel) => arr.reduce((a, b) => a + sel(b), 0)
const rowKey = (u) => Buffer.from(u.buffer, u.byteOffset, u.byteLength).toString('base64')

// --- IndexedDB probe: getAll() results inside a trim window ---------------
const probe = {
  /** @type {null | { reads: Array<{ values: Array<Uint8Array> }> }} */
  trim: null
}
const storeProto = IDBObjectStore.prototype
const realGetAll = storeProto.getAll
storeProto.getAll = function (...args) {
  const req = realGetAll.apply(this, args)
  const trim = probe.trim
  if (trim !== null && this.name === 'updates') {
    const read = { values: [] }
    trim.reads.push(read)
    req.addEventListener('success', () => { read.values = req.result })
  }
  return req
}

/** Structs + delete-set ranges a Y.applyUpdate of `rows` walks. */
const structCount = (rows) => {
  let n = 0
  for (const r of rows) {
    const { structs, ds } = Y.decodeUpdate(r)
    n += structs.length
    ds.clients.forEach(ranges => { n += ranges.length })
  }
  return n
}

/**
 * Runs storeState(provider, false) inside the probe window and measures it.
 * `ownRows` holds the encoded updates this provider flushed since it last
 * hydrated or trimmed (one row per update).
 */
const measuredTrim = async (provider, doc, ownRows, label) => {
  const trim = { reads: [] }
  probe.trim = trim
  const t0 = performance.now()
  try {
    await storeState(provider, false)
  } finally {
    probe.trim = null
  }
  const trimMs = performance.now() - t0
  // The trim's first getAll is the catch-up read: its rows are applied. Any
  // later getAll is the incremental path re-reading the tail for the merge.
  const catchUp = trim.reads.length > 0 ? trim.reads[0].values : []
  const tail = trim.reads.length > 1 ? trim.reads[1].values : []
  const ownInCatchUp = catchUp.filter(v => ownRows.has(rowKey(v))).length
  const ownInTail = tail.filter(v => ownRows.has(rowKey(v))).length

  // CPU: the redundant re-apply (exactly what applyStoredUpdates does with
  // these rows; the doc already holds them, so it is idempotent) vs the
  // merge an incremental trim needs. Interleaved, median of REPEATS.
  const ownCatchUp = catchUp.filter(v => ownRows.has(rowKey(v)))
  const reapplyMs = []
  const mergeMs = []
  for (let r = 0; r < REPEATS; r++) {
    let t = performance.now()
    Y.transact(doc, () => {
      for (const v of ownCatchUp) Y.applyUpdate(doc, v)
    }, provider, false)
    reapplyMs.push(performance.now() - t)
    if (tail.length > 1) {
      t = performance.now()
      Y.mergeUpdates(tail)
      mergeMs.push(performance.now() - t)
    }
  }
  return {
    label,
    mode: provider._dbsize === 1 ? 'full' : 'delta',
    ownWritten: ownRows.size,
    catchUpRows: catchUp.length,
    catchUpBytes: sum(catchUp, v => v.byteLength),
    ownInCatchUp,
    ownCatchUpBytes: sum(ownCatchUp, v => v.byteLength),
    ownCatchUpStructs: structCount(ownCatchUp),
    tailRows: tail.length,
    tailBytes: sum(tail, v => v.byteLength),
    ownInTail,
    reapplyMs: median(reapplyMs),
    mergeMs: mergeMs.length > 0 ? median(mergeMs) : 0,
    trimMs
  }
}

/**
 * Ages one doc through `sessions` sessions of `eventsPerSession` events
 * (fresh clientID + fresh provider per session, one flush per event).
 * `midSession`: trim right after the flush that crosses the threshold;
 * otherwise trim once at session end (aging.mjs).
 */
const runScenario = async ({ name, sessions, eventsPerSession, midSession }) => {
  const db = `versicle-trim-catchup-${name}`
  await clearDocument(db)
  const sim = createSim({ seed: SEED, eventsPerSession })
  const trims = []
  for (let s = 0; s < sessions; s++) {
    const doc = new Y.Doc()
    doc.clientID = clientIdForSession(SEED, s)
    const provider = new IndexeddbPersistence(db, doc)
    provider._storeTimeout = 1e9 // trims are run explicitly below
    await provider.whenSynced
    let ownRows = new Set()
    const onUpdate = (u, origin) => {
      if (origin !== provider) ownRows.add(rowKey(u))
    }
    doc.on('update', onUpdate)
    const trimIfDue = async () => {
      if (provider._dbsize >= PREFERRED_TRIM_SIZE) {
        trims.push(await measuredTrim(provider, doc, ownRows, `s${s + 1}/e${sim.totalEvents}`))
        ownRows = new Set()
      }
    }
    await runSession(sim, doc, async () => {
      await provider.flush()
      if (midSession) await trimIfDue()
    })
    if (!midSession) await trimIfDue()
    doc.off('update', onUpdate)
    await provider.destroy()
    doc.destroy()
  }
  await clearDocument(db)

  const deltas = trims.filter(x => x.mode === 'delta')
  const ownRows = sum(trims, x => x.ownInCatchUp)
  const ownBytes = sum(trims, x => x.ownCatchUpBytes)
  const readRows = sum(trims, x => x.catchUpRows + x.tailRows)
  const readBytes = sum(trims, x => x.catchUpBytes + x.tailBytes)
  const reapply = sum(trims, x => x.reapplyMs)
  const merge = sum(deltas, x => x.mergeMs)
  const deltaReapply = sum(deltas, x => x.reapplyMs)
  console.log(`\n=== ${name}: ${sessions} sessions x ${eventsPerSession} events, trim ${midSession ? 'mid-session' : 'at session end'} ===`)
  console.log(`events=${sim.totalEvents} trims=${trims.length} (full=${trims.length - deltas.length} delta=${deltas.length})`)
  console.log(`own rows re-read + re-applied by catch-up reads: ${ownRows} rows / ${fmtBytes(ownBytes)} / ${sum(trims, x => x.ownCatchUpStructs)} structs+ds ranges` +
    ` (per trim: ${(ownRows / Math.max(1, trims.length)).toFixed(0)} rows / ${fmtBytes(Math.round(ownBytes / Math.max(1, trims.length)))})`)
  console.log(`foreign/unseen rows in catch-up reads: ${sum(trims, x => x.catchUpRows - x.ownInCatchUp)} (single tab: rows hydration already applied are never re-read)`)
  console.log(`own rows read twice (catch-up + merge tail): ${sum(trims, x => Math.min(x.ownInCatchUp, x.ownInTail))}`)
  console.log(`values read by trims: ${readRows} rows / ${fmtBytes(readBytes)}; without the own-row catch-up: ${readRows - ownRows} rows / ${fmtBytes(readBytes - ownBytes)} (${(100 * ownBytes / Math.max(1, readBytes)).toFixed(0)}% of bytes redundant)`)
  console.log(`CPU (median of ${REPEATS}): re-apply own rows ${fmtMs(reapply)} total, ${fmtMs(reapply / Math.max(1, trims.length))}/trim;` +
    ` delta trims: re-apply ${fmtMs(deltaReapply / Math.max(1, deltas.length))} vs merge ${fmtMs(merge / Math.max(1, deltas.length))} per trim` +
    ` (re-apply = ${(100 * deltaReapply / Math.max(1e-9, deltaReapply + merge)).toFixed(0)}% of trim Yjs CPU)`)
  console.log(`trim wall time on fake-indexeddb: ${fmtMs(sum(trims, x => x.trimMs))} total (not browser-representative)`)
  console.log('trim | mode | own written | catch-up rows (own) | catch-up bytes | own structs | tail rows (own) | tail bytes | re-apply | merge')
  const show = trims.length <= 12 ? trims : [...trims.slice(0, 6), null, ...trims.slice(-5)]
  for (const x of show) {
    if (x === null) {
      console.log('...')
      continue
    }
    console.log([
      x.label, x.mode, x.ownWritten, `${x.catchUpRows} (${x.ownInCatchUp})`, fmtBytes(x.catchUpBytes), x.ownCatchUpStructs,
      `${x.tailRows} (${x.ownInTail})`, fmtBytes(x.tailBytes), fmtMs(x.reapplyMs), fmtMs(x.mergeMs)
    ].join(' | '))
  }
}

const main = async () => {
  if (SCENARIOS.includes('aging')) {
    await runScenario({ name: 'aging', sessions: AGING_SESSIONS, eventsPerSession: 60, midSession: false })
  }
  if (SCENARIOS.includes('long')) {
    await runScenario({ name: 'long', sessions: LONG_SESSIONS, eventsPerSession: LONG_EVENTS, midSession: true })
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
