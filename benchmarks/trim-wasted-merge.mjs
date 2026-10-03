/* eslint-env browser */
/**
 * y-idb trim benchmark: tail merge wasted before a full consolidation
 *
 * The incremental trim in `_storeState` reads the tail (getAllKeys + getAll
 * over (lastSegKey, inf)) and runs `Y.mergeUpdates` over it BEFORE checking
 * whether the trim must consolidate fully (segRows + 1 >= trimSegmentRows,
 * or segBytes + merged >= max(trimFullCompactBytes, baseBytes)). When the
 * check fires, `fullConsolidation()` re-encodes the whole doc and the merge
 * result is discarded — and the merge and the full encode run back to back
 * in ONE synchronous task, the trim's longest main-thread task.
 *
 * This ages the versicle-shaped doc of ./aging.mjs (same seed, same
 * workload, same "trim when _dbsize >= PREFERRED_TRIM_SIZE" driver) with the
 * real provider on fake-indexeddb and instruments every trim:
 *
 *  - deterministic counters taken INSIDE storeState(): tail reads
 *    (getAllKeys requests), tail rows/bytes returned by the tail getAll, and
 *    the strings Yjs decodes after the tail read was issued
 *    (TextDecoder.prototype.decode calls/bytes). Nothing else in a trim
 *    decodes after that point (fullConsolidation only encodes), so non-zero
 *    decodes in a full trim = a merge whose result was thrown away.
 *  - the trim's decision inputs, read from the store before the trim
 *    (segRows, segBytes, baseBytes) plus the merged size, so the proposed
 *    pre-checks (row count before the tail read; segBytes + raw tail bytes
 *    before the merge) can be checked against the exact decision.
 *  - CPU split of the critical task: Y.mergeUpdates(tail) vs
 *    Y.encodeStateAsUpdate(doc) on the very inputs of that trim, timed
 *    outside the trim, interleaved, median of TRIM_REPS runs; and inline,
 *    the synchronous block between the trim's last IDB read callback and its
 *    first add() (merge [+ encode]).
 *
 * Scenario A ("aging"): TRIM_SESSIONS (240) sessions of the versicle workload.
 * Scenario B ("initstate"): one more boot in which the doc already holds the
 * full state when the provider attaches (e.g. it was hydrated from the
 * remote first), so hydration writes an O(document) initial-state row into
 * the tail; sessions continue until the next trim.
 *
 * Timings come from fake-indexeddb + Node and are only indicative; the
 * counters are deterministic for the seed.
 *
 * Run with: node benchmarks/trim-wasted-merge.mjs
 *   env: TRIM_SESSIONS (default 240), TRIM_REPS (default 7),
 *        TRIM_SEGMENT_ROWS / TRIM_FULL_COMPACT_BYTES (provider trim options)
 */
import 'fake-indexeddb/auto'
import * as Y from 'yjs'
import { performance } from 'node:perf_hooks'
import {
  IndexeddbPersistence, storeState, clearDocument, PREFERRED_TRIM_SIZE
} from '../src/y-idb.js'
import { createSim, runSession, clientIdForSession } from './versicle-workload.mjs'

const SEED = 20260820
const SESSIONS = Number(process.env.TRIM_SESSIONS || 240)
const REPS = Number(process.env.TRIM_REPS || 7)
// Optional policy overrides, e.g. TRIM_FULL_COMPACT_BYTES=67108864
// TRIM_SEGMENT_ROWS=8 to exercise the row-count trigger instead.
const PROVIDER_OPTS = {}
if (process.env.TRIM_SEGMENT_ROWS) PROVIDER_OPTS.trimSegmentRows = Number(process.env.TRIM_SEGMENT_ROWS)
if (process.env.TRIM_FULL_COMPACT_BYTES) PROVIDER_OPTS.trimFullCompactBytes = Number(process.env.TRIM_FULL_COMPACT_BYTES)
const DB = 'versicle-trim-wasted-merge-bench'
const TRIM_STATE_KEY = '__yidb_trim_v1'

const fmtBytes = (n) => n >= 1048576 ? (n / 1048576).toFixed(2) + ' MB' : n >= 1024 ? (n / 1024).toFixed(1) + ' KB' : n + ' B'
const fmtMs = (n) => n.toFixed(1)
const median = (xs) => {
  const s = xs.slice().sort((a, b) => a - b)
  return s.length === 0 ? 0 : s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2
}

// ---------------------------------------------------------------------------
// Probe: counts what storeState() itself does, between begin() and end().
// ---------------------------------------------------------------------------
const probe = {
  active: false,
  tailIssued: false,
  expectTailValues: false,
  getAllKeys: 0,
  tailRows: 0,
  tailBytes: 0,
  decodes: 0,
  decodedBytes: 0,
  lastSuccessAt: 0,
  blockMs: -1,
  begin () {
    Object.assign(probe, {
      active: true,
      tailIssued: false,
      expectTailValues: false,
      getAllKeys: 0,
      tailRows: 0,
      tailBytes: 0,
      decodes: 0,
      decodedBytes: 0,
      lastSuccessAt: 0,
      blockMs: -1
    })
  },
  end () { probe.active = false }
}

const storeProto = IDBObjectStore.prototype
/** Stamp the success time of every request the trim issues. */
const stamp = (req) => {
  req.addEventListener('success', () => { probe.lastSuccessAt = performance.now() })
  return req
}
for (const m of ['get', 'count', 'openKeyCursor', 'delete', 'put']) {
  const orig = storeProto[m]
  storeProto[m] = function (...args) {
    const req = orig.apply(this, args)
    return probe.active ? stamp(req) : req
  }
}
const origGetAllKeys = storeProto.getAllKeys
storeProto.getAllKeys = function (...args) {
  const req = origGetAllKeys.apply(this, args)
  if (probe.active) {
    // getAllKeysValues() (the incremental trim's tail read) is the only
    // getAllKeys caller in a trim; its getAll is issued right after.
    probe.getAllKeys++
    probe.tailIssued = true
    probe.expectTailValues = true
    stamp(req)
  }
  return req
}
const origGetAll = storeProto.getAll
storeProto.getAll = function (...args) {
  const req = origGetAll.apply(this, args)
  if (probe.active) {
    stamp(req)
    if (probe.expectTailValues) {
      probe.expectTailValues = false
      req.addEventListener('success', () => {
        for (const v of req.result) {
          probe.tailRows++
          probe.tailBytes += v.byteLength
        }
      })
    }
  }
  return req
}
const origAdd = storeProto.add
storeProto.add = function (...args) {
  // The first add() after the tail read ends the synchronous merge
  // [+ full encode] block.
  if (probe.active && probe.tailIssued && probe.blockMs < 0) {
    probe.blockMs = performance.now() - probe.lastSuccessAt
  }
  const req = origAdd.apply(this, args)
  return probe.active ? stamp(req) : req
}
const origDecode = TextDecoder.prototype.decode
TextDecoder.prototype.decode = function (input, opts) {
  if (probe.active && probe.tailIssued) {
    probe.decodes++
    probe.decodedBytes += input ? input.byteLength : 0
  }
  return origDecode.call(this, input, opts)
}

// ---------------------------------------------------------------------------
// Raw reads of the store (outside the probe)
// ---------------------------------------------------------------------------
const req2p = (req) => new Promise((resolve, reject) => {
  req.onsuccess = () => resolve(req.result)
  req.onerror = () => reject(req.error)
})

/** Trim bookkeeping, delta-row count and the tail rows, as the trim sees them. */
const readTrimInputs = async (db) => {
  const tx = db.transaction(['updates', 'custom'], 'readonly')
  const updates = tx.objectStore('updates')
  const trimState = await req2p(tx.objectStore('custom').get(TRIM_STATE_KEY))
  if (trimState === undefined) return { trimState }
  const segRows = trimState.lastSegKey > trimState.baseKey
    ? await req2p(updates.count(IDBKeyRange.bound(trimState.baseKey, trimState.lastSegKey, true, false)))
    : 0
  const tail = await req2p(updates.getAll(IDBKeyRange.lowerBound(trimState.lastSegKey, true)))
  return { trimState, segRows, tail }
}

const readTrimState = async (db) => {
  const tx = db.transaction(['custom'], 'readonly')
  return req2p(tx.objectStore('custom').get(TRIM_STATE_KEY))
}

// ---------------------------------------------------------------------------
// One instrumented trim
// ---------------------------------------------------------------------------
const trims = []

const instrumentedTrim = async (provider, doc, label) => {
  const inputs = await readTrimInputs(provider.db)
  probe.begin()
  await storeState(provider, false)
  probe.end()
  const after = await readTrimState(provider.db)
  const full = after.baseKey === after.lastSegKey && after.segBytes === 0
  const rec = {
    label,
    mode: full ? 'full' : 'delta',
    trigger: 'delta',
    segRows: inputs.segRows ?? 0,
    getAllKeys: probe.getAllKeys,
    tailRows: probe.tailRows,
    tailBytes: probe.tailBytes,
    decodes: probe.decodes,
    decodedBytes: probe.decodedBytes,
    blockMs: probe.blockMs,
    mergedBytes: 0,
    budget: 0,
    mergeMs: 0,
    encodeMs: 0,
    encodedBytes: after.baseBytes,
    precheck: '-'
  }
  if (inputs.trimState === undefined) {
    rec.trigger = 'base' // no bookkeeping yet: the tail is never read
  } else if (inputs.tail.length > 0) {
    const { trimState, segRows, tail } = inputs
    const merged = tail.length === 1 ? tail[0] : Y.mergeUpdates(tail)
    const rawTail = tail.reduce((a, v) => a + v.byteLength, 0)
    rec.mergedBytes = merged.byteLength
    rec.budget = Math.max(provider._trimFullCompactBytes, trimState.baseBytes)
    const byRows = segRows + 1 >= provider._trimSegmentRows
    const byBytesExact = trimState.segBytes + merged.byteLength >= rec.budget
    const byBytesUpper = trimState.segBytes + rawTail >= rec.budget
    if (full) rec.trigger = byRows ? 'rows' : 'bytes'
    // Proposed decision: rows first, then segBytes + raw tail bytes as an
    // upper bound, then the exact check on the merged result.
    const proposedFull = byRows || byBytesUpper || byBytesExact
    rec.precheck = proposedFull === full ? 'same' : 'FLIP'
    rec.precheckSkipsMerge = full && (byRows || byBytesUpper)
    // CPU split on the same inputs, interleaved, median of REPS
    const mergeT = []
    const encodeT = []
    for (let r = 0; r < REPS; r++) {
      let t = performance.now()
      if (tail.length > 1) Y.mergeUpdates(tail)
      mergeT.push(performance.now() - t)
      t = performance.now()
      Y.encodeStateAsUpdate(doc)
      encodeT.push(performance.now() - t)
    }
    rec.mergeMs = median(mergeT)
    rec.encodeMs = median(encodeT)
  }
  trims.push(rec)
  return rec
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------
let lastDoc = null

/**
 * One versicle session; `preload` hydrates the doc with the full state
 * BEFORE the provider attaches (scenario B).
 */
const session = async (sim, s, label, preload) => {
  const doc = new Y.Doc()
  doc.clientID = clientIdForSession(SEED, s)
  if (preload) Y.applyUpdate(doc, Y.encodeStateAsUpdate(preload))
  const provider = new IndexeddbPersistence(DB, doc, PROVIDER_OPTS)
  provider._storeTimeout = 1e9 // trims are driven (and instrumented) below
  await provider.whenSynced
  await runSession(sim, doc, async () => { await provider.flush() })
  let rec = null
  if (provider._dbsize >= PREFERRED_TRIM_SIZE) {
    rec = await instrumentedTrim(provider, doc, label)
    rec.session = s + 1
  }
  await provider.destroy()
  if (lastDoc) lastDoc.destroy()
  lastDoc = doc
  return rec
}

const printTable = (rows) => {
  console.log('trim | scen | session | mode | trigger | segRows | tailReads | tailRows | tailBytes | merged | budget | strDecodes (bytes) | merge ms | encode ms | merge share | block ms | pre-check')
  rows.forEach((r, i) => {
    const share = r.mode === 'full' && r.mergeMs > 0 ? (100 * r.mergeMs / (r.mergeMs + r.encodeMs)).toFixed(0) + '%' : '-'
    console.log([
      i + 1, r.label, r.session, r.mode, r.trigger, r.segRows, r.getAllKeys, r.tailRows, fmtBytes(r.tailBytes),
      r.mergedBytes ? fmtBytes(r.mergedBytes) : '-', r.budget ? fmtBytes(r.budget) : '-',
      `${r.decodes} (${fmtBytes(r.decodedBytes)})`,
      r.mergeMs ? fmtMs(r.mergeMs) : '-', r.encodeMs ? fmtMs(r.encodeMs) : '-', share,
      r.blockMs >= 0 ? fmtMs(r.blockMs) : '-', r.precheck
    ].join(' | '))
  })
}

const summarize = (title, rows) => {
  const full = rows.filter(r => r.mode === 'full' && r.trigger !== 'base')
  const thrown = full.filter(r => r.decodes > 0)
  const byRows = full.filter(r => r.trigger === 'rows')
  const sum = (arr, sel) => arr.reduce((a, r) => a + sel(r), 0)
  console.log(`\n--- ${title} ---`)
  console.log(`full consolidations decided by the tiered policy: ${full.length} (rows-triggered=${byRows.length}, bytes-triggered=${full.length - byRows.length}); of those, merged the tail and threw it away: ${thrown.length}`)
  if (full.length === 0) return
  console.log(`tail rows read by rows-triggered full trims (not needed for the decision): ${sum(byRows, r => r.tailRows)} rows, ${fmtBytes(sum(byRows, r => r.tailBytes))}`)
  console.log(`merge work thrown away: ${sum(thrown, r => r.tailRows)} rows / ${fmtBytes(sum(thrown, r => r.tailBytes))} merged, ${sum(thrown, r => r.decodes)} strings (${fmtBytes(sum(thrown, r => r.decodedBytes))}) decoded`)
  console.log(`CPU of the merge on those tails vs the full encode (offline, median of ${REPS} interleaved runs each; median over trims): merge ${fmtMs(median(full.map(r => r.mergeMs)))} ms, encode ${fmtMs(median(full.map(r => r.encodeMs)))} ms -> merge = ${(100 * median(full.map(r => r.mergeMs / (r.mergeMs + r.encodeMs)))).toFixed(0)}% of merge+encode`)
  console.log(`inline critical block (last IDB callback -> first add(), median over trims): ${fmtMs(median(full.map(r => r.blockMs)))} ms`)
  console.log(`proposed pre-checks (rows before the tail read, segBytes + raw tail bytes before the merge) decide before merging in ${full.filter(r => r.precheckSkipsMerge).length}/${full.length} full trims; decision flips over all ${rows.length} trims: ${rows.filter(r => r.precheck === 'FLIP').length}`)
}

const main = async () => {
  await clearDocument(DB)
  const sim = createSim({ seed: SEED })

  // Scenario A: versicle aging
  for (let s = 0; s < SESSIONS; s++) {
    await session(sim, s, 'A', null)
  }
  const aRows = trims.slice()
  const deltas = aRows.filter(r => r.mode === 'delta')

  // Scenario B: one boot with the full state already in the doc, then
  // sessions until the next trim folds that initial-state row.
  let s = SESSIONS
  let rec = await session(sim, s++, 'B', lastDoc)
  while (rec === null && s < SESSIONS + 50) {
    rec = await session(sim, s++, 'B', null)
  }
  const bRows = trims.slice(aRows.length)

  console.log('\n=== y-idb trim: tail merge wasted before a full consolidation ===')
  console.log(`seed=${SEED} sessions=${SESSIONS} events=${sim.totalEvents} reps=${REPS} trims=${trims.length} opts=${JSON.stringify(PROVIDER_OPTS)}`)
  printTable(trims)
  console.log(`\ndelta trims (merge is needed): ${deltas.length}, median merge ${fmtMs(median(deltas.map(r => r.mergeMs)))} ms over ${median(deltas.map(r => r.tailRows))} rows`)
  summarize('scenario A: versicle aging', aRows)
  summarize('scenario B: initial-state row in the tail', bRows)

  await clearDocument(DB)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
