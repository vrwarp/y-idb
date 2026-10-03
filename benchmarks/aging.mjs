/* eslint-env browser */
/**
 * y-idb aging benchmark: IndexedDB-path costs vs document age
 *
 * Ages one versicle-shaped Y.Doc (see ./versicle-workload.mjs) through many
 * sessions persisted with the REAL IndexeddbPersistence provider running on
 * fake-indexeddb, and samples the age-sensitive costs at regular epochs:
 *
 *  - hydration: `new IndexeddbPersistence(...)` -> whenSynced on the aged
 *    database (what every versicle boot pays before the UI has data)
 *  - trim (storeState): the full-document re-encode + rewrite that fires
 *    every PREFERRED_TRIM_SIZE updates — main-thread cost in the browser
 *  - snapshot row size, row count, total database bytes
 *  - write amplification: bytes written to IndexedDB / bytes of update
 *    blobs actually produced by edits
 *  - readSnapshot(): versicle's staged-swap / checkpoint read path
 *
 * Harness health (NOT library costs — they say whether the timings above can
 * be trusted). fake-indexeddb 6.x keeps every IDBTransaction ever created on
 * its internal Database object (`transactions[]` is only filtered for
 * scheduling, never pruned; closed connections ARE dropped from
 * `connections[]`), and each finished transaction still references its
 * oncomplete/onerror closures and its never-cleared rollback log — so every
 * session's provider and Y.Doc would stay reachable for the whole run.
 * Browsers release finished transactions, and so does this bench: it drops
 * them from that list before each new transaction (fake-indexeddb is pinned
 * to 6.2.5 for it). Without that, the retained heap grew with sessions x
 * document size (6.5 GB at 240 sessions; a 4 GB heap OOMs near 190) and the
 * major-GC pauses landed inside the timed samples: at 240 sessions hydration
 * read 994 ms instead of 394 ms (growth 72 vs 28 ms per 1k events) and
 * readSnapshot 293 instead of 154 ms (medians of 5 interleaved runs). Figures
 * taken before the prune carry that inflation — e.g. y-cinder's
 * docs/performance.md "hydration on every boot grew 37 ms → 1,004 ms" is
 * about 2.5x too high at the old end. These columns show the prune holding:
 *
 *  - harnessTx: IDBTransactions fake-indexeddb still holds on the bench DB
 *  - liveDocs: session Y.Docs not yet garbage-collected (destroyed docs only)
 *  - heap: V8 heapUsed at the epoch (forced full GC first when run with
 *    `node --expose-gc`, so it is the retained heap)
 *  - gc: GC pause time that overlapped the timed hydration / readSnapshot /
 *    trim windows (from PerformanceObserver 'gc' entries)
 *
 * Run with: npm run bench
 * (AGING_JSON=1 prints the epochs and summary as one JSON object instead.)
 */
import 'fake-indexeddb/auto'
import FDBDatabase from 'fake-indexeddb/lib/FDBDatabase'
import * as Y from 'yjs'
import { performance, PerformanceObserver } from 'node:perf_hooks'
import {
  IndexeddbPersistence, storeState, readSnapshot, clearDocument, PREFERRED_TRIM_SIZE
} from '../src/y-idb.js'
import {
  createSim, runSession, clientIdForSession, docStructStats
} from './versicle-workload.mjs'

const SEED = 20260820
const SESSIONS = Number(process.env.AGING_SESSIONS || 240)
const EPOCH_EVERY = Number(process.env.AGING_EPOCH || 24)
const DB = 'versicle-aging-bench'
const JSON_OUT = Boolean(process.env.AGING_JSON)

const fmtBytes = (n) => n >= 1048576 ? (n / 1048576).toFixed(2) + ' MB' : n >= 1024 ? (n / 1024).toFixed(1) + ' KB' : n + ' B'
const fmtMs = (n) => n >= 100 ? n.toFixed(0) + ' ms' : n.toFixed(2) + ' ms'

// Release finished IDBTransactions like browsers do (see the header): drop
// them from fake-indexeddb's internal list before each new transaction is
// queued. Only 'finished' ones go — its scheduler, closeConnection and the
// versionchange checks still need every unfinished transaction listed. This
// relies on fake-indexeddb internals (pinned to 6.2.5); without them nothing
// is pruned and harnessTx reports null.
const fakeTransaction = FDBDatabase.prototype.transaction
FDBDatabase.prototype.transaction = function (...args) {
  const raw = /** @type {any} */ (this)._rawDatabase
  if (raw && Array.isArray(raw.transactions)) {
    raw.transactions = raw.transactions.filter(tx => tx._state !== 'finished')
  }
  return fakeTransaction.apply(this, args)
}

/** Reads raw row stats (keyed sizes) straight out of the updates store. */
const dbRowStats = async () => {
  const dbreq = indexedDB.open(DB)
  const db = await new Promise((resolve, reject) => {
    dbreq.onsuccess = () => resolve(dbreq.result)
    dbreq.onerror = () => reject(dbreq.error)
  })
  const [rows, keys] = await new Promise((resolve, reject) => {
    const tx = db.transaction(['updates'], 'readonly')
    const store = tx.objectStore('updates')
    const reqV = store.getAll()
    const reqK = store.getAllKeys()
    tx.oncomplete = () => resolve([reqV.result, reqK.result])
    tx.onerror = () => reject(tx.error)
  })
  // Harness health: transactions fake-indexeddb still holds on this
  // database (null when not running on fake-indexeddb's internals).
  const raw = /** @type {any} */ (db)._rawDatabase
  const harnessTx = raw && Array.isArray(raw.transactions) ? raw.transactions.length : null
  db.close()
  let total = 0
  let largest = 0
  const sizesByKey = new Map()
  for (let i = 0; i < rows.length; i++) {
    const len = rows[i].byteLength ?? rows[i].length ?? 0
    total += len
    if (len > largest) largest = len
    sizesByKey.set(keys[i], len)
  }
  return { count: rows.length, totalBytes: total, largestBytes: largest, sizesByKey, harnessTx }
}

// --- Harness health: GC pauses and Y.Doc liveness -------------------------
/** @type {Array<[number, number]>} [start, end] of every GC pause */
const gcPauses = []
const gcObserver = new PerformanceObserver(list => {
  for (const e of list.getEntries()) gcPauses.push([e.startTime, e.startTime + e.duration])
})
gcObserver.observe({ entryTypes: ['gc'] })
/** GC pause ms overlapping the given [start, end] windows. */
const gcInside = (windows) => {
  let ms = 0
  for (const [a, b] of windows) {
    for (const [s, e] of gcPauses) {
      const lo = Math.max(a, s)
      const hi = Math.min(b, e)
      if (hi > lo) ms += hi - lo
    }
  }
  return ms
}
let docsCreated = 0
let docsCollected = 0
const docRegistry = new FinalizationRegistry(() => { docsCollected++ })
const heapMB = () => {
  if (typeof globalThis.gc === 'function') globalThis.gc()
  return process.memoryUsage().heapUsed / 1048576
}

const fitSlope = (xs, ys) => {
  const n = xs.length
  const mx = xs.reduce((a, b) => a + b, 0) / n
  const my = ys.reduce((a, b) => a + b, 0) / n
  let num = 0
  let den = 0
  for (let i = 0; i < n; i++) {
    num += (xs[i] - mx) * (ys[i] - my)
    den += (xs[i] - mx) * (xs[i] - mx)
  }
  return den === 0 ? 0 : num / den
}

const main = async () => {
  await clearDocument(DB)
  const sim = createSim({ seed: SEED })

  let idbBytesWritten = 0
  const epochs = []
  const trimSamples = []
  let hydrationSamples = []
  /** @type {Array<[number, number]>} timed hydration windows of this epoch */
  let hydrationWindows = []
  /** @type {Array<[number, number]>} */
  const trimWindows = []
  const tRun = performance.now()

  for (let s = 0; s < SESSIONS; s++) {
    const doc = new Y.Doc()
    doc.clientID = clientIdForSession(SEED, s)
    docRegistry.register(doc, s)
    docsCreated++

    // --- Hydration: what a versicle boot pays before data is usable ---
    const t0 = performance.now()
    const provider = new IndexeddbPersistence(DB, doc)
    provider._storeTimeout = 1e9 // trims are timed explicitly below
    await provider.whenSynced
    const t0end = performance.now()
    hydrationSamples.push(t0end - t0)
    hydrationWindows.push([t0, t0end])

    // Track the update bytes this session actually produces (the "useful"
    // write volume, denominator of write amplification)
    const onUpdate = (u, origin) => {
      if (origin !== provider) {
        sim.bytesProduced += u.byteLength
        idbBytesWritten += u.byteLength
      }
    }
    doc.on('update', onUpdate)

    // --- One session of versicle usage; one flush per event ---
    await runSession(sim, doc, async () => {
      await provider.flush()
    })
    doc.off('update', onUpdate)

    // --- Trim exactly like the provider does when the row count crosses
    //     PREFERRED_TRIM_SIZE (timed; runs on the main thread in browsers) ---
    if (provider._dbsize >= PREFERRED_TRIM_SIZE) {
      const before = await dbRowStats()
      const t1 = performance.now()
      await storeState(provider, false)
      const trimMs = performance.now() - t1
      trimWindows.push([t1, t1 + trimMs])
      const after = await dbRowStats()
      // Bytes the trim actually wrote = rows that exist now but not before
      let written = 0
      for (const [k, len] of after.sizesByKey) {
        if (!before.sizesByKey.has(k)) written += len
      }
      idbBytesWritten += written
      // Full consolidation leaves exactly one row; incremental trims leave
      // the base plus delta rows.
      const mode = after.count === 1 ? 'full' : 'delta'
      trimSamples.push({ session: s + 1, events: sim.totalEvents, trimMs, writtenBytes: written, mode })
    }

    await provider.destroy()

    // --- Epoch metrics ---
    if ((s + 1) % EPOCH_EVERY === 0) {
      const stats = docStructStats(doc)
      const rows = await dbRowStats()

      const tRead = performance.now()
      await readSnapshot(DB)
      const readSnapshotMs = performance.now() - tRead
      const readSnapshotWindow = /** @type {[number, number]} */ ([tRead, tRead + readSnapshotMs])

      const hydrationMs = hydrationSamples.reduce((a, b) => a + b, 0) / hydrationSamples.length
      const lastTrim = trimSamples[trimSamples.length - 1]
      epochs.push({
        session: s + 1,
        events: sim.totalEvents,
        hydrationMs,
        trimMs: lastTrim ? lastTrim.trimMs : 0,
        trimMode: lastTrim ? lastTrim.mode : '-',
        trimWritten: lastTrim ? lastTrim.writtenBytes : 0,
        rows: rows.count,
        dbBytes: rows.totalBytes,
        readSnapshotMs,
        items: stats.items,
        deletedItems: stats.deletedItems,
        dsRanges: stats.dsRanges,
        svClients: stats.svClients,
        writeAmp: idbBytesWritten / Math.max(1, sim.bytesProduced),
        // harness health (GC overlap is filled in after the run: the
        // observer delivers entries asynchronously)
        harnessTx: rows.harnessTx,
        // the current session's doc is still referenced here
        liveDocs: docsCreated - docsCollected - 1,
        heapMB: heapMB(),
        hydrationWindows,
        readSnapshotWindow,
        hydrationGcMs: 0,
        readSnapshotGcMs: 0
      })
      hydrationSamples = []
      hydrationWindows = []
    }
    doc.destroy()
  }
  const runMs = performance.now() - tRun
  // let the observer deliver the last GC entries
  await new Promise(resolve => setTimeout(resolve, 50))
  gcObserver.disconnect()
  for (const e of epochs) {
    e.hydrationGcMs = gcInside(e.hydrationWindows) / e.hydrationWindows.length
    e.readSnapshotGcMs = gcInside([e.readSnapshotWindow])
  }
  const allHydrationWindows = epochs.flatMap(e => e.hydrationWindows)
  const windowMs = (/** @type {Array<[number, number]>} */ w) => w.reduce((a, [x, y]) => a + y - x, 0)
  const health = {
    runMs,
    gcTotalMs: windowMs(gcPauses),
    hydrationMs: windowMs(allHydrationWindows),
    hydrationGcMs: gcInside(allHydrationWindows),
    readSnapshotMs: windowMs(epochs.map(e => e.readSnapshotWindow)),
    readSnapshotGcMs: gcInside(epochs.map(e => e.readSnapshotWindow)),
    trimMs: windowMs(trimWindows),
    trimGcMs: gcInside(trimWindows),
    peakHeapMB: Math.max(0, ...epochs.map(e => e.heapMB)),
    maxRssMB: process.resourceUsage().maxRSS / 1024
  }

  const fullTrims = trimSamples.filter(x => x.mode === 'full')
  const deltaTrims = trimSamples.filter(x => x.mode === 'delta')
  const avg = (arr, sel) => arr.length === 0 ? 0 : arr.reduce((a, b) => a + sel(b), 0) / arr.length
  const xs = epochs.map(e => e.events / 1000)
  const slope = sel => fitSlope(xs, epochs.map(sel))

  if (JSON_OUT) {
    console.log(JSON.stringify({
      sessions: SESSIONS,
      events: sim.totalEvents,
      epochs: epochs.map(({ hydrationWindows, readSnapshotWindow, ...e }) => e),
      health,
      slopes: {
        hydrationMs: slope(e => e.hydrationMs),
        hydrationMsExGc: slope(e => e.hydrationMs - e.hydrationGcMs),
        readSnapshotMs: slope(e => e.readSnapshotMs),
        readSnapshotMsExGc: slope(e => e.readSnapshotMs - e.readSnapshotGcMs)
      }
    }))
    await clearDocument(DB)
    return
  }

  console.log('\n=== Versicle-shaped aging: y-idb costs vs age ===')
  console.log(`sessions=${SESSIONS} events=${sim.totalEvents} trims=${trimSamples.length} (full=${fullTrims.length} delta=${deltaTrims.length}) updateBytes=${fmtBytes(sim.bytesProduced)} idbBytesWritten=${fmtBytes(idbBytesWritten)}`)
  console.log(`avg trim: full=${fmtMs(avg(fullTrims, x => x.trimMs))} (${fmtBytes(Math.round(avg(fullTrims, x => x.writtenBytes)))}) delta=${fmtMs(avg(deltaTrims, x => x.trimMs))} (${fmtBytes(Math.round(avg(deltaTrims, x => x.writtenBytes)))})`)
  console.log('session | events | hydration | trim(mode) | trimWritten | rows | dbBytes | readSnapshot | items(dead) | dsRanges | svClients | writeAmp')
  for (const e of epochs) {
    console.log([
      e.session, e.events, fmtMs(e.hydrationMs), `${fmtMs(e.trimMs)}(${e.trimMode})`, fmtBytes(e.trimWritten),
      e.rows, fmtBytes(e.dbBytes), fmtMs(e.readSnapshotMs),
      `${e.items}(${e.deletedItems})`, e.dsRanges, e.svClients, e.writeAmp.toFixed(2)
    ].join(' | '))
  }

  console.log('\n--- harness health (fake-indexeddb retention; not library costs) ---')
  console.log('session | harnessTx | liveDocs | heap | hydration (of which gc) | readSnapshot (of which gc)')
  for (const e of epochs) {
    console.log([
      e.session, e.harnessTx ?? 'n/a', e.liveDocs, e.heapMB.toFixed(0) + ' MB',
      `${fmtMs(e.hydrationMs)} (${fmtMs(e.hydrationGcMs)})`,
      `${fmtMs(e.readSnapshotMs)} (${fmtMs(e.readSnapshotGcMs)})`
    ].join(' | '))
  }
  const pct = (part, whole) => (100 * part / Math.max(1e-9, whole)).toFixed(0) + '%'
  console.log(`run ${(health.runMs / 1000).toFixed(1)} s, gc ${(health.gcTotalMs / 1000).toFixed(1)} s, peak heap ${health.peakHeapMB.toFixed(0)} MB, max RSS ${health.maxRssMB.toFixed(0)} MB`)
  console.log(`gc inside timed samples: hydration ${pct(health.hydrationGcMs, health.hydrationMs)}, readSnapshot ${pct(health.readSnapshotGcMs, health.readSnapshotMs)}, trim ${pct(health.trimGcMs, health.trimMs)}`)

  console.log('\n--- growth per 1000 events (least-squares slope) ---')
  console.log(`hydration ms:       ${slope(e => e.hydrationMs).toFixed(3)}`)
  console.log(`  excluding gc:     ${slope(e => e.hydrationMs - e.hydrationGcMs).toFixed(3)}`)
  console.log(`trim ms:            ${slope(e => e.trimMs).toFixed(3)}`)
  console.log(`snapshot row bytes: ${fmtBytes(Math.round(slope(e => e.snapBytes)))}`)
  console.log(`readSnapshot ms:    ${slope(e => e.readSnapshotMs).toFixed(3)}`)
  console.log(`  excluding gc:     ${slope(e => e.readSnapshotMs - e.readSnapshotGcMs).toFixed(3)}`)
  console.log(`live items:         ${slope(e => e.items).toFixed(0)}`)
  console.log(`dead items:         ${slope(e => e.deletedItems).toFixed(0)}`)

  await clearDocument(DB)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
