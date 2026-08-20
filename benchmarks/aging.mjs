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
 * Run with: npm run bench
 */
import 'fake-indexeddb/auto'
import * as Y from 'yjs'
import { performance } from 'node:perf_hooks'
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

const fmtBytes = (n) => n >= 1048576 ? (n / 1048576).toFixed(2) + ' MB' : n >= 1024 ? (n / 1024).toFixed(1) + ' KB' : n + ' B'
const fmtMs = (n) => n >= 100 ? n.toFixed(0) + ' ms' : n.toFixed(2) + ' ms'

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
  return { count: rows.length, totalBytes: total, largestBytes: largest, sizesByKey }
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

  for (let s = 0; s < SESSIONS; s++) {
    const doc = new Y.Doc()
    doc.clientID = clientIdForSession(SEED, s)

    // --- Hydration: what a versicle boot pays before data is usable ---
    const t0 = performance.now()
    const provider = new IndexeddbPersistence(DB, doc)
    provider._storeTimeout = 1e9 // trims are timed explicitly below
    await provider.whenSynced
    hydrationSamples.push(performance.now() - t0)

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
        writeAmp: idbBytesWritten / Math.max(1, sim.bytesProduced)
      })
      hydrationSamples = []
    }
    doc.destroy()
  }

  const fullTrims = trimSamples.filter(x => x.mode === 'full')
  const deltaTrims = trimSamples.filter(x => x.mode === 'delta')
  const avg = (arr, sel) => arr.length === 0 ? 0 : arr.reduce((a, b) => a + sel(b), 0) / arr.length
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

  const xs = epochs.map(e => e.events / 1000)
  const slope = sel => fitSlope(xs, epochs.map(sel))
  console.log('\n--- growth per 1000 events (least-squares slope) ---')
  console.log(`hydration ms:       ${slope(e => e.hydrationMs).toFixed(3)}`)
  console.log(`trim ms:            ${slope(e => e.trimMs).toFixed(3)}`)
  console.log(`snapshot row bytes: ${fmtBytes(Math.round(slope(e => e.snapBytes)))}`)
  console.log(`readSnapshot ms:    ${slope(e => e.readSnapshotMs).toFixed(3)}`)
  console.log(`live items:         ${slope(e => e.items).toFixed(0)}`)
  console.log(`dead items:         ${slope(e => e.deletedItems).toFixed(0)}`)

  await clearDocument(DB)
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
