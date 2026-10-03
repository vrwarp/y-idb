/* eslint-env browser */
/**
 * y-idb hydration benchmark: the 'update' encode y-idb's own listener forces
 *
 * Yjs builds a V1 update for EVERY transaction while the doc has an 'update'
 * observer (`cleanupTransactions`: `if (doc._observers.has('update'))`). The
 * provider attaches `doc.on('update', _storeUpdate)` in its constructor, so
 * the hydration transaction (`applyStoredUpdates`: every stored row applied
 * in one `Y.transact`) makes Yjs encode an update as large as the whole
 * loaded document — and `_storeUpdate` drops it immediately because its
 * origin is the provider. When y-idb is the only 'update' listener during
 * hydration (e.g. the app awaits `whenSynced` before constructing its
 * network provider), that O(document) encode and doc-sized allocation are
 * pure waste on every cold start.
 *
 * Ages one versicle-shaped Y.Doc (./versicle-workload.mjs, same seed as
 * ./aging.mjs) through many sessions with the REAL provider on
 * fake-indexeddb, dumps the updates store at checkpoints, then measures a
 * cold start on a copy of each dump:
 *
 *  - deterministic counters (exact, identical on every run):
 *      rows / stored bytes loaded, doc size (encodeStateAsUpdate),
 *      bytes produced by Y.UpdateEncoderV1 during hydration, and the
 *      provider-origin 'update' events Yjs emitted (all discarded)
 *  - timings, median of HYD_REPS interleaved runs (noisy; counters rule):
 *      provider cold start (wall + process CPU), the hydration Y.transact,
 *      and the part of it after 'afterTransactionCleanup' where Yjs encodes
 *      and emits the update — as shipped vs. with the provider's listener
 *      detached for the duration of hydration (what a fix achieves)
 *
 * Run with: node benchmarks/hydration-encode.mjs
 * Env: HYD_CHECKPOINTS (default "60,120,240"; "120,240,480" ages twice
 * as far, ~5 min on a busy 4-CPU box), HYD_REPS (default 9).
 * Add --expose-gc to run a GC before every timed sample.
 */
import 'fake-indexeddb/auto'
import * as Y from 'yjs'
import { performance } from 'node:perf_hooks'
import {
  IndexeddbPersistence, storeState, clearDocument, PREFERRED_TRIM_SIZE
} from '../src/y-idb.js'
import { createSim, runSession, clientIdForSession } from './versicle-workload.mjs'

const SEED = 20260820
const CHECKPOINTS = (process.env.HYD_CHECKPOINTS || '60,120,240').split(',').map(Number).sort((a, b) => a - b)
const REPS = Number(process.env.HYD_REPS || 9)
const AGING_DB = 'versicle-hydration-encode-aging'
const MEASURE_DB = 'versicle-hydration-encode-measure'

const fmtBytes = (n) => n >= 1048576 ? (n / 1048576).toFixed(2) + ' MB' : n >= 1024 ? (n / 1024).toFixed(1) + ' KB' : n + ' B'
const fmtMs = (n) => n >= 100 ? n.toFixed(0) + ' ms' : n.toFixed(1) + ' ms'
const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b)
  return s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2
}
const gc = () => { if (typeof globalThis.gc === 'function') globalThis.gc() }
const cpuMs = () => {
  const u = process.cpuUsage()
  return (u.user + u.system) / 1000
}

const openDb = (name) => new Promise((resolve, reject) => {
  const req = indexedDB.open(name)
  req.onupgradeneeded = () => {
    req.result.createObjectStore('updates', { autoIncrement: true })
    req.result.createObjectStore('custom')
  }
  req.onsuccess = () => resolve(req.result)
  req.onerror = () => reject(req.error)
})

/** Keys and values of one store, in key order. */
const readStore = async (name, store) => {
  const db = await openDb(name)
  const out = await new Promise((resolve, reject) => {
    const tx = db.transaction([store], 'readonly')
    const os = tx.objectStore(store)
    const ks = os.getAllKeys()
    const vs = os.getAll()
    tx.oncomplete = () => resolve({ keys: ks.result, values: vs.result })
    tx.onerror = () => reject(tx.error)
  })
  db.close()
  return out
}

/**
 * Delete `name` and recreate it holding exactly `stores` (store name ->
 * { keys, values }; keys omitted = auto-generated).
 */
const recreateWith = async (name, stores) => {
  await clearDocument(name)
  const db = await openDb(name)
  await new Promise((resolve, reject) => {
    const tx = db.transaction(['updates', 'custom'], 'readwrite')
    for (const store in stores) {
      const { keys, values } = stores[store]
      const os = tx.objectStore(store)
      values.forEach((v, i) => keys ? os.put(v, keys[i]) : os.add(v))
    }
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error)
  })
  db.close()
}

/**
 * fake-indexeddb (6.x) never drops finished transactions from its
 * per-database list, and each one retains its requests' handlers -> the
 * provider -> its Y.Doc: without this, an aging run keeps every session's
 * document alive (gigabytes by a few hundred sessions; the cgroup OOM-kills
 * it) and every transaction gets slower (scheduling filters the whole
 * list). Recreating the database after every session through the public
 * API, with the same keys and records, drops that list; y-idb sees
 * identical state (row keys, trim bookkeeping, key generator at max key +
 * 1). The helpers are separate functions so no closure of the new database
 * captures the old connection.
 */
const recreateDb = async (name) => {
  const updates = await readStore(name, 'updates')
  const custom = await readStore(name, 'custom')
  await recreateWith(name, { updates, custom })
}

/**
 * Count the V1 update bytes Yjs produces while `fn` runs. UpdateEncoderV1
 * inherits toUint8Array from DSEncoderV1; an own override on its prototype
 * counts update encodes only (not state vectors).
 */
const countV1Encodes = async (fn) => {
  const proto = Y.UpdateEncoderV1.prototype
  const inherited = proto.toUint8Array
  const counter = { calls: 0, bytes: 0 }
  proto.toUint8Array = function () {
    const out = inherited.call(this)
    counter.calls++
    counter.bytes += out.byteLength
    return out
  }
  try {
    await fn()
  } finally {
    delete proto.toUint8Array
  }
  return counter
}

/**
 * Cold start: fresh Y.Doc, new provider, await whenSynced.
 *
 * Also times two synchronous phases of the hydration transaction (origin
 * === provider) via doc events, which do not affect the 'update' gate:
 * apply = 'beforeTransaction' -> 'afterAllTransactions' (the whole
 * `Y.transact` in applyStoredUpdates), and update = 'afterTransactionCleanup'
 * -> 'afterAllTransactions' (where Yjs encodes and emits the V1 update).
 *
 * @param {'shipped'|'detached'} variant 'detached' takes the provider's
 *   'update' listener off for the duration of hydration (nothing else edits
 *   the doc meanwhile), i.e. what the proposed fix achieves.
 */
const coldStart = async (variant, onDoc) => {
  const doc = new Y.Doc()
  if (onDoc) onDoc(doc)
  const phase = { applyMs: 0, updateMs: 0 }
  let provider = null
  let tApply = 0
  let tCleanup = 0
  doc.on('beforeTransaction', tr => { if (tr.origin === provider) tApply = performance.now() })
  doc.on('afterTransactionCleanup', tr => { if (tr.origin === provider) tCleanup = performance.now() })
  doc.on('afterAllTransactions', () => {
    if (tApply === 0) return
    const now = performance.now()
    phase.applyMs += now - tApply
    phase.updateMs += now - tCleanup
    tApply = tCleanup = 0
  })
  provider = new IndexeddbPersistence(MEASURE_DB, doc)
  if (variant === 'detached') {
    doc.off('update', provider._storeUpdate)
    provider.whenSynced.then(() => doc.on('update', provider._storeUpdate))
  }
  await provider.whenSynced
  return { doc, provider, phase }
}

const ageAndDump = async () => {
  await clearDocument(AGING_DB)
  const sim = createSim({ seed: SEED })
  const dumps = new Map()
  const last = CHECKPOINTS[CHECKPOINTS.length - 1]
  for (let s = 0; s < last; s++) {
    const doc = new Y.Doc()
    doc.clientID = clientIdForSession(SEED, s)
    const provider = new IndexeddbPersistence(AGING_DB, doc)
    provider._storeTimeout = 1e9
    await provider.whenSynced
    await runSession(sim, doc, () => provider.flush())
    if (provider._dbsize >= PREFERRED_TRIM_SIZE) await storeState(provider, false)
    await provider.destroy()
    doc.destroy()
    await recreateDb(AGING_DB)
    if (CHECKPOINTS.includes(s + 1)) {
      dumps.set(s + 1, { rows: (await readStore(AGING_DB, 'updates')).values, events: sim.totalEvents })
      gc()
      process.stderr.write(`aged ${s + 1} sessions (heap ${(process.memoryUsage().heapUsed / 1048576).toFixed(0)} MB)\n`)
    }
  }
  await clearDocument(AGING_DB)
  return dumps
}

const measure = async (sessions, { rows, events }) => {
  // A fresh copy of the dump before every cold start (see recreateDb).
  const restore = () => recreateWith(MEASURE_DB, { updates: { values: rows } })
  await restore()
  const storedBytes = rows.reduce((a, r) => a + r.byteLength, 0)

  // --- Deterministic counters: one cold start, y-idb the only listener ---
  const emitted = []
  let provider = null
  let doc = null
  const enc = await countV1Encodes(async () => {
    ({ doc, provider } = await coldStart('shipped', d => {
      const emit = d.emit
      // Instance wrapper: sees what Yjs emits without adding an observer
      // (adding one would itself switch the encode on).
      d.emit = function (name, args) {
        if (name === 'update') emitted.push({ origin: args[1], bytes: args[0].byteLength })
        return emit.call(this, name, args)
      }
    }))
  })
  const self = emitted.filter(e => e.origin === provider)
  const selfEvents = self.length
  const selfBytes = self.reduce((a, e) => a + e.bytes, 0)
  const queued = provider._pendingUpdates.length
  await provider.destroy()
  const docBytes = Y.encodeStateAsUpdate(doc).byteLength
  doc.destroy()
  const after = await readStore(MEASURE_DB, 'updates')
  if (after.values.length !== rows.length) throw new Error('measurement cold start wrote rows')

  // --- Timings, interleaved ---
  const t = {}
  const push = (k, v) => { (t[k] = t[k] || []).push(v) }
  for (let r = 0; r < REPS; r++) {
    const order = r % 2 === 0 ? ['shipped', 'detached'] : ['detached', 'shipped']
    for (const variant of order) {
      await restore()
      gc()
      const c0 = cpuMs()
      const w0 = performance.now()
      const { doc, provider, phase } = await coldStart(variant)
      const w1 = performance.now()
      const c1 = cpuMs()
      push(variant + 'Wall', w1 - w0)
      push(variant + 'Cpu', c1 - c0)
      push(variant + 'Apply', phase.applyMs)
      push(variant + 'Update', phase.updateMs)
      await provider.destroy()
      doc.destroy()
    }
  }
  const m = {}
  for (const k in t) m[k] = median(t[k])
  return { sessions, events, rows: rows.length, storedBytes, docBytes, enc, selfEvents, selfBytes, queued, m }
}

const main = async () => {
  const dumps = await ageAndDump()
  const results = []
  for (const [sessions, dump] of dumps) results.push(await measure(sessions, dump))
  await clearDocument(MEASURE_DB)

  console.log('\n=== y-idb cold start: encode forced by the provider\'s own \'update\' listener ===')
  console.log(`seed=${SEED} reps=${REPS} (timings: medians, interleaved${typeof globalThis.gc === 'function' ? ', gc before each sample' : ''})`)
  console.log('\n-- deterministic counters (one cold start, y-idb the only update listener) --')
  console.log('sessions | events | rows | stored bytes | doc bytes | V1 encodes during hydration | provider-origin update events | queued by _storeUpdate')
  for (const x of results) {
    console.log([
      x.sessions, x.events, x.rows, fmtBytes(x.storedBytes), fmtBytes(x.docBytes),
      `${x.enc.calls} (${fmtBytes(x.enc.bytes)} = ${(100 * x.enc.bytes / x.docBytes).toFixed(0)}% of doc)`,
      `${x.selfEvents} (${fmtBytes(x.selfBytes)}, discarded)`, x.queued
    ].join(' | '))
  }
  console.log('\n-- timings (shipped -> listener detached during hydration) --')
  console.log('sessions | cold start wall | cold start CPU | hydration Y.transact | of which update encode+emit')
  for (const x of results) {
    const { m } = x
    const pct = (a, b) => `${b >= a ? '+' : ''}${(100 * (b - a) / a).toFixed(0)}%`
    const ab = (k) => `${fmtMs(m['shipped' + k])} -> ${fmtMs(m['detached' + k])} (${pct(m['shipped' + k], m['detached' + k])})`
    console.log([
      x.sessions, ab('Wall'), ab('Cpu'), ab('Apply'),
      `${fmtMs(m.shippedUpdate)} -> ${fmtMs(m.detachedUpdate)} (${(100 * m.shippedUpdate / m.shippedCpu).toFixed(0)}% of shipped cold-start CPU)`
    ].join(' | '))
  }
}

main().catch(err => {
  console.error(err)
  process.exit(1)
})
