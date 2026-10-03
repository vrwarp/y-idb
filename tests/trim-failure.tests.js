/* eslint-env browser */

/**
 * Performance regression tests: a trim that keeps failing must back off.
 *
 * When a trim transaction aborts (realistically a QuotaExceededError at
 * commit when the origin is near its quota), restoreCursorOnAbort puts
 * `_dbref`/`_dbsize` back and nothing else records the failure. `_dbsize`
 * is still >= PREFERRED_TRIM_SIZE, so the oncomplete of the very next flush
 * arms another trim `_storeTimeout` later, and that retry redoes all of it:
 * re-read and re-apply every row after the restored cursor, re-encode the
 * whole document (full consolidation) and offer the O(document) row again.
 * Nothing commits, so the backlog grows: one O(doc + rows) attempt per write
 * event, O(T^2) work over a failure that lasts T.
 *
 * The provider's own timers run on a virtual clock (setTimeout,
 * clearTimeout and Date.now are replaced; fake-indexeddb schedules with
 * setImmediate and is unaffected), so the unmodified `_storeTimeout` arming
 * is exercised deterministically and costs are pinned with counters only:
 * trim transactions opened, rows they re-read, bytes they offered to add().
 */

import * as Y from 'yjs'
import * as idb from 'lib0/indexeddb'
import { IndexeddbPersistence, clearDocument, storeState, readSnapshot, PREFERRED_TRIM_SIZE } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * Rows a row-count trim leaves room for; seeded so the first flush crosses
 * PREFERRED_TRIM_SIZE.
 */
const SEED_ROWS = PREFERRED_TRIM_SIZE + 20

/**
 * Simulated quota: a readwrite transaction that adds a row larger than this
 * aborts at commit. Flush rows (~20 B) fit; the consolidated document of the
 * seeded rows (~60 KB) does not.
 */
const QUOTA_ROW_LIMIT = 16 * 1024

const requestMethods = /** @type {const} */ ([
  'add', 'put', 'delete', 'clear', 'get', 'getKey', 'getAll', 'getAllKeys', 'count', 'openCursor', 'openKeyCursor'
])

const tick = () => new Promise(resolve => setImmediate(resolve))

/**
 * Writes SEED_ROWS small updates (one distinct map key each) straight into
 * the updates store, as earlier sessions would have flushed them.
 *
 * @param {string} name
 */
const seedRows = async name => {
  const gen = new Y.Doc()
  /** @type {Array<Uint8Array>} */
  const rows = []
  gen.on('update', /** @param {Uint8Array} u */ u => { rows.push(u) })
  for (let i = 0; i < SEED_ROWS; i++) {
    gen.getMap('m').set('k' + i, 'v'.repeat(100) + i)
  }
  gen.destroy()
  const db = await idb.openDB(name, db => idb.createStores(db, [
    ['updates', { autoIncrement: true }],
    ['custom']
  ]))
  await new Promise((resolve, reject) => {
    const tx = db.transaction(['updates'], 'readwrite')
    const store = tx.objectStore('updates')
    rows.forEach(row => store.add(row))
    tx.oncomplete = () => resolve(undefined)
    tx.onerror = () => reject(tx.error)
  })
  db.close()
}

/**
 * @typedef {object} TrimCounters
 * @property {number} trims Trim transactions opened (the only ones over
 * ['custom', 'updates'])
 * @property {number} committed
 * @property {number} aborted
 * @property {number} rowsRead Rows returned to trims by getAll
 * @property {number} bytesOffered Bytes trims passed to add()
 */

/**
 * Runs `fn` with IndexedDB instrumented and the provider's timers on a
 * virtual clock.
 *
 * - Counts trim work (see {@link TrimCounters}).
 * - `quota.limit`: a readwrite transaction that adds a row larger than this
 *   is aborted when it would otherwise commit (every request succeeded and
 *   no further one was issued), like a commit-time QuotaExceededError.
 * - `advance(ms)`: moves virtual time forward, firing due timers in order
 *   and letting the IndexedDB work each one starts settle.
 * - `settle()`: waits until no transaction is in flight.
 * - `pendingTimers()`: number of armed virtual timers.
 *
 * @param {function({ c: TrimCounters, quota: { limit: number }, advance: function(number):Promise<void>, settle: function():Promise<void>, pendingTimers: function():number }):Promise<void>} fn
 */
const withInstrumentedClock = async fn => {
  const realTransaction = IDBDatabase.prototype.transaction
  const proto = /** @type {any} */ (IDBObjectStore.prototype)
  const realMethods = requestMethods.map(m => proto[m])
  const realSetTimeout = globalThis.setTimeout
  const realClearTimeout = globalThis.clearTimeout
  const realDateNow = Date.now
  /** @type {TrimCounters} */
  const c = { trims: 0, committed: 0, aborted: 0, rowsRead: 0, bytesOffered: 0 }
  const quota = { limit: Infinity }
  /** @type {Set<IDBTransaction>} */
  const live = new Set()
  /** @type {Set<IDBTransaction>} */
  const trimTxs = new Set()
  /** @type {Map<IDBTransaction, { requests: Array<IDBRequest>, aborted: boolean }>} */
  const doomed = new Map()

  // @ts-ignore - override the prototype to observe every transaction
  IDBDatabase.prototype.transaction = function (storeNames, mode, options) {
    const tx = realTransaction.call(this, storeNames, mode, options)
    live.add(tx)
    const names = Array.isArray(storeNames) ? storeNames : [storeNames]
    if (mode === 'readwrite' && names.length === 2 && names.includes('custom') && names.includes('updates')) {
      c.trims++
      trimTxs.add(tx)
    }
    /**
     * @param {boolean} committed
     */
    const done = committed => {
      live.delete(tx)
      doomed.delete(tx)
      if (trimTxs.delete(tx)) {
        if (committed) c.committed++
        else c.aborted++
      }
    }
    tx.addEventListener('complete', () => done(true))
    tx.addEventListener('abort', () => done(false))
    return tx
  }
  requestMethods.forEach((m, i) => {
    /**
     * @this {IDBObjectStore}
     * @param {...any} args
     */
    proto[m] = function (...args) {
      const req = /** @type {IDBRequest} */ (realMethods[i].apply(this, args))
      const tx = this.transaction
      if (trimTxs.has(tx)) {
        if (m === 'add') c.bytesOffered += args[0].byteLength
        if (m === 'getAll' && this.name === 'updates') {
          req.addEventListener('success', () => { c.rowsRead += req.result.length })
        }
      }
      if (m === 'add' && args[0].byteLength > quota.limit && !doomed.has(tx)) {
        doomed.set(tx, { requests: [], aborted: false })
      }
      const d = doomed.get(tx)
      if (d) {
        d.requests.push(req)
        req.addEventListener('success', () => {
          // Runs after the success callbacks' microtasks (which may issue
          // the next request) and before fake-indexeddb's next transaction
          // step: abort only when the transaction would otherwise commit.
          // The callers' `aborted === trims` precondition catches a miss.
          setImmediate(() => {
            if (!d.aborted && d.requests.every(r => r.readyState === 'done')) {
              d.aborted = true
              try { tx.abort() } catch (e) {}
            }
          })
        })
      }
      return req
    }
  })

  let now = 0
  let seq = 0
  const base = realDateNow()
  /** @type {Map<object, { due: number, seq: number, fn: function(...any):void, args: Array<any> }>} */
  const timers = new Map()
  /**
   * @param {function(...any):void} fn
   * @param {number} [ms]
   * @param {...any} args
   */
  const virtualSetTimeout = (fn, ms, ...args) => {
    const id = { virtualTimer: ++seq }
    timers.set(id, { due: now + Math.max(0, Number(ms) || 0), seq, fn, args })
    return id
  }
  /**
   * @param {any} id
   */
  const virtualClearTimeout = id => { timers.delete(id) }
  globalThis.setTimeout = /** @type {any} */ (virtualSetTimeout)
  globalThis.clearTimeout = /** @type {any} */ (virtualClearTimeout)
  Date.now = () => base + now

  /** Waits until no transaction is in flight and promise chains settled. */
  const quiesce = async () => {
    for (let idle = 0; idle < 3;) {
      await tick()
      idle = live.size === 0 ? idle + 1 : 0
    }
  }
  /**
   * @param {number} ms
   */
  const advance = async ms => {
    const target = now + ms
    for (;;) {
      /** @type {[object, { due: number, seq: number, fn: function(...any):void, args: Array<any> }] | null} */
      let next = null
      for (const entry of timers) {
        const tm = entry[1]
        if (tm.due <= target && (next === null || tm.due < next[1].due || (tm.due === next[1].due && tm.seq < next[1].seq))) next = entry
      }
      if (next === null) break
      timers.delete(next[0])
      now = next[1].due
      next[1].fn(...next[1].args)
      await quiesce()
    }
    now = target
  }
  try {
    await fn({ c, quota, advance, settle: quiesce, pendingTimers: () => timers.size })
  } finally {
    IDBDatabase.prototype.transaction = realTransaction
    requestMethods.forEach((m, i) => { proto[m] = realMethods[i] })
    globalThis.setTimeout = realSetTimeout
    globalThis.clearTimeout = realClearTimeout
    Date.now = realDateNow
  }
}

/**
 * Edit once and flush, then let `intervalMs` of virtual time pass (firing
 * the trim timer the flush armed, if any). One call = one write event.
 *
 * @param {IndexeddbPersistence} provider
 * @param {function(number):Promise<void>} advance
 * @param {number} i
 * @param {number} intervalMs
 */
const writeEvent = async (provider, advance, i, intervalMs) => {
  provider.doc.getMap('m').set('e' + i, i)
  await provider.flush()
  await advance(intervalMs)
}

/**
 * Persistent trim failure during active editing: 150 write events, 2 s of
 * virtual time apart (5 minutes), each flushed. The flushes commit, every
 * trim attempt aborts at commit.
 *
 * Unfixed, every flush re-arms the trim 1 s later: 150 attempts, each
 * re-encoding the whole document (~60 KB here) and re-reading every row
 * flushed since hydration, so attempts grow linearly and re-read rows
 * quadratically with the failure's duration. With exponential backoff the
 * same 5 minutes cost O(log T) attempts (about 8-14 for a base of
 * `_storeTimeout` and a cap between 30 s and a few minutes); the bounds
 * below leave room for any such schedule.
 *
 * @param {t.TestCase} tc
 */
export const testFailedTrimIsNotRetriedOnEveryFlush = async tc => {
  const name = tc.testName
  await clearDocument(name)
  await seedRows(name)
  const EVENTS = 150
  const INTERVAL_MS = 2000
  const MAX_ATTEMPTS = 30
  await withInstrumentedClock(async ({ c, quota, advance }) => {
    const doc = new Y.Doc()
    const provider = new IndexeddbPersistence(name, doc)
    try {
      await provider.whenSynced
      t.assert(provider._dbsize === SEED_ROWS, 'precondition: hydration counted the seeded rows')
      const fullStateBytes = Y.encodeStateAsUpdate(doc).byteLength
      t.assert(fullStateBytes > QUOTA_ROW_LIMIT, 'precondition: the consolidated row exceeds the simulated quota')
      let errors = 0
      provider.on('error', () => { errors++ })
      quota.limit = QUOTA_ROW_LIMIT

      for (let i = 0; i < EVENTS; i++) {
        await writeEvent(provider, advance, i, INTERVAL_MS)
      }

      t.assert(c.trims > 0, 'precondition: the store crossed PREFERRED_TRIM_SIZE and a trim was attempted')
      t.assert(c.committed === 0 && c.aborted === c.trims, `precondition: every trim attempt aborted (committed=${c.committed}, aborted=${c.aborted}, attempts=${c.trims})`)
      const summary = `${EVENTS} write events ${INTERVAL_MS} ms apart started ${c.trims} trim attempts (all aborted, ${errors} 'error' events), offered ${c.bytesOffered} bytes (${(c.bytesOffered / fullStateBytes).toFixed(1)} full-document encodes of ${fullStateBytes} B) and re-read ${c.rowsRead} rows`
      t.assert(c.trims <= MAX_ATTEMPTS, `a failed trim must back off instead of being re-armed by every later flush: ${summary}; expected <= ${MAX_ATTEMPTS} attempts`)
      t.assert(c.bytesOffered <= MAX_ATTEMPTS * fullStateBytes * 1.1, `failed full consolidations re-encode and re-offer the whole document on every flush: ${summary}`)
      t.assert(c.rowsRead <= MAX_ATTEMPTS * EVENTS, `each failed trim re-reads every row flushed since the last successful one: ${summary}; expected <= ${MAX_ATTEMPTS * EVENTS} rows`)
    } finally {
      quota.limit = Infinity
      await provider.destroy()
    }
  })
}

/**
 * Guard for the fix (passes on the unfixed code too): once the failure
 * clears, a trim still runs and commits under continued editing (one edit
 * every 10 s, within 20 minutes of virtual time), an explicit
 * `storeState(provider, true)` is not held back by any backoff, no data is
 * lost, and destroy() leaves no timer armed.
 *
 * @param {t.TestCase} tc
 */
export const testTrimRecoversOnceFailureClears = async tc => {
  const name = tc.testName
  await clearDocument(name)
  await seedRows(name)
  await withInstrumentedClock(async ({ c, quota, advance, settle, pendingTimers }) => {
    const doc = new Y.Doc()
    const provider = new IndexeddbPersistence(name, doc)
    let destroyed = false
    try {
      await provider.whenSynced
      quota.limit = QUOTA_ROW_LIMIT
      let i = 0
      for (; i < 60; i++) {
        await writeEvent(provider, advance, i, 2000)
      }
      t.assert(c.trims > 0 && c.committed === 0, 'precondition: trims were attempted and all failed')

      // The failure clears; keep editing.
      quota.limit = Infinity
      for (let k = 0; k < 120 && c.committed === 0; k++, i++) {
        await writeEvent(provider, advance, i, 10_000)
      }
      t.assert(c.committed > 0, 'a trim must run and commit within 20 minutes of the failure clearing')
      t.assert(provider._dbsize < PREFERRED_TRIM_SIZE, `the committed trim shrinks the store (rows=${provider._dbsize})`)

      // An explicit consolidation right after a failed trim is not deferred.
      for (let k = 0; k < PREFERRED_TRIM_SIZE && provider._dbsize < PREFERRED_TRIM_SIZE; k++) {
        await writeEvent(provider, advance, i++, 0)
      }
      // The next trim is incremental (its merged tail of tiny edit rows is a
      // few KB): lower the simulated quota so that it fails too.
      quota.limit = 1024
      const failedBefore = c.aborted
      await writeEvent(provider, advance, i++, 2000)
      t.assert(c.aborted === failedBefore + 1, 'precondition: a trim failed again')
      quota.limit = Infinity
      const committedBefore = c.committed
      await storeState(provider, true)
      await settle()
      t.assert(c.committed === committedBefore + 1 && provider._dbsize === 1, `storeState(provider, true) consolidates at once (committed ${c.committed - committedBefore}, rows=${provider._dbsize})`)

      await provider.destroy()
      destroyed = true
      t.assert(pendingTimers() === 0, `destroy() must clear every timer, including a trim backoff (${pendingTimers()} left)`)

      const snapshot = await readSnapshot(name)
      const reloaded = new Y.Doc()
      Y.applyUpdate(reloaded, /** @type {Uint8Array} */ (snapshot))
      const m = reloaded.getMap('m')
      t.assert(m.get('k0') === 'v'.repeat(100) + 0 && m.get('k' + (SEED_ROWS - 1)) === 'v'.repeat(100) + (SEED_ROWS - 1), 'seeded rows survive')
      t.assert(m.get('e0') === 0 && m.get('e' + (i - 1)) === i - 1, 'edits made during and after the failure survive')
    } finally {
      quota.limit = Infinity
      if (!destroyed) await provider.destroy()
    }
  })
}

/**
 * Each failed trim counts once towards the backoff, however it fails: a
 * failed request rejects the trim's promise chain AND aborts its
 * transaction, and a failing transactionRunner rejects without opening one.
 * After one failure the next trim is armed 2 x `_storeTimeout` after the
 * flush, after two 4 x, and the trim that commits resets the backoff.
 *
 * @param {t.TestCase} tc
 */
export const testFailedTrimCountsOncePerAttempt = async tc => {
  const name = tc.testName
  await clearDocument(name)
  await seedRows(name)
  await withInstrumentedClock(async ({ c, advance }) => {
    let rejectRunner = false
    let runs = 0
    /**
     * @template T
     * @param {() => Promise<T>} work
     * @return {Promise<T>}
     */
    const runner = work => {
      runs++
      return rejectRunner ? Promise.reject(new Error('runner rejects the trim')) : work()
    }
    const doc = new Y.Doc()
    const provider = new IndexeddbPersistence(name, doc, { transactionRunner: runner })
    const instrumentedAdd = IDBObjectStore.prototype.add
    try {
      await provider.whenSynced
      const timeout = provider._storeTimeout
      let errors = 0
      provider.on('error', () => { errors++ })

      // A request error: the trim's add re-uses key 1, which exists.
      let failAdd = true
      /**
       * @this {IDBObjectStore}
       * @param {any} value
       * @param {IDBValidKey} [key]
       */
      IDBObjectStore.prototype.add = function (value, key) {
        if (failAdd && this.transaction.objectStoreNames.contains('custom')) {
          failAdd = false
          return instrumentedAdd.call(this, value, 1)
        }
        return key === undefined ? instrumentedAdd.call(this, value) : instrumentedAdd.call(this, value, key)
      }
      await writeEvent(provider, advance, 0, timeout)
      IDBObjectStore.prototype.add = instrumentedAdd
      t.assert(c.trims === 1 && c.aborted === 1 && errors === 1, `precondition: the trim failed on a request error (attempts=${c.trims}, aborted=${c.aborted}, errors=${errors})`)
      t.assert(provider._trimFailures === 1, `a request error rejects and aborts, but counts as one failure (got ${provider._trimFailures})`)

      // A runner rejection, armed after twice the timeout.
      await writeEvent(provider, advance, 1, 0)
      rejectRunner = true
      const runsBefore = runs
      await advance(2 * timeout - 1)
      t.assert(runs === runsBefore, 'after one failure the next trim waits twice the timeout')
      await advance(1)
      rejectRunner = false
      t.assert(runs === runsBefore + 1 && errors === 2, `precondition: the trim's runner rejected (runs=${runs - runsBefore}, errors=${errors})`)
      t.assert(provider._trimFailures === 2, `a runner rejection counts as one failure (got ${provider._trimFailures})`)

      // The next trim, armed after four times the timeout, commits.
      await writeEvent(provider, advance, 2, 0)
      await advance(4 * timeout - 1)
      t.assert(c.trims === 1, 'after two failures the next trim waits four times the timeout')
      await advance(1)
      t.assert(c.trims === 2 && c.committed === 1 && provider._dbsize < PREFERRED_TRIM_SIZE, `precondition: the trim committed (attempts=${c.trims}, committed=${c.committed}, rows=${provider._dbsize})`)
      t.assert(provider._trimFailures === 0, `a committed trim resets the backoff (got ${provider._trimFailures})`)
    } finally {
      IDBObjectStore.prototype.add = instrumentedAdd
      await provider.destroy()
    }
  })
}
