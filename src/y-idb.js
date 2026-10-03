/* eslint-env browser */

import * as Y from 'yjs'
import * as idb from 'lib0/indexeddb'
import * as promise from 'lib0/promise'
import { Observable } from 'lib0/observable'

const customStoreName = 'custom'
const updatesStoreName = 'updates'

export const PREFERRED_TRIM_SIZE = 500

/**
 * Maximum number of rows the tiered trim leaves in the updates store before
 * a full consolidation is forced. Incremental trims merge each tail of
 * fresh updates into ONE delta row (O(new updates) work), so between full
 * consolidations the store holds: 1 base row + up to this many delta rows.
 */
export const MAX_SEGMENT_ROWS = 24

/**
 * Delta rows are allowed to accumulate up to max(base row size, this many
 * bytes) before a full consolidation is forced. Bounds the database size at
 * roughly 2x the consolidated document size.
 */
export const MIN_FULL_COMPACT_BYTES = 1_048_576

/**
 * While trims keep failing, the trim a flush arms waits `_storeTimeout` *
 * 2^failures instead of `_storeTimeout`, with the exponent capped here:
 * at most 256 s (about 4 minutes) for the default 1 s timeout.
 */
const MAX_TRIM_BACKOFF_EXPONENT = 8

/**
 * Cap on the backoff between automatic flush retries. It keeps doubling past
 * `maxRetries` (when 'retry-exhausted' is emitted) up to this, and stays
 * there until a write commits: every attempt offers the whole backlog, so
 * retrying any faster while the database keeps failing makes the work grow
 * quadratically with the length of the failure.
 */
export const MAX_RETRY_BACKOFF_MS = 30_000

/**
 * IDB request to promise. Used instead of lib0's request wrappers, which
 * reject with `new Error(domException)` (name 'Error', the real name folded
 * into the message) or, for cursors, with the raw error Event. This rejects
 * with the request's own DOMException, so 'error' listeners see the real
 * name (e.g. QuotaExceededError) on every path, as they do on the flush path.
 *
 * @param {IDBRequest} request
 * @return {Promise<any>}
 */
const rtop = request => promise.create((resolve, reject) => {
  request.onerror = () => reject(/** @type {DOMException} */ (request.error))
  request.onsuccess = () => resolve(request.result)
})

/**
 * @param {IDBObjectStore} store
 * @return {Promise<any>} The highest key, or null if the store is empty
 */
const getLastKey = store =>
  rtop(store.openKeyCursor(null, 'prev')).then(cursor => cursor === null ? null : cursor.key)

/**
 * @param {IDBObjectStore} store
 * @param {IDBKeyRange} range
 * @return {Promise<Array<{ k: any, v: any }>>}
 */
const getAllKeysValues = (store, range) =>
  promise.all([rtop(store.getAllKeys(range)), rtop(store.getAll(range))])
    .then(([ks, vs]) => ks.map((/** @type {any} */ k, /** @type {number} */ i) => ({ k, v: vs[i] })))

/**
 * Run `work` through `transactionRunner` when one is supplied. Callers attach
 * their failure handling to the returned promise, so a runner that throws
 * synchronously (e.g. a non-async wrapper around a disposed lock) is turned
 * into a rejection instead of bypassing it — in `_flush` that would lose the
 * detached batch and leave `_writing` stuck forever.
 *
 * @template T
 * @param {(<U>(work: () => Promise<U>) => Promise<U>)|undefined} transactionRunner
 * @param {() => Promise<T>} work
 * @return {Promise<T>}
 */
const runTransaction = (transactionRunner, work) => {
  if (!transactionRunner) {
    return work()
  }
  try {
    return Promise.resolve(transactionRunner(work))
  } catch (e) {
    return Promise.reject(e)
  }
}

/**
 * @template T
 * @param {IndexeddbPersistence} idbPersistence
 * @param {() => Promise<T>} work
 * @return {Promise<T>}
 */
const transactWrite = (idbPersistence, work) => runTransaction(idbPersistence.transactionRunner, work)

/**
 * `_dbref` and `_dbsize` are advanced from request callbacks, before the
 * transaction commits. If it aborts instead (a failed request, or a
 * QuotaExceededError / background kill at commit), IndexedDB discards its
 * rows AND reverts the autoIncrement key generator, so the next row any tab
 * writes re-uses a key this provider already counts as applied: it would
 * never be fetched, and the next full consolidation would delete it.
 * Restore the values the transaction started from.
 *
 * Call it from the transaction's first request callback: every earlier
 * transaction on the store has finished by then (and run its own restore),
 * so the snapshot holds committed values only. Read `_dbref` for the
 * transaction's catch-up read there too, not when the transaction is
 * created: an earlier trim or fetch of this provider may still be running
 * then (a serializing transactionRunner releases its lock on the work's
 * last request callback, before commit), and if it aborts, a read bounded
 * by the value it advanced to skips the row another tab writes at the
 * reverted key.
 *
 * @param {IndexeddbPersistence} idbPersistence
 * @param {IDBTransaction} tx
 */
const restoreCursorOnAbort = (idbPersistence, tx) => {
  const dbref = idbPersistence._dbref
  const dbsize = idbPersistence._dbsize
  tx.addEventListener('abort', () => {
    idbPersistence._dbref = dbref
    idbPersistence._dbsize = dbsize
  })
}

/**
 * Move `_dbref` past a batch of rows this provider has just written itself
 * (buffered doc updates, already in the doc), so the next trim's catch-up
 * read does not fetch and re-apply them.
 *
 * Call it from the transaction's 'complete' handler only, never from a
 * request callback: an aborted transaction discards its rows and reverts
 * the key generator, so another tab re-uses their keys (see
 * restoreCursorOnAbort).
 *
 * The adds of one readwrite transaction get consecutive autoIncrement keys
 * (no other transaction on the store runs in between), so a first key equal
 * to `_dbref` proves no unseen row sits between the rows already applied
 * and the batch. Otherwise (another tab wrote first, or the key generator
 * is ahead of `_dbref`, e.g. on a fresh store whose keys start at 1)
 * `_dbref` stays put and the next trim reads the batch again.
 *
 * @param {IndexeddbPersistence} idbPersistence
 * @param {Array<IDBRequest<IDBValidKey>>} requests The batch's add() requests, in order
 */
const advanceCursorPastOwnRows = (idbPersistence, requests) => {
  if (requests.length > 0 && requests[0].result === idbPersistence._dbref) {
    idbPersistence._dbref = /** @type {number} */ (requests[requests.length - 1].result) + 1
  }
}

/**
 * Add a batch of buffered updates to the updates store, in order, in the
 * readwrite transaction `newTransaction` opens.
 *
 * add() throws synchronously when an update cannot be cloned, e.g. because
 * another 'update' listener transferred its buffer (`postMessage(update,
 * [update.buffer])`) while it was buffered. That does not abort the
 * transaction: the adds before it would commit unaccounted for, and every
 * retry of the batch would stop at the same update. Skipping the update is
 * no way out either, as Yjs cannot apply this client's later updates
 * without it. So that transaction is aborted and the doc's state, which
 * covers every buffered update, is written in a new one instead. It
 * replaces `batch` in place (a failure handler re-buffers it), and the
 * caller reports `addError`.
 *
 * Throws, with nothing written, when a transaction cannot be opened.
 *
 * @param {IndexeddbPersistence} idbPersistence
 * @param {() => IDBTransaction} newTransaction
 * @param {Array<Uint8Array>} batch
 * @return {{ tx: IDBTransaction, requests: Array<IDBRequest<IDBValidKey>>, addError: any }}
 */
const addBatch = (idbPersistence, newTransaction, batch) => {
  const tx = newTransaction()
  const store = tx.objectStore(updatesStoreName)
  try {
    return { tx, requests: batch.map(update => store.add(update)), addError: null }
  } catch (addError) {
    tx.abort()
    batch.splice(0, batch.length, Y.encodeStateAsUpdate(idbPersistence.doc))
    const stateTx = newTransaction()
    return { tx: stateTx, requests: [stateTx.objectStore(updatesStoreName).add(batch[0])], addError }
  }
}

/**
 * The error that failed `tx`, for its `error`/`abort` handlers. A failing
 * request's bubbling 'error' event reaches the transaction BEFORE the abort
 * steps set `tx.error`, so fall back to the error of the request the event
 * came from, and to a generic Error when neither is set (e.g. an explicit
 * `abort()` with no pending requests).
 *
 * @param {IDBTransaction} tx
 * @param {Event | undefined} event
 * @param {string} message
 * @return {Error}
 */
const transactionError = (tx, event, message) => {
  const target = /** @type {IDBRequest | IDBTransaction | null} */ (event ? event.target : null)
  return tx.error || (target && target.error) || new Error(message)
}

/**
 * Emit an event without letting a throwing listener unwind into the caller.
 * Listeners are user code that runs synchronously: an exception escaping
 * into the provider's own follow-up work (arming a retry, settling a write,
 * closing the database) would skip it and wedge the provider. The exception
 * is re-thrown from a microtask instead, so it is still reported, like one
 * thrown by a DOM event listener.
 *
 * @param {Observable<string>} observable
 * @param {string} name
 * @param {Array<any>} args
 */
const emitIsolated = (observable, name, args) => {
  try {
    observable.emit(name, args)
  } catch (e) {
    queueMicrotask(() => { throw e })
  }
}

/**
 * The doc's 'update' listeners, or undefined when there are none or lib0's
 * Observable internals are not what we expect.
 *
 * @param {Y.Doc} doc
 * @return {Set<function(...any):any>|undefined}
 */
const updateListeners = doc => doc._observers instanceof Map ? doc._observers.get('update') : undefined

/**
 * Apply rows read from the updates store. They are applied with origin ===
 * idbPersistence, so _storeUpdate does not write them back.
 *
 * Each row is applied on its own: one undecodable row (storage corruption,
 * a buggy writer, a bad writeSnapshot payload) would otherwise abort the
 * batch, leaving the rows after it unloaded and the read cursor in front of
 * it, so every later load and trim would throw on it again. Skipped rows
 * are reported via 'error' and make the next trim a full consolidation,
 * which deletes them.
 *
 * A remote update whose dependencies are missing is parked by Yjs in
 * doc.store.pendingStructs / pendingDs without an 'update' event, so it was
 * never queued. A loaded row (e.g. written by another tab) can supply the
 * missing dependency, and Yjs then integrates the parked update inside this
 * transaction — under our origin. Queue the parked bytes first (Yjs keeps
 * them V2-encoded; Y.convertUpdateFormatV2ToV1 is exported from yjs 13.5.23
 * on, hence the peer floor) so everything the load integrates is persisted.
 *
 * Yjs encodes an update for every transaction while the doc has an 'update'
 * listener. For this transaction that update is as large as everything
 * loaded, and _storeUpdate drops it (origin === idbPersistence). So when
 * _storeUpdate is the only listener it is detached while the rows are
 * applied. Yjs emits the 'update' of a transaction an observer starts during
 * the cleanup before Y.transact returns, so the next transaction's
 * 'beforeTransaction' re-attaches it first: writes made in reaction to the
 * load are still persisted.
 *
 * @param {IndexeddbPersistence} idbPersistence
 * @param {Array<Uint8Array>} rows
 */
const applyStoredUpdates = (idbPersistence, rows) => {
  const doc = idbPersistence.doc
  const { pendingStructs, pendingDs } = doc.store
  if (rows.length > 0 && (pendingStructs || pendingDs)) {
    if (pendingStructs) idbPersistence._pendingUpdates.push(Y.convertUpdateFormatV2ToV1(pendingStructs.update))
    if (pendingDs) idbPersistence._pendingUpdates.push(Y.convertUpdateFormatV2ToV1(pendingDs))
    idbPersistence._scheduleFlush()
  }
  const storeUpdate = idbPersistence._storeUpdate
  const listeners = updateListeners(doc)
  // Nested in another transaction there is nothing to save: that one's
  // update is encoded after we return.
  let detached = rows.length > 0 && doc._transaction === null &&
    listeners !== undefined && listeners.size === 1 && listeners.has(storeUpdate)
  const reattach = () => {
    if (!detached) return
    detached = false
    doc.off('beforeTransaction', reattach)
    // destroy() (maybe called by an observer meanwhile) detached it for good.
    if (idbPersistence._destroyed) return
    // Put it back first, where it was: an observer may have added listeners.
    const added = Array.from(updateListeners(doc) || [])
    added.forEach(f => doc.off('update', f))
    doc.on('update', storeUpdate)
    added.forEach(f => doc.on('update', f))
  }
  if (detached) doc.off('update', storeUpdate)
  /** @type {Array<any>} */
  const errors = []
  try {
    Y.transact(doc, () => {
      // Registered in here so it fires for the next transaction, not this one.
      if (detached) doc.on('beforeTransaction', reattach)
      rows.forEach(val => {
        try {
          Y.applyUpdate(doc, val)
        } catch (err) {
          errors.push(err)
        }
      })
    }, idbPersistence, false)
  } finally {
    reattach()
  }
  if (errors.length > 0) {
    idbPersistence._hasCorruptRows = true
    // Isolated: the caller still has to finish loading (or trimming), which
    // a throwing listener must not cut short.
    errors.forEach(err => emitIsolated(idbPersistence, 'error', [err]))
  }
}

/**
 * @param {IndexeddbPersistence} idbPersistence
 * @param {function(IDBObjectStore):any} [beforeApplyUpdatesCallback]
 * @param {function(IDBObjectStore):void} [afterApplyUpdatesCallback]
 * @return {Promise<any>}
 */
const _fetchUpdates = (idbPersistence, beforeApplyUpdatesCallback, afterApplyUpdatesCallback) => {
  if (idbPersistence._destroyed) return promise.resolve()
  if (!idbPersistence.db) {
    return idbPersistence._db.then(db => {
      idbPersistence.db = db
      return _fetchUpdates(idbPersistence, beforeApplyUpdatesCallback, afterApplyUpdatesCallback)
    })
  }
  const [updatesStore] = idb.transact(/** @type {IDBDatabase} */ (idbPersistence.db), [updatesStoreName], 'readwrite')
  // Any first request will do: its callback is where _dbref may be read
  // (see restoreCursorOnAbort).
  return getLastKey(updatesStore).then(() => {
    if (idbPersistence._destroyed) return
    // getLastKey below can return the uncommitted initial-state row.
    restoreCursorOnAbort(idbPersistence, updatesStore.transaction)
    return rtop(updatesStore.getAll(idb.createIDBKeyRangeLowerBound(idbPersistence._dbref, false)))
  }).then(updates => {
    if (idbPersistence._destroyed) return
    if (beforeApplyUpdatesCallback) beforeApplyUpdatesCallback(updatesStore)
    applyStoredUpdates(idbPersistence, updates)
    if (afterApplyUpdatesCallback) afterApplyUpdatesCallback(updatesStore)
  })
    .then(() => {
      if (idbPersistence._destroyed) return
      return getLastKey(updatesStore).then(lastKey => {
        if (idbPersistence._destroyed) return
        idbPersistence._dbref = (lastKey === null || lastKey === undefined) ? 0 : lastKey + 1
      })
    })
    .then(() => {
      if (idbPersistence._destroyed) return
      return rtop(updatesStore.count()).then(cnt => {
        if (idbPersistence._destroyed) return
        idbPersistence._dbsize = cnt
      })
    })
    .then(() => updatesStore)
}

/**
 * @param {IndexeddbPersistence} idbPersistence
 * @param {function(IDBObjectStore):any} [beforeApplyUpdatesCallback]
 * @param {function(IDBObjectStore):void} [afterApplyUpdatesCallback]
 * @return {Promise<any>}
 */
export const fetchUpdates = (idbPersistence, beforeApplyUpdatesCallback, afterApplyUpdatesCallback) =>
  transactWrite(idbPersistence, () => _fetchUpdates(idbPersistence, beforeApplyUpdatesCallback, afterApplyUpdatesCallback))

/**
 * Consolidates the updates store.
 *
 * Tiered strategy (the aged-document fix): re-encoding the WHOLE document
 * with `Y.encodeStateAsUpdate` costs O(document) main-thread CPU and writes
 * an O(document) row — paying that every PREFERRED_TRIM_SIZE updates makes
 * both trim latency and write amplification grow linearly with document
 * age. Instead:
 *
 * - **Incremental trim** (the common case): merge only the fresh tail rows
 *   into ONE delta row with `Y.mergeUpdates` — O(new updates) CPU, no
 *   document materialization, no O(document) write.
 * - **Full consolidation** (rare, or `forceStore`): the legacy behavior —
 *   write `Y.encodeStateAsUpdate(doc)` (which reflects in-memory GC) and
 *   delete everything older. Triggered when delta rows accumulate beyond
 *   MAX_SEGMENT_ROWS, or their bytes exceed max(base row,
 *   MIN_FULL_COMPACT_BYTES), keeping the database bounded at roughly 2x
 *   the consolidated document size.
 *
 * @param {IndexeddbPersistence} idbPersistence
 * @param {boolean} forceStore
 */
export const storeState = (idbPersistence, forceStore = true) =>
  transactWrite(idbPersistence, () => _storeState(idbPersistence, forceStore))

/**
 * Key of the tiered-trim bookkeeping record in the custom store.
 * Written atomically with every trim (updates + custom in one transaction).
 */
const trimStateKey = '__yidb_trim_v1'

/**
 * @typedef {object} TrimState
 * @property {number} baseKey Key of the full-consolidation row
 * @property {number} lastSegKey Highest key already folded into a delta row
 * @property {number} segBytes Total bytes of delta rows since the last full
 * @property {number} baseBytes Size of the full-consolidation row
 */

/**
 * @param {IndexeddbPersistence} idbPersistence
 * @param {boolean} forceStore
 * @param {function(IDBTransaction):void} [onTransaction] Called with the
 * trim transaction as soon as it is created.
 * @return {Promise<any>}
 */
const _storeState = (idbPersistence, forceStore, onTransaction) => {
  if (idbPersistence._destroyed) return promise.resolve()
  if (!idbPersistence.db) {
    return idbPersistence._db.then(db => {
      idbPersistence.db = db
      return _storeState(idbPersistence, forceStore, onTransaction)
    })
  }
  const db = /** @type {IDBDatabase} */ (idbPersistence.db)
  const [updatesStore, customStore] = idb.transact(db, [updatesStoreName, customStoreName], 'readwrite')
  // Any committed trim, explicit ones included, ends the backoff of failed
  // ones (see `_trim`).
  updatesStore.transaction.addEventListener('complete', () => {
    idbPersistence._trimFailures = 0
  })
  // The chain below settles in its last request's callback, before the
  // commit. Settle on the transaction's outcome instead, so a trim that
  // aborts at commit (e.g. a QuotaExceededError) rejects rather than
  // resolving as if it had been stored.
  const committed = promise.create((resolve, reject) => {
    const tx = updatesStore.transaction
    tx.addEventListener('complete', () => resolve(undefined))
    tx.addEventListener('abort', event => reject(transactionError(tx, event, 'trim transaction failed')))
  })
  // Unused when a failed request rejects the chain itself first.
  committed.catch(() => {})
  if (onTransaction) onTransaction(updatesStore.transaction)
  // Count first: _dbref is read in a request callback (see
  // restoreCursorOnAbort).
  return rtop(updatesStore.count()).then(cnt => {
    if (idbPersistence._destroyed) return
    // The trims below advance _dbref past their own, uncommitted row.
    restoreCursorOnAbort(idbPersistence, updatesStore.transaction)
    idbPersistence._dbsize = cnt
    const prevDbref = idbPersistence._dbref
    // Fetch (and apply) rows we have not seen yet — they may have been
    // written by another tab.
    return rtop(updatesStore.getAll(idb.createIDBKeyRangeLowerBound(prevDbref, false))).then(newRows => {
      if (idbPersistence._destroyed) return
      applyStoredUpdates(idbPersistence, newRows)
      if (!forceStore && cnt < PREFERRED_TRIM_SIZE) {
        // Nothing to trim; just advance the cursor past what was applied.
        return getLastKey(updatesStore).then(lastKey => {
          if (idbPersistence._destroyed) return
          idbPersistence._dbref = (lastKey === null || lastKey === undefined) ? 0 : lastKey + 1
        })
      }
      return rtop(customStore.get(trimStateKey)).then((rawTrimState) => {
        if (idbPersistence._destroyed) return
        const trimState = /** @type {TrimState|undefined} */ (/** @type {unknown} */ (rawTrimState))

        /**
         * Full consolidation (legacy behavior): one row holding
         * Y.encodeStateAsUpdate(doc) — O(document) CPU and write, so the
         * tiered path below reserves it for when delta rows have piled up.
         * The doc covers every decodable stored row here: rows below
         * prevDbref were applied during hydration/earlier fetches or
         * written by this provider, newer ones just above.
         * @return {Promise<any>}
         */
        const fullConsolidation = () => {
          const fullState = Y.encodeStateAsUpdate(idbPersistence.doc)
          return rtop(updatesStore.add(fullState))
            .then(key => {
              if (idbPersistence._destroyed) return
              idbPersistence._dbref = key + 1
              // The delete below drops every older row, corrupt ones too,
              // but only if it commits: an aborted transaction keeps them,
              // and the next trim must consolidate fully again.
              updatesStore.transaction.addEventListener('complete', () => {
                idbPersistence._hasCorruptRows = false
              })
              return rtop(updatesStore.delete(idb.createIDBKeyRangeUpperBound(key, true)))
                .then(() => rtop(customStore.put({
                  baseKey: key,
                  lastSegKey: key,
                  segBytes: 0,
                  baseBytes: fullState.byteLength
                }, trimStateKey)))
            })
            .then(() => {
              if (idbPersistence._destroyed) return
              return rtop(updatesStore.count()).then(cnt2 => {
                if (idbPersistence._destroyed) return
                idbPersistence._dbsize = cnt2
              })
            })
        }

        // No bookkeeping yet (fresh or legacy database) — establish the
        // base row with a full consolidation. Same when a row failed to
        // decode: it may sit outside the tail an incremental trim folds.
        if (forceStore || idbPersistence._hasCorruptRows || trimState === undefined || typeof trimState.lastSegKey !== 'number') {
          return fullConsolidation()
        }

        // Incremental trim: fold every row after the last fold boundary
        // into ONE delta row — O(new updates), no document re-encode.
        // Rows in (baseKey, lastSegKey] are the delta rows written by
        // earlier incremental trims (zero right after a full
        // consolidation, when the two keys are equal).
        const segRowsPromise = /** @type {Promise<number>} */ (
          trimState.lastSegKey > trimState.baseKey
            ? rtop(updatesStore.count(idb.createIDBKeyRangeBound(trimState.baseKey, trimState.lastSegKey, true, false)))
            : promise.resolve(0)
        )
        return segRowsPromise.then(segRows => {
          if (idbPersistence._destroyed) return
          // Too many delta rows, or (below) too many delta bytes:
          // consolidate fully instead of writing yet another delta row.
          // Bounds the database at roughly 2x the consolidated document.
          // Decide before reading the tail (rows) and before merging it
          // (bytes): a full consolidation re-encodes the doc instead and
          // would discard the merge, in the same main-thread task.
          if (segRows + 1 >= idbPersistence._trimSegmentRows) {
            return fullConsolidation()
          }
          // Rows between lastSegKey and prevDbref may predate this session
          // (leftover tail from an earlier run) or be this provider's own
          // flushed rows; they are already in the doc, but still need
          // folding, so read them here.
          return getAllKeysValues(updatesStore, idb.createIDBKeyRangeLowerBound(trimState.lastSegKey, true)).then(tail => {
            if (idbPersistence._destroyed) return
            if (tail.length === 0) {
              return undefined
            }
            const budget = Math.max(idbPersistence._trimFullCompactBytes, trimState.baseBytes)
            // Count the tail at its stored size first: a merged row is
            // usually barely smaller (about 95% on typical tails), so this
            // settles most byte-triggered trims without merging. When it
            // does not, the exact check on the merged row below decides.
            const tailBytes = tail.reduce((bytes, row) => bytes + row.v.byteLength, 0)
            if (trimState.segBytes + tailBytes >= budget) {
              return fullConsolidation()
            }
            /** @type {Uint8Array} */
            let merged
            try {
              merged = tail.length === 1 ? tail[0].v : Y.mergeUpdates(tail.map(row => row.v))
            } catch (e) {
              // A corrupted row cannot be merged — fall back to a full
              // consolidation, which encodes the (valid) in-memory state
              // and deletes the bad row.
              return fullConsolidation()
            }
            const segBytes = trimState.segBytes + merged.byteLength
            if (segBytes >= budget) {
              return fullConsolidation()
            }
            return rtop(updatesStore.add(merged))
              .then(key => {
                if (idbPersistence._destroyed) return
                idbPersistence._dbref = key + 1
                return rtop(updatesStore.delete(idb.createIDBKeyRangeBound(tail[0].k, tail[tail.length - 1].k, false, false)))
                  .then(() => rtop(customStore.put({
                    baseKey: trimState.baseKey,
                    lastSegKey: key,
                    segBytes,
                    baseBytes: trimState.baseBytes
                  }, trimStateKey)))
              })
              .then(() => {
                if (idbPersistence._destroyed) return
                return rtop(updatesStore.count()).then(cnt2 => {
                  if (idbPersistence._destroyed) return
                  idbPersistence._dbsize = cnt2
                })
              })
          })
        })
      })
    })
  }).then(() => committed)
}

/**
 * @param {string} name
 */
export const clearDocument = name => idb.deleteDB(name)

/**
 * Read the COMPLETE persisted state of database `name` as one merged Yjs
 * update, without constructing an {@link IndexeddbPersistence} binding.
 *
 * Resolves `null` when the database holds no update rows (a missing database
 * included: opening creates an empty one with this module's store layout).
 * Multiple rows (a snapshot plus debounced incremental updates) are merged
 * with `Y.mergeUpdates`, so the result always hydrates a fresh doc to the
 * full persisted state. The optional `transactionRunner` wraps the whole
 * open→read→close unit (pass the same gate used by {@link writeSnapshot} so a
 * read can never interleave a concurrent snapshot write).
 *
 * @param {string} name
 * @param {object} [opts]
 * @param {<T>(work: () => Promise<T>) => Promise<T>} [opts.transactionRunner]
 * @return {Promise<Uint8Array|null>}
 */
export const readSnapshot = (name, { transactionRunner } = {}) => {
  const work = () => idb.openDB(name, db =>
    idb.createStores(db, [
      ['updates', { autoIncrement: true }],
      ['custom']
    ])
  ).then(db => new Promise((resolve, reject) => {
    /**
     * @type {IDBTransaction}
     */
    let tx
    try {
      tx = db.transaction([updatesStoreName], 'readonly')
    } catch (e) {
      db.close()
      reject(e)
      return
    }
    const request = tx.objectStore(updatesStoreName).getAll()
    tx.oncomplete = () => {
      db.close()
      // This runs in an event handler, outside the promise executor: a row
      // that cannot be merged (corrupt or foreign data) must reject here,
      // or the error escapes as an uncaught exception and the promise never
      // settles.
      try {
        /** @type {Array<Uint8Array>} */
        const rows = (request.result || []).map(row =>
          row instanceof Uint8Array ? row : new Uint8Array(row)
        )
        if (rows.length === 0) {
          resolve(null)
        } else if (rows.length === 1) {
          resolve(rows[0])
        } else {
          resolve(Y.mergeUpdates(rows))
        }
      } catch (e) {
        reject(e)
      }
    }
    tx.onerror = tx.onabort = event => {
      db.close()
      reject(transactionError(tx, event, 'readSnapshot transaction failed'))
    }
  }))
  return runTransaction(transactionRunner, work)
}

/**
 * Write `update` as the COMPLETE content of database `name` using this
 * module's own store layout: open/create the database → clear `updates` →
 * add the single snapshot row → await the transaction commit → close.
 *
 * Resolves only after the transaction has COMMITTED, so a page reload
 * immediately afterwards cannot lose the snapshot. The optional
 * `transactionRunner` wraps the whole open→commit→close unit.
 *
 * PRECONDITION: no live {@link IndexeddbPersistence} is bound to `name`
 * (destroy it first) — a concurrent binding could interleave its own update
 * rows.
 *
 * @param {string} name
 * @param {Uint8Array} update
 * @param {object} [opts]
 * @param {<T>(work: () => Promise<T>) => Promise<T>} [opts.transactionRunner]
 * @return {Promise<void>}
 */
export const writeSnapshot = (name, update, { transactionRunner } = {}) => {
  const work = () => idb.openDB(name, db =>
    idb.createStores(db, [
      ['updates', { autoIncrement: true }],
      ['custom']
    ])
  ).then(db => new Promise((resolve, reject) => {
    /**
     * @type {IDBTransaction}
     */
    let tx
    try {
      tx = db.transaction([updatesStoreName, customStoreName], 'readwrite')
    } catch (e) {
      db.close()
      reject(e)
      return
    }
    // Raw requests on purpose (no promise wrappers): request failures
    // surface through tx.onerror below instead of dangling rejections.
    try {
      const store = tx.objectStore(updatesStoreName)
      store.clear()
      store.add(update)
      // The tiered-trim bookkeeping refers to row keys that no longer exist;
      // drop it so the next trim re-establishes a fresh base row.
      tx.objectStore(customStoreName).delete(trimStateKey)
    } catch (e) {
      // add() throws synchronously when `update` cannot be cloned (e.g. a
      // detached buffer). That does not abort the transaction, so it would
      // commit the clear() alone and wipe the database — roll it back.
      tx.abort()
      db.close()
      reject(e)
      return
    }
    tx.oncomplete = () => {
      db.close()
      resolve(undefined)
    }
    tx.onerror = tx.onabort = event => {
      db.close()
      reject(transactionError(tx, event, 'writeSnapshot transaction failed'))
    }
  }))
  return runTransaction(transactionRunner, work)
}

/**
 * @extends Observable<string>
 */
export class IndexeddbPersistence extends Observable {
  /**
   * @param {string} name
   * @param {Y.Doc} doc
   * @param {object} [opts]
   * @param {number} [opts.writeDebounceMs]
   * @param {'default'|'relaxed'} [opts.durability]
   * @param {<T>(work: () => Promise<T>) => Promise<T>} [opts.transactionRunner]
   * @param {number} [opts.maxRetries] Number of times a failed write is
   * retried with exponential backoff before 'retry-exhausted' is emitted.
   * Retrying then continues, the backoff capped at MAX_RETRY_BACKOFF_MS,
   * until a write commits.
   * @param {number} [opts.trimSegmentRows] Maximum delta rows the tiered
   * trim accumulates before forcing a full consolidation.
   * @param {number} [opts.trimFullCompactBytes] Delta-row byte budget
   * (lower bound; the effective budget is max(this, base row size)) before
   * forcing a full consolidation.
   */
  constructor (name, doc, { writeDebounceMs = 0, durability = 'default', transactionRunner, maxRetries = 5, trimSegmentRows = MAX_SEGMENT_ROWS, trimFullCompactBytes = MIN_FULL_COMPACT_BYTES } = {}) {
    super()
    this.doc = doc
    this.name = name
    this._dbref = 0
    this._dbsize = 0
    /**
     * Set when a stored row failed to decode; cleared by the full
     * consolidation that deletes it.
     */
    this._hasCorruptRows = false
    this._trimSegmentRows = trimSegmentRows
    this._trimFullCompactBytes = trimFullCompactBytes
    this._destroyed = false
    this.writeDebounceMs = writeDebounceMs
    this.durability = durability
    this.transactionRunner = transactionRunner
    /**
     * Failed flush attempts since a write last committed. Above
     * `_maxRetries` once retries are exhausted: it is not reset until a
     * write commits, so the backoff stays at its cap meanwhile.
     */
    this._retryCount = 0
    this._maxRetries = maxRetries
    /**
     * Total failed flush attempts. flush() compares it across an attempt to
     * tell a failure from a success that left newly arrived updates queued.
     */
    this._failedFlushes = 0
    /**
     * Pending backoff timer after a failed flush. While it is armed, flush
     * scheduling is deferred to it so the backoff cannot be bypassed.
     * @type {any}
     */
    this._retryTimeoutId = null
    /**
     * @type {Promise<any>|null}
     */
    this._flushPromise = null
    /**
     * Set while a flush waits for the transactionRunner to start its work:
     * concludes that attempt and returns its batch, so the work opens no
     * transaction once it runs. The page-hide write takes the batch over
     * with it (see `_unloadListener`).
     * @type {(() => Array<Uint8Array>)|null}
     */
    this._takeQueuedFlush = null
    /**
     * @type {Promise<void>|null}
     */
    this._destroyPromise = null
    /**
     * @type {Array<Uint8Array>}
     */
    this._pendingUpdates = []
    /**
     * Whether content the doc held before this provider attached still
     * depends on the hydration transaction, the only write of it, to
     * commit. Every later update from this client builds on that content,
     * so if hydration fails or is cut short it is re-buffered instead (see
     * `_requeueInitialState`).
     */
    this._initialStatePending = doc.store.clients.size > 0
    this._writing = false
    this._flushScheduled = false
    /**
     * Pending `writeDebounceMs` timer, tracked so destroy() can clear it.
     * @type {any}
     */
    this._debounceTimeoutId = null
    /**
     * Page-hide writes still in flight (see `_unloadListener`). They bypass
     * the flusher, so flush() and destroy() wait for them separately.
     * @type {Set<Promise<void>>}
     */
    this._unloadWrites = new Set()
    /**
     * @type {IDBDatabase|null}
     */
    this.db = null
    this.synced = false
    this._db = idb.openDB(name, db =>
      idb.createStores(db, [
        ['updates', { autoIncrement: true }],
        ['custom']
      ])
    )
    /**
     * @type {Promise<IndexeddbPersistence>}
     */
    this.whenSynced = promise.create(resolve => this.on('synced', () => resolve(this)))

    this._db.then(db => {
      this.db = db
      const emitSynced = () => {
        if (this._destroyed) return
        this.synced = true
        this.emit('synced', [this])
        this._scheduleFlush()
      }
      /**
       * Isolated: every caller still has to emit 'synced' or schedule the
       * flush, which a throwing listener must not skip.
       * @param {any} err
       */
      const emitHydrationError = err => {
        if (!this._destroyed) {
          emitIsolated(this, 'error', [err])
        }
      }
      /**
       * State of the hydration transaction from the point its handlers are
       * attached (see beforeApplyUpdatesCallback).
       * @type {'unattached'|'running'|'complete'|'aborted'}
       */
      let hydrationTxState = 'unattached'
      /**
       * Failure of the hydration chain while its transaction was still
       * running. The transaction's outcome reports it (see below), so one
       * failed hydration emits one 'error'.
       * @type {{ err: any }|null}
       */
      let deferredError = null
      /**
       * @param {IDBObjectStore} updatesStore
       */
      const beforeApplyUpdatesCallback = (updatesStore) => {
        const initUpdate = Y.encodeStateAsUpdate(doc)
        if (initUpdate.length > 2) {
          // Use the raw request instead of the lib0 promise wrapper: the
          // promise would be discarded, so a transaction failure would
          // surface as an unhandled rejection instead of the tx error event.
          updatesStore.add(initUpdate)
        }
        // Defer the 'synced' emit to the hydration transaction's `complete`
        // event. That transaction carries the initial-state write above, so
        // `whenSynced` guarantees the write has COMMITTED, not merely been
        // issued. Stored updates are still applied to the doc strictly
        // before the emit: neither event can dispatch before this task ends.
        // Attach the handlers here, inside the work, not once fetchUpdates
        // resolves: the transactionRunner may settle in a later task than
        // the one in which the work resolved (a lock released after commit,
        // a setTimeout hop, ...), by which time the transaction has already
        // dispatched `complete`; and an app observer throwing out of the
        // apply, or a request error aborting the transaction, rejects that
        // chain after the rows are in the doc.
        // On abort the initial-state write was rolled back: it is re-buffered
        // and the failure reported like any failed write, and the emit still
        // happens (consumers must not wedge; the data has been applied to the
        // in-memory doc either way). No `error` handler: a request error
        // bubbles to the transaction and is then followed by `abort`, so it
        // would emit 'synced' twice.
        const tx = updatesStore.transaction
        hydrationTxState = 'running'
        tx.oncomplete = () => {
          hydrationTxState = 'complete'
          this._initialStatePending = false
          if (deferredError !== null) {
            const { err } = deferredError
            deferredError = null
            emitHydrationError(err)
          }
          emitSynced()
        }
        tx.onabort = () => {
          hydrationTxState = 'aborted'
          this._requeueInitialState()
          // Report the chain's failure if it already rejected (a failed
          // request), else what aborted the transaction (e.g. a commit-time
          // QuotaExceededError).
          const err = deferredError !== null ? deferredError.err : (tx.error || new Error('hydration transaction aborted'))
          deferredError = null
          emitHydrationError(err)
          emitSynced()
        }
      }
      // fetchUpdates can also throw synchronously (creating the transaction
      // fails on a closing connection or a database without the 'updates'
      // store). Route that into the rejection handler below instead of out
      // of this callback.
      /**
       * @type {Promise<any>}
       */
      let hydration
      try {
        hydration = fetchUpdates(this, beforeApplyUpdatesCallback)
      } catch (err) {
        hydration = Promise.reject(err)
      }
      hydration.catch(err => {
        // Initial sync failed (corrupt database, failing transactionRunner,
        // ...). Surface it instead of leaving an unhandled rejection, and
        // still start flushing: the connection itself is open, so buffered
        // updates (and the initial state the failed sync did not write) can
        // be persisted.
        if (hydrationTxState === 'running') {
          // The stored rows were already applied and the transaction is still
          // running: its outcome decides whether the initial state must be
          // re-buffered, reports this failure and still emits 'synced'.
          deferredError = { err }
        } else if (hydrationTxState !== 'aborted') {
          // (An abort has already re-buffered and reported the failure.)
          this._requeueInitialState()
          emitHydrationError(err)
        }
        this._scheduleFlush()
      })
    }, err => {
      // Opening the database failed (backing store error, storage disabled,
      // no IndexedDB, ...). Surface it instead of leaving an unhandled
      // rejection. There is no connection to flush to, so updates stay
      // buffered in memory and 'synced' never fires.
      if (!this._destroyed) {
        this.emit('error', [err])
      }
    })
    /**
     * Timeout in ms until data is merged and persisted in idb.
     */
    this._storeTimeout = 1000
    /**
     * @type {any}
     */
    this._storeTimeoutId = null
    /**
     * Consecutive failed trims (see `_trim`). While non-zero, the trim a
     * flush arms is delayed by exponential backoff instead of being retried
     * `_storeTimeout` after every later flush.
     */
    this._trimFailures = 0
    /**
     * @param {Uint8Array} update
     * @param {any} origin
     */
    this._storeUpdate = (update, origin) => {
      if (origin !== this) {
        this._pendingUpdates.push(update)
        this._scheduleFlush()
      }
    }
    doc.on('update', this._storeUpdate)
    this.destroy = this.destroy.bind(this)
    doc.on('destroy', this.destroy)

    /**
     * Writes the buffered updates, and the batch of a flush still waiting
     * for the transactionRunner, in a transaction opened synchronously:
     * pagehide cannot wait for the runner, as the page may be gone right
     * after the event. Resolves once the transaction has settled, so a
     * runner wrapping this call holds its lock until then.
     * @return {Promise<void>}
     */
    this._unloadListener = () => {
      const db = this.db
      if (!db) {
        return Promise.resolve()
      }
      // Captured up front: a listener running on an already destroyed
      // instance drops the batch, but a write that fails while destroy()
      // waits for it must hand the batch back for the final write.
      const destroyed = this._destroyed
      if (!destroyed && this._takeQueuedFlush !== null) {
        // A flush still waiting for a busy runner already took its batch
        // out of the buffer, and it never runs once the page is gone: write
        // that batch here, ahead of the updates that build on it. (On a
        // destroyed instance, destroy() waits for that flush instead.)
        this._pendingUpdates = this._takeQueuedFlush().concat(this._pendingUpdates)
      }
      if (this._pendingUpdates.length === 0) {
        return Promise.resolve()
      }
      // Hydration may not get to commit the initial state once the page
      // is gone; the buffered updates build on it, so write it with them.
      this._requeueInitialState()
      const batch = this._pendingUpdates.splice(0, this._pendingUpdates.length)
      /** @type {(value: void) => void} */
      let resolveWrite = () => {}
      /** @type {Promise<void>} */
      const write = new Promise(resolve => { resolveWrite = resolve })
      this._unloadWrites.add(write)
      const settle = () => {
        this._unloadWrites.delete(write)
        resolveWrite()
      }
      /**
       * @param {any} err
       */
      const onFailed = err => {
        if (!destroyed) {
          // Hand the batch back to the flusher, which owns retries: an
          // in-flight flush picks it up when it settles, an armed backoff
          // retry when it fires, otherwise the flush scheduled here does.
          this._pendingUpdates = batch.concat(this._pendingUpdates)
          // Isolated: a throwing listener must not skip scheduling the
          // retry or settling the write flush()/destroy() wait for.
          emitIsolated(this, 'error', [err])
          this._scheduleFlush()
        }
        settle()
      }
      try {
        const { tx, requests, addError } = addBatch(this, () => db.transaction([updatesStoreName], 'readwrite'), batch)
        tx.oncomplete = () => {
          this._dbsize += batch.length
          advanceCursorPastOwnRows(this, requests)
          // Writes work again: end a failure episode, as a flush commit
          // does, instead of leaving updates that arrived meanwhile to a
          // backoff of up to MAX_RETRY_BACKOFF_MS.
          this._retryCount = 0
          if (this._retryTimeoutId !== null) {
            clearTimeout(this._retryTimeoutId)
            this._retryTimeoutId = null
            this._scheduleFlush()
          }
          settle()
        }
        let handled = false
        tx.onerror = tx.onabort = event => {
          if (handled) return
          handled = true
          onFailed(transactionError(tx, event, 'page-hide write failed'))
        }
        // Isolated: a throwing listener must not reach the catch below and
        // hand back the batch the transaction is writing.
        if (addError !== null) emitIsolated(this, 'error', [addError])
      } catch (e) {
        onFailed(e)
      }
      return write
    }
    if (typeof addEventListener !== 'undefined') {
      addEventListener('pagehide', this._unloadListener)
    }
    if (typeof document !== 'undefined') {
      this._visibilityListener = () => {
        if (document.visibilityState === 'hidden') {
          // Unlike pagehide, a tab switch can wait for the transactionRunner
          // (without one this still writes synchronously). The updates stay
          // buffered until the write starts, so a pagehide that fires while
          // the runner is busy still writes them itself. A write the runner
          // starts only after destroy() leaves them to destroy()'s final
          // write, which destroy() waits for and whose failure it reports.
          transactWrite(this, () => this._destroyed ? Promise.resolve() : this._unloadListener()).catch(err => {
            if (!this._destroyed) {
              this.emit('error', [err])
            }
          })
        }
      }
      document.addEventListener('visibilitychange', this._visibilityListener)
    }
  }

  _scheduleFlush () {
    if (this._destroyed || this._writing || this._pendingUpdates.length === 0) return
    // A failed flush armed a backoff timer that will reschedule; flushing
    // now (e.g. because a new update arrived) would bypass the backoff.
    if (this._retryTimeoutId !== null) return
    if (this._flushScheduled) return
    this._flushScheduled = true
    if (this.writeDebounceMs > 0) {
      this._debounceTimeoutId = setTimeout(() => {
        this._debounceTimeoutId = null
        this._flushScheduled = false
        this._flush()
      }, this.writeDebounceMs)
    } else {
      queueMicrotask(() => {
        this._flushScheduled = false
        this._flush()
      })
    }
  }

  /**
   * Re-buffer the content the doc held before this provider attached when
   * the hydration transaction will not commit it (failed, aborted, or cut
   * short by teardown), ahead of the updates that build on it. Without it,
   * rows written by this client cannot be applied on reload. No-op once the
   * initial state is committed or already re-buffered.
   */
  _requeueInitialState () {
    if (!this._initialStatePending) return
    this._initialStatePending = false
    this._pendingUpdates.unshift(Y.encodeStateAsUpdate(this.doc))
  }

  /**
   * Recover from a failed flush attempt: re-buffer the batch, surface the
   * error, and schedule a retry with exponential backoff, capped at
   * MAX_RETRY_BACKOFF_MS. 'retry-exhausted' is emitted once `maxRetries`
   * retries have failed, once per failure episode; the retries go on at the
   * capped backoff. Restarting the short retry cycle on the next update
   * instead would retry every couple of seconds while the app keeps
   * editing, each attempt re-adding the whole backlog.
   *
   * @param {Array<Uint8Array>} batch
   * @param {any} err
   */
  _onFlushFailed (batch, err) {
    this._failedFlushes++
    this._pendingUpdates = batch.concat(this._pendingUpdates)
    this._writing = false
    this._flushPromise = null
    // Isolated: a throwing listener must not skip arming the retry below or
    // the caller's resolve() — the flush work would never settle, hanging
    // flush()/destroy() and wedging a serializing transactionRunner.
    emitIsolated(this, 'error', [err])
    if (!this._destroyed) {
      this._retryCount++
      const backoff = Math.min(Math.pow(2, this._retryCount) * 100, MAX_RETRY_BACKOFF_MS)
      this._retryTimeoutId = setTimeout(() => {
        this._retryTimeoutId = null
        this._scheduleFlush()
      }, backoff)
      // After arming the retry: a listener that destroys the provider then
      // clears it.
      if (this._retryCount === this._maxRetries + 1) {
        emitIsolated(this, 'retry-exhausted', [err || new Error('Retry exhausted')])
      }
    }
  }

  _flush () {
    if (this._destroyed || this._writing || this._pendingUpdates.length === 0) return
    // A failed flush armed a backoff retry that will reschedule. A debounce
    // timer or microtask scheduled before the failure must not attempt the
    // write inside the backoff: a second failure would arm another retry
    // over the first, leaving that one untracked (two retry chains, and a
    // timer destroy() cannot clear). flush() clears the retry before calling
    // this when it bypasses the backoff on purpose.
    if (this._retryTimeoutId !== null) return
    const db = this.db
    if (!db) {
      // Don't re-schedule here — the _db.then() callback in the constructor
      // will call _scheduleFlush() once the database is ready. Re-scheduling
      // via queueMicrotask would create an infinite spin-loop that starves
      // the event loop and prevents _db from ever resolving.
      return
    }
    this._writing = true
    const batch = this._pendingUpdates
    this._pendingUpdates = []
    // Each flush attempt is concluded exactly once, and `_flushPromise`
    // resolves only then. Once the transaction is under way, its outcome
    // concludes the attempt: the transactionRunner's promise may settle
    // before it does (a deadline that rejects, or a watchdog that resolves,
    // while the transaction is stalled), and acting on that would let a
    // retry open a second flush transaction beside the pending one,
    // flush()/destroy() stop waiting for it, and its late outcome be handled
    // a second time. The runner settling only concludes an attempt whose
    // transaction never got under way.
    let concluded = false
    let txUnderWay = false
    /** @type {() => void} */
    let onConcluded = () => {}
    /** @type {Promise<void>} */
    const flushPromise = new Promise(resolve => { onConcluded = resolve })
    /**
     * @param {any} err
     */
    const onFailed = err => {
      if (concluded) return
      concluded = true
      this._takeQueuedFlush = null
      this._onFlushFailed(batch, err)
      onConcluded()
    }
    // pagehide cannot wait for the runner: until the transaction is under
    // way, the page-hide write may take the batch over (see
    // `_unloadListener`), which concludes the attempt.
    this._takeQueuedFlush = () => {
      concluded = true
      this._takeQueuedFlush = null
      this._writing = false
      this._flushPromise = null
      onConcluded()
      return batch
    }
    transactWrite(this, () => new Promise(resolve => {
      // The runner settled without running this work and runs it only now
      // (e.g. a lock queue whose timeout does not dequeue), or the page-hide
      // write took the batch over: the batch was already re-buffered or
      // written, so don't open a stale transaction.
      if (concluded) {
        resolve(undefined)
        return
      }
      /**
       * @type {ReturnType<typeof addBatch>}
       */
      let added
      try {
        added = addBatch(this, () => db.transaction([updatesStoreName], 'readwrite', { durability: this.durability }), batch)
      } catch (e) {
        onFailed(e)
        resolve(undefined)
        return
      }
      const { tx, requests, addError } = added
      tx.oncomplete = () => {
        concluded = true
        this._retryCount = 0
        this._dbsize += batch.length
        advanceCursorPastOwnRows(this, requests)
        this._writing = false
        this._flushPromise = null
        if (this._pendingUpdates.length > 0) {
          this._scheduleFlush()
        }
        // Schedule a compaction if none is pending yet. Don't reset a
        // pending timer: under sustained writes that would postpone the trim
        // indefinitely and let the store grow without bound. While trims
        // keep failing (e.g. a QuotaExceededError at commit), back off
        // instead of retrying after every flush: each attempt redoes the
        // trim, re-reading and re-merging the rows written since the last
        // committed trim (re-applying those other tabs wrote) or, for a
        // full consolidation, re-encoding the whole doc.
        if (!this._destroyed && this._dbsize >= PREFERRED_TRIM_SIZE && this._storeTimeoutId === null) {
          const backoff = Math.pow(2, Math.min(this._trimFailures, MAX_TRIM_BACKOFF_EXPONENT))
          this._storeTimeoutId = setTimeout(() => {
            this._storeTimeoutId = null
            this._trim()
          }, this._storeTimeout * backoff)
        }
        onConcluded()
        resolve(undefined)
      }
      // A failed transaction fires a bubbling 'error' event for every pending
      // request and then 'abort' — `onFailed` handles one failure once.
      /**
       * @param {Event} [event]
       */
      const onErrorOrAbort = event => {
        onFailed(transactionError(tx, event, 'flush transaction failed'))
        resolve(undefined)
      }
      tx.onerror = onErrorOrAbort
      tx.onabort = onErrorOrAbort
      // From here on only the transaction's outcome concludes the attempt.
      txUnderWay = true
      this._takeQueuedFlush = null
      if (addError !== null) emitIsolated(this, 'error', [addError])
    })).then(() => {
      // The runner resolved without the work getting its transaction under
      // way (it skipped the work, or runs it only later): nothing else will
      // conclude the attempt.
      if (!txUnderWay) {
        onFailed(new Error('transactionRunner resolved without running the flush'))
      }
    }, err => {
      // The transactionRunner itself failed. Without this, _writing would
      // stay true forever and every future update would silently pile up in
      // _pendingUpdates without ever being written.
      if (!txUnderWay) {
        onFailed(err)
      }
    })
    // Unless the attempt already failed synchronously (the transaction
    // could not be opened), which cleared `_flushPromise`.
    if (!concluded) {
      this._flushPromise = flushPromise
    }
  }

  /**
   * The trim a flush arms once the store holds PREFERRED_TRIM_SIZE rows.
   * A failed attempt counts once towards `_trimFailures`, whether the
   * transaction's 'abort' reports it (the only report when a
   * transactionRunner resolves before the commit) or the rejection does (a
   * failing transactionRunner, a closing database; an aborted transaction,
   * at commit too, causes both), and the rejection is emitted as 'error'.
   * Only a committed trim resets `_trimFailures` (see `_storeState`), not a
   * successful flush.
   */
  _trim () {
    let concluded = false
    const onFailed = () => {
      if (concluded) return
      concluded = true
      this._trimFailures++
    }
    /**
     * @param {any} err
     */
    const onError = err => {
      onFailed()
      if (!this._destroyed) {
        this.emit('error', [err])
      }
    }
    // The trim can fail synchronously (transact on a closing db throws) or
    // asynchronously — surface both via 'error' instead of an uncaught
    // exception / unhandled rejection.
    try {
      transactWrite(this, () => _storeState(this, false, tx => {
        // A transactionRunner may still reject after the commit.
        tx.addEventListener('complete', () => { concluded = true })
        tx.addEventListener('abort', onFailed)
      })).catch(onError)
    } catch (err) {
      onError(err)
    }
  }

  /**
   * Force-drain the pending update queue NOW, bypassing the
   * `writeDebounceMs` timer: runs `_flush()` immediately, awaits the
   * in-flight transaction commit (the existing `_flushPromise` resolves in
   * the transaction's `oncomplete`/`onerror`), and loops until
   * `_pendingUpdates` is empty with no flush in flight — updates that arrive
   * mid-flush are drained too. A page-hide write in flight is awaited the
   * same way. Resolves immediately when idle.
   *
   * While a backoff retry is armed after a failed flush, this waits for the
   * scheduled retry instead of hot-spinning a failing transaction. Once
   * retries are exhausted it does not wait out the capped backoff (up to
   * MAX_RETRY_BACKOFF_MS) but retries on a short backoff of its own. On
   * persistent failure it keeps retrying, so callers that need a bound
   * should race it against a deadline.
   *
   * A debounce timer that is already scheduled is left to fire: its
   * `_flush()` no-ops once the queue has been drained here, or while a
   * backoff retry is armed.
   *
   * @return {Promise<void>}
   */
  async flush () {
    await this._db
    let exhaustedFailures = 0
    /**
     * Once retries are exhausted (at the first failure with `maxRetries:
     * 0`) this loop does not wait for the armed retry (see below), so
     * retrying at once would loop through microtasks only — starving every
     * timer, including the deadline callers race flush() against. Back off
     * as the automatic retries do before exhaustion instead.
     *
     * @param {number} failedBefore `_failedFlushes` before the attempt
     */
    const backOffIfExhausted = async failedBefore => {
      if (this._failedFlushes === failedBefore || this._retryCount <= this._maxRetries || this._destroyed) return
      exhaustedFailures++
      const backoff = Math.pow(2, Math.min(exhaustedFailures, 5)) * 100
      await new Promise(resolve => setTimeout(resolve, backoff))
    }
    for (;;) {
      // A flush is in flight — wait for it to settle. `_flushPromise`
      // resolves only once the attempt has concluded and cleared `_writing`
      // (not when the transactionRunner's promise settles), so this cannot
      // re-await a settled promise in a loop that starves the event loop.
      if (this._writing) {
        const failedBefore = this._failedFlushes
        await (this._flushPromise || new Promise(resolve => setTimeout(resolve, 10)))
        await backOffIfExhausted(failedBefore)
        continue
      }
      // A page-hide write took the queue into its own transaction — wait for
      // it too. If it failed, its batch is back in the queue.
      if (this._unloadWrites.size > 0) {
        await Promise.all(this._unloadWrites)
        continue
      }
      if (this._destroyed || this._pendingUpdates.length === 0) return
      if (this._retryTimeoutId !== null) {
        if (this._retryCount <= this._maxRetries) {
          // A backoff retry is armed (see `_onFlushFailed`) — wait for its
          // `_scheduleFlush` to fire (`_flush()` declines to run while it is
          // armed) rather than bypassing the backoff.
          await new Promise(resolve => setTimeout(resolve, 50))
          continue
        }
        // Retries are exhausted, so the armed retry may be up to
        // MAX_RETRY_BACKOFF_MS away: attempt now, paced by
        // backOffIfExhausted. A failed attempt re-arms the retry.
        clearTimeout(this._retryTimeoutId)
        this._retryTimeoutId = null
      }
      const failedBefore = this._failedFlushes
      this._flush()
      if (!this._writing) {
        // `_flush` declined to run (raced the debounce timer's own run) or
        // failed synchronously — yield and re-check rather than spinning.
        await new Promise(resolve => setTimeout(resolve, 10))
        await backOffIfExhausted(failedBefore)
      }
    }
  }

  destroy () {
    if (this._destroyPromise) {
      return this._destroyPromise
    }
    if (this._storeTimeoutId !== null) {
      clearTimeout(this._storeTimeoutId)
      this._storeTimeoutId = null
    }
    if (this._retryTimeoutId !== null) {
      clearTimeout(this._retryTimeoutId)
      this._retryTimeoutId = null
    }
    if (this._debounceTimeoutId !== null) {
      clearTimeout(this._debounceTimeoutId)
      this._debounceTimeoutId = null
    }
    this.doc.off('update', this._storeUpdate)
    this.doc.off('destroy', this.destroy)
    this._destroyed = true
    if (typeof addEventListener !== 'undefined') {
      removeEventListener('pagehide', this._unloadListener)
    }
    if (typeof document !== 'undefined' && this._visibilityListener) {
      document.removeEventListener('visibilitychange', this._visibilityListener)
    }

    // Wait for an in-flight flush and any page-hide writes to settle before
    // writing the remaining pending updates: if one fails it re-buffers its
    // batch into _pendingUpdates, so snapshotting the queue only afterwards
    // guarantees the failed batch is included in the final write instead of
    // lost.
    const activeFlushPromise = Promise.all([this._flushPromise, ...this._unloadWrites]).then(() => {}, () => {})
    this._destroyPromise = activeFlushPromise
      // Before the connection opens no flush can be in flight and `this.db`
      // is still null, so the final write below would be skipped. Wait for
      // the connection: the constructor's `_db.then` callback is registered
      // first, so it has assigned `this.db` by the time this resolves.
      .then(() => this._db.then(() => {}, () => {}))
      .then(() => {
        const db = this.db
        if (db) {
          // Hydration may have been cut short before committing the initial
          // state: write it with the final batch.
          this._requeueInitialState()
        }
        if (!this.synced) {
          // The initial sync did not complete, so its initial-state write
          // (see beforeApplyUpdatesCallback) may be missing, and updates made
          // on top of that state (buffered or already flushed) would not
          // decode without it. Write the whole doc state instead; it covers
          // every buffered update (and the re-queued initial state).
          const initUpdate = Y.encodeStateAsUpdate(this.doc)
          if (initUpdate.length > 2) {
            this._pendingUpdates = [initUpdate]
          }
        }
        if (db && this._pendingUpdates.length > 0) {
          const batch = this._pendingUpdates.splice(0, this._pendingUpdates.length)
          return transactWrite(this, () => new Promise((resolve) => {
            try {
              const { tx, addError } = addBatch(this, () => db.transaction([updatesStoreName], 'readwrite', { durability: this.durability }), batch)
              tx.oncomplete = () => resolve(undefined)
              let handled = false
              tx.onerror = tx.onabort = event => {
                if (!handled) {
                  handled = true
                  // Isolated: a commit failure fires only 'abort', so a
                  // throwing listener would skip the one resolve() and
                  // destroy() would never settle.
                  emitIsolated(this, 'error', [transactionError(tx, event, 'final flush transaction failed')])
                }
                resolve(undefined)
              }
              if (addError !== null) emitIsolated(this, 'error', [addError])
            } catch (e) {
              emitIsolated(this, 'error', [e])
              resolve(undefined)
            }
          })).catch(err => {
            // transactionRunner failure during the final flush — nothing
            // more can be done at teardown beyond surfacing it.
            emitIsolated(this, 'error', [err])
          })
        }
      })
      .then(() => this._db.then(db => { db.close() }, () => {}))
      .then(() => {
        super.destroy()
      })
    return this._destroyPromise
  }

  /**
   * Destroys this instance and removes all data from indexeddb.
   *
   * @return {Promise<void>}
   */
  clearData () {
    return this.destroy().then(() => idb.deleteDB(this.name))
  }

  /**
   * @param {String | number | ArrayBuffer | Date} key
   * @return {Promise<String | number | ArrayBuffer | Date | any>}
   */
  get (key) {
    return this._db.then(db => {
      const [custom] = idb.transact(db, [customStoreName], 'readonly')
      return rtop(custom.get(key))
    })
  }

  /**
   * @param {String | number | ArrayBuffer | Date} key
   * @param {String | number | ArrayBuffer | Date} value
   * @return {Promise<String | number | ArrayBuffer | Date>}
   */
  set (key, value) {
    return this._db.then(db =>
      transactWrite(this, () => {
        const [custom] = idb.transact(db, [customStoreName])
        return rtop(custom.put(value, key))
      })
    )
  }

  /**
   * @param {String | number | ArrayBuffer | Date} key
   * @return {Promise<undefined>}
   */
  del (key) {
    return this._db.then(db =>
      transactWrite(this, () => {
        const [custom] = idb.transact(db, [customStoreName])
        return rtop(custom.delete(key))
      })
    )
  }
}
