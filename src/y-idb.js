/* eslint-env browser */

import * as Y from 'yjs'
import * as idb from 'lib0/indexeddb'
import * as promise from 'lib0/promise'
import { Observable } from 'lib0/observable'

const customStoreName = 'custom'
const updatesStoreName = 'updates'

export const PREFERRED_TRIM_SIZE = 500

/**
 * @template T
 * @param {IndexeddbPersistence} idbPersistence
 * @param {() => Promise<T>} work
 * @return {Promise<T>}
 */
const transactWrite = (idbPersistence, work) => {
  if (idbPersistence.transactionRunner) {
    return idbPersistence.transactionRunner(work)
  }
  return work()
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
  return idb.getAll(updatesStore, idb.createIDBKeyRangeLowerBound(idbPersistence._dbref, false)).then(updates => {
    if (idbPersistence._destroyed) return
    if (beforeApplyUpdatesCallback) beforeApplyUpdatesCallback(updatesStore)
    Y.transact(idbPersistence.doc, () => {
      updates.forEach(val => Y.applyUpdate(idbPersistence.doc, val))
    }, idbPersistence, false)
    if (afterApplyUpdatesCallback) afterApplyUpdatesCallback(updatesStore)
  })
    .then(() => {
      if (idbPersistence._destroyed) return
      return idb.getLastKey(updatesStore).then(lastKey => {
        if (idbPersistence._destroyed) return
        idbPersistence._dbref = (lastKey === null || lastKey === undefined) ? 0 : lastKey + 1
      })
    })
    .then(() => {
      if (idbPersistence._destroyed) return
      return idb.count(updatesStore).then(cnt => {
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
 * @param {IndexeddbPersistence} idbPersistence
 * @param {boolean} forceStore
 */
export const storeState = (idbPersistence, forceStore = true) =>
  transactWrite(idbPersistence, () =>
    _fetchUpdates(idbPersistence)
      .then(updatesStore => {
        if (idbPersistence._destroyed) return
        if (forceStore || idbPersistence._dbsize >= PREFERRED_TRIM_SIZE) {
          return idb.addAutoKey(updatesStore, Y.encodeStateAsUpdate(idbPersistence.doc))
            .then(() => {
              if (idbPersistence._destroyed) return
              return idb.del(updatesStore, idb.createIDBKeyRangeUpperBound(idbPersistence._dbref, true))
            })
            .then(() => {
              if (idbPersistence._destroyed) return
              return idb.count(updatesStore).then(cnt => {
                if (idbPersistence._destroyed) return
                idbPersistence._dbsize = cnt
              })
            })
        }
      })
  )

/**
 * @param {string} name
 */
export const clearDocument = name => idb.deleteDB(name)

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
   */
  constructor (name, doc, { writeDebounceMs = 0, durability = 'default', transactionRunner } = {}) {
    super()
    this.doc = doc
    this.name = name
    this._dbref = 0
    this._dbsize = 0
    this._destroyed = false
    this.writeDebounceMs = writeDebounceMs
    this.durability = durability
    this.transactionRunner = transactionRunner
    this._retryCount = 0
    this._maxRetries = 5
    /**
     * @type {Promise<any>|null}
     */
    this._flushPromise = null
    /**
     * @type {Promise<void>|null}
     */
    this._destroyPromise = null
    /**
     * @type {Array<Uint8Array>}
     */
    this._pendingUpdates = []
    this._writing = false
    this._flushScheduled = false
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
      }
      const afterApplyUpdatesCallback = () => {
        if (this._destroyed) return this
        this.synced = true
        this.emit('synced', [this])
      }
      fetchUpdates(this, beforeApplyUpdatesCallback, afterApplyUpdatesCallback).then(() => {
        this._scheduleFlush()
      })
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

    this._unloadListener = () => {
      if (this.db && this._pendingUpdates.length > 0) {
        const batch = this._pendingUpdates.splice(0, this._pendingUpdates.length)
        try {
          const tx = this.db.transaction([updatesStoreName], 'readwrite')
          const store = tx.objectStore(updatesStoreName)
          for (let i = 0; i < batch.length; i++) {
            store.add(batch[i])
          }
          tx.onerror = tx.onabort = () => {
            if (!this._destroyed) {
              this._pendingUpdates = batch.concat(this._pendingUpdates)
            }
          }
        } catch (e) {
          if (!this._destroyed) {
            this._pendingUpdates = batch.concat(this._pendingUpdates)
          }
        }
      }
    }
    if (typeof addEventListener !== 'undefined') {
      addEventListener('pagehide', this._unloadListener)
    }
    if (typeof document !== 'undefined') {
      this._visibilityListener = () => {
        if (document.visibilityState === 'hidden') {
          this._unloadListener()
        }
      }
      document.addEventListener('visibilitychange', this._visibilityListener)
    }
  }

  _scheduleFlush () {
    if (this._destroyed || this._writing || this._pendingUpdates.length === 0) return
    if (this._flushScheduled) return
    this._flushScheduled = true
    if (this.writeDebounceMs > 0) {
      setTimeout(() => {
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

  _flush () {
    if (this._destroyed || this._writing || this._pendingUpdates.length === 0) return
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
    this._flushPromise = transactWrite(this, () => new Promise(resolve => {
      /**
       * @type {IDBTransaction}
       */
      let tx
      try {
        tx = db.transaction([updatesStoreName], 'readwrite', { durability: this.durability })
      } catch (e) {
        this._pendingUpdates = batch.concat(this._pendingUpdates)
        this._writing = false
        this._flushPromise = null
        this.emit('error', [e])
        resolve(undefined)
        return
      }
      const store = tx.objectStore(updatesStoreName)
      for (let i = 0; i < batch.length; i++) {
        store.add(batch[i])
      }
      tx.oncomplete = () => {
        this._retryCount = 0
        this._dbsize += batch.length
        this._writing = false
        this._flushPromise = null
        if (this._pendingUpdates.length > 0) {
          this._scheduleFlush()
        }
        if (this._dbsize >= PREFERRED_TRIM_SIZE) {
          if (this._storeTimeoutId !== null) {
            clearTimeout(this._storeTimeoutId)
          }
          this._storeTimeoutId = setTimeout(() => {
            storeState(this, false)
            this._storeTimeoutId = null
          }, this._storeTimeout)
        }
        resolve(undefined)
      }
      const onErrorOrAbort = () => {
        this._pendingUpdates = batch.concat(this._pendingUpdates)
        this._writing = false
        this._flushPromise = null
        this.emit('error', [tx.error])
        if (!this._destroyed) {
          this._retryCount++
          if (this._retryCount <= this._maxRetries) {
            const backoff = Math.pow(2, this._retryCount) * 100
            setTimeout(() => {
              this._scheduleFlush()
            }, backoff)
          } else {
            this._retryCount = 0
            this.emit('retry-exhausted', [tx.error || new Error('Retry exhausted')])
          }
        }
        resolve(undefined)
      }
      tx.onerror = onErrorOrAbort
      tx.onabort = onErrorOrAbort
    }))
  }

  destroy () {
    if (this._destroyPromise) {
      return this._destroyPromise
    }
    if (this._storeTimeoutId) {
      clearTimeout(this._storeTimeoutId)
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

    const db = this.db
    let flushPromise = Promise.resolve()
    if (db && this._pendingUpdates.length > 0) {
      const batch = this._pendingUpdates.splice(0, this._pendingUpdates.length)
      flushPromise = transactWrite(this, () => new Promise((resolve) => {
        try {
          const tx = db.transaction([updatesStoreName], 'readwrite', { durability: this.durability })
          const store = tx.objectStore(updatesStoreName)
          for (let i = 0; i < batch.length; i++) {
            store.add(batch[i])
          }
          tx.oncomplete = () => resolve(undefined)
          tx.onerror = tx.onabort = () => resolve(undefined)
        } catch (e) {
          resolve(undefined)
        }
      }))
    }

    const activeFlushPromise = this._flushPromise || Promise.resolve()

    this._destroyPromise = Promise.all([flushPromise, activeFlushPromise]).then(() => this._db.then(db => {
      db.close()
    }))
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
      return idb.get(custom, key)
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
        return idb.put(custom, value, key)
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
        return idb.del(custom, key)
      })
    )
  }
}
