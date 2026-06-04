/* eslint-env browser */

import * as Y from 'yjs'
import * as idb from 'lib0/indexeddb'
import * as promise from 'lib0/promise'
import { Observable } from 'lib0/observable'

const customStoreName = 'custom'
const updatesStoreName = 'updates'

export const PREFERRED_TRIM_SIZE = 500

/**
 * @param {IndexeddbPersistence} idbPersistence
 * @param {function(IDBObjectStore):void} [beforeApplyUpdatesCallback]
 * @param {function(IDBObjectStore):void} [afterApplyUpdatesCallback]
 */
export const fetchUpdates = (idbPersistence, beforeApplyUpdatesCallback = () => {}, afterApplyUpdatesCallback = () => {}) => {
  const [updatesStore] = idb.transact(/** @type {IDBDatabase} */ (idbPersistence.db), [updatesStoreName]) // , 'readonly')
  return idb.getAll(updatesStore, idb.createIDBKeyRangeLowerBound(idbPersistence._dbref, false)).then(updates => {
    if (!idbPersistence._destroyed) {
      beforeApplyUpdatesCallback(updatesStore)
      Y.transact(idbPersistence.doc, () => {
        updates.forEach(val => Y.applyUpdate(idbPersistence.doc, val))
      }, idbPersistence, false)
      afterApplyUpdatesCallback(updatesStore)
    }
  })
    .then(() => idb.getLastKey(updatesStore).then(lastKey => { idbPersistence._dbref = lastKey + 1 }))
    .then(() => idb.count(updatesStore).then(cnt => { idbPersistence._dbsize = cnt }))
    .then(() => updatesStore)
}

/**
 * @param {IndexeddbPersistence} idbPersistence
 * @param {boolean} forceStore
 */
export const storeState = (idbPersistence, forceStore = true) =>
  fetchUpdates(idbPersistence)
    .then(updatesStore => {
      if (forceStore || idbPersistence._dbsize >= PREFERRED_TRIM_SIZE) {
        idb.addAutoKey(updatesStore, Y.encodeStateAsUpdate(idbPersistence.doc))
          .then(() => idb.del(updatesStore, idb.createIDBKeyRangeUpperBound(idbPersistence._dbref, true)))
          .then(() => idb.count(updatesStore).then(cnt => { idbPersistence._dbsize = cnt }))
      }
    })

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
   */
  constructor (name, doc, { writeDebounceMs = 0 } = {}) {
    super()
    this.doc = doc
    this.name = name
    this._dbref = 0
    this._dbsize = 0
    this._destroyed = false
    this.writeDebounceMs = writeDebounceMs
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
      const beforeApplyUpdatesCallback = (updatesStore) => idb.addAutoKey(updatesStore, Y.encodeStateAsUpdate(doc))
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
            idb.addAutoKey(store, batch[i])
          }
        } catch (e) {
          // ignore
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
      this._scheduleFlush()
      return
    }
    this._writing = true
    const batch = this._pendingUpdates
    this._pendingUpdates = []
    /**
     * @type {IDBTransaction}
     */
    let tx
    try {
      tx = db.transaction([updatesStoreName], 'readwrite')
    } catch (e) {
      this._pendingUpdates = batch.concat(this._pendingUpdates)
      this._writing = false
      this.emit('error', [e])
      return
    }
    const store = tx.objectStore(updatesStoreName)
    for (let i = 0; i < batch.length; i++) {
      idb.addAutoKey(store, batch[i])
    }
    tx.oncomplete = () => {
      this._dbsize += batch.length
      this._writing = false
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
    }
    const onErrorOrAbort = () => {
      this._pendingUpdates = batch.concat(this._pendingUpdates)
      this._writing = false
      this.emit('error', [tx.error])
      if (!this._destroyed) {
        setTimeout(() => {
          this._scheduleFlush()
        }, 500)
      }
    }
    tx.onerror = onErrorOrAbort
    tx.onabort = onErrorOrAbort
  }

  destroy () {
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
      flushPromise = new Promise((resolve) => {
        try {
          const tx = db.transaction([updatesStoreName], 'readwrite')
          const store = tx.objectStore(updatesStoreName)
          for (let i = 0; i < batch.length; i++) {
            idb.addAutoKey(store, batch[i])
          }
          tx.oncomplete = () => resolve()
          tx.onerror = tx.onabort = () => resolve()
        } catch (e) {
          resolve()
        }
      })
    }

    return flushPromise.then(() => this._db.then(db => {
      db.close()
    }))
  }

  /**
   * Destroys this instance and removes all data from indexeddb.
   *
   * @return {Promise<void>}
   */
  clearData () {
    return this.destroy().then(() => {
      idb.deleteDB(this.name)
    })
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
    return this._db.then(db => {
      const [custom] = idb.transact(db, [customStoreName])
      return idb.put(custom, value, key)
    })
  }

  /**
   * @param {String | number | ArrayBuffer | Date} key
   * @return {Promise<undefined>}
   */
  del (key) {
    return this._db.then(db => {
      const [custom] = idb.transact(db, [customStoreName])
      return idb.del(custom, key)
    })
  }
}
