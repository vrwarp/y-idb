/* eslint-env browser */

import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * Hydration cost pinned by counters (no timings).
 *
 * Yjs encodes a V1 update for every transaction while the doc has an
 * 'update' observer (`cleanupTransactions`:
 * `if (doc._observers.has('update'))`). The provider's own
 * `doc.on('update', _storeUpdate)` is such an observer, so the hydration
 * transaction — every stored row applied in ONE `Y.transact` with origin ===
 * provider — makes Yjs encode an update as large as the whole loaded
 * document, which `_storeUpdate` then drops because of its origin. When
 * y-idb is the only 'update' listener during hydration that encode (and its
 * doc-sized buffer) is pure waste, paid on every cold start.
 *
 * The first test pins the waste (V1 and V2 encodes alike); the others are
 * guards a fix must keep green (another listener still gets the hydration
 * update; writes that observers and doc handlers make in reaction to the
 * load are still persisted; a listener added during the load still runs
 * after the provider's; a throwing observer does not leave the provider
 * deaf to later edits; a provider destroyed during the load stays
 * detached).
 */

const BATCHES = 40
const PER_BATCH = 50

/**
 * Persist a deterministic document as BATCHES separate update rows (one
 * flush per transaction, like a session saving edits), with a hot key
 * overwritten in every batch so the rows carry deletions too.
 *
 * @param {string} name
 */
const seedRows = async name => {
  await clearDocument(name)
  const doc = new Y.Doc()
  const provider = new IndexeddbPersistence(name, doc)
  await provider.whenSynced
  const m = doc.getMap('m')
  for (let b = 0; b < BATCHES; b++) {
    doc.transact(() => {
      for (let i = 0; i < PER_BATCH; i++) m.set(`k${b}-${i}`, `value ${b}/${i} with some padding text`)
      m.set('hot', b)
    })
    await provider.flush()
  }
  await provider.destroy()
  doc.destroy()
}

/**
 * Bytes and count of the stored update rows (raw read, no provider).
 *
 * @param {string} name
 * @return {Promise<{ rows: number, bytes: number }>}
 */
const storedRows = name => new Promise((resolve, reject) => {
  const req = indexedDB.open(name)
  req.onerror = () => reject(req.error)
  req.onsuccess = () => {
    const db = req.result
    const tx = db.transaction(['updates'], 'readonly')
    const all = tx.objectStore('updates').getAll()
    tx.oncomplete = () => {
      db.close()
      resolve({ rows: all.result.length, bytes: all.result.reduce((a, r) => a + r.byteLength, 0) })
    }
    tx.onerror = () => {
      db.close()
      reject(tx.error)
    }
  }
})

/**
 * Yjs does not export UpdateEncoderV2: take its prototype from the encoder
 * a V2 encode hands to Item#write.
 *
 * @return {any}
 */
const updateEncoderV2Prototype = () => {
  const proto = /** @type {any} */ (Y.Item.prototype)
  const write = proto.write
  /** @type {any} */
  let found = null
  proto.write = function (/** @type {any} */ encoder, /** @type {number} */ offset) {
    found = Object.getPrototypeOf(encoder)
    return write.call(this, encoder, offset)
  }
  try {
    const doc = new Y.Doc()
    doc.getMap('m').set('k', 1)
    Y.encodeStateAsUpdateV2(doc)
  } finally {
    proto.write = write
  }
  t.assert(found !== null && typeof found.toUint8Array === 'function', 'found the UpdateEncoderV2 prototype')
  return found
}

/**
 * Run `fn` while counting the update bytes Yjs produces, per encoder.
 * UpdateEncoderV1 inherits toUint8Array from DSEncoderV1; an own override
 * on its prototype sees every V1 update encode (Yjs' 'update' events,
 * encodeStateAsUpdate, mergeUpdates) and no state-vector encode.
 * UpdateEncoderV2 has its own toUint8Array ('updateV2' events, V2 encodes);
 * counting it too means moving the work to V2 cannot pass for a fix.
 *
 * @param {() => Promise<void>} fn
 * @return {Promise<{ v1: { calls: number, bytes: number }, v2: { calls: number, bytes: number } }>}
 */
const countUpdateEncodes = async fn => {
  /**
   * @param {any} proto
   * @param {{ calls: number, bytes: number }} counter
   * @return {() => void} restores the prototype
   */
  const wrap = (proto, counter) => {
    const own = Object.getOwnPropertyDescriptor(proto, 'toUint8Array')
    const original = proto.toUint8Array
    proto.toUint8Array = function () {
      const out = original.call(this)
      counter.calls++
      counter.bytes += out.byteLength
      return out
    }
    return () => {
      if (own) {
        Object.defineProperty(proto, 'toUint8Array', own)
      } else {
        Reflect.deleteProperty(proto, 'toUint8Array')
      }
    }
  }
  const counters = { v1: { calls: 0, bytes: 0 }, v2: { calls: 0, bytes: 0 } }
  const restoreV1 = wrap(Y.UpdateEncoderV1.prototype, counters.v1)
  const restoreV2 = wrap(updateEncoderV2Prototype(), counters.v2)
  try {
    await fn()
  } finally {
    restoreV2()
    restoreV1()
  }
  return counters
}

/**
 * Record every 'update' and 'updateV2' event Yjs emits on `doc` WITHOUT
 * registering an observer (registering one would itself switch the encode
 * on).
 *
 * @param {Y.Doc} doc
 * @return {Array<{ name: string, origin: any, bytes: number }>}
 */
const recordUpdateEmits = doc => {
  /** @type {Array<{ name: string, origin: any, bytes: number }>} */
  const emitted = []
  const target = /** @type {any} */ (doc)
  const emit = target.emit
  target.emit = function (/** @type {string} */ name, /** @type {Array<any>} */ args) {
    if (name === 'update' || name === 'updateV2') emitted.push({ name, origin: args[1], bytes: args[0].byteLength })
    return emit.call(this, name, args)
  }
  return emitted
}

/**
 * Cold start with y-idb as the doc's only 'update' listener (e.g. the app
 * awaits `whenSynced` before constructing its network provider): hydration
 * must not make Yjs encode the loaded document just to have `_storeUpdate`
 * discard it.
 *
 * @param {t.TestCase} tc
 */
export const testHydrationDoesNotEncodeLoadedDocForOwnListener = async tc => {
  await seedRows(tc.testName)
  const stored = await storedRows(tc.testName)
  t.assert(stored.rows >= BATCHES, `expected one row per batch, got ${stored.rows}`)

  const doc = new Y.Doc()
  const emitted = recordUpdateEmits(doc)
  /** @type {IndexeddbPersistence|null} */
  let provider = null
  const enc = await countUpdateEncodes(async () => {
    provider = new IndexeddbPersistence(tc.testName, doc)
    await provider.whenSynced
  })
  const p = /** @type {IndexeddbPersistence} */ (/** @type {unknown} */ (provider))
  try {
    t.compare(doc.getMap('m').size, BATCHES * PER_BATCH + 1, 'every stored row was loaded')
    const docBytes = Y.encodeStateAsUpdate(doc).byteLength
    const discarded = emitted.filter(e => e.origin === p)
    const discardedBytes = discarded.reduce((a, e) => a + e.bytes, 0)
    const encBytes = enc.v1.bytes + enc.v2.bytes
    t.assert(p._pendingUpdates.length === 0, 'nothing loaded is queued for writing back')
    // A fixed hydration still encodes the initial-state probe (2 bytes for
    // an empty doc); the waste is an encode the size of the document.
    t.assert(
      encBytes * 100 < stored.bytes,
      `hydration made Yjs encode ${encBytes} bytes in ${enc.v1.calls} V1 and ${enc.v2.calls} V2 update encodes ` +
      `(${(100 * encBytes / docBytes).toFixed(0)}% of the ${docBytes}-byte doc; ${stored.rows} rows / ${stored.bytes} bytes loaded); ` +
      `${discarded.length} provider-origin 'update'/'updateV2' event(s) of ${discardedBytes} bytes were emitted`
    )
    t.assert(discarded.length === 0, `hydration emitted ${discarded.length} provider-origin 'update'/'updateV2' event(s) of ${discardedBytes} bytes`)
  } finally {
    await p.destroy()
  }
}

/**
 * Guard (passes before and after a fix): when another 'update' listener is
 * attached before hydration (e.g. a network provider constructed first),
 * it still receives the hydration update, with the provider as origin, and
 * the encode counter sees it — so the test above measures a real encode.
 *
 * @param {t.TestCase} tc
 */
export const testHydrationStillEmitsUpdateForOtherListeners = async tc => {
  await seedRows(tc.testName)
  const doc = new Y.Doc()
  /** @type {Array<{ update: Uint8Array, origin: any }>} */
  const received = []
  doc.on('update', (/** @type {Uint8Array} */ update, /** @type {any} */ origin) => { received.push({ update, origin }) })
  /** @type {IndexeddbPersistence|null} */
  let provider = null
  const enc = await countUpdateEncodes(async () => {
    provider = new IndexeddbPersistence(tc.testName, doc)
    await provider.whenSynced
  })
  const p = /** @type {IndexeddbPersistence} */ (/** @type {unknown} */ (provider))
  try {
    t.assert(received.length === 1, `other listener got ${received.length} hydration updates`)
    t.assert(received[0].origin === p, 'the hydration update carries the provider as origin')
    const replica = new Y.Doc()
    Y.applyUpdate(replica, received[0].update)
    t.compare(replica.getMap('m').toJSON(), doc.getMap('m').toJSON(), 'the hydration update holds the loaded document')
    t.assert(enc.v1.bytes >= received[0].update.byteLength, 'the encode counter sees the hydration encode')
  } finally {
    await p.destroy()
  }
}

/**
 * Guard: writes that app code makes IN REACTION to the load — from a type
 * observer, or from an 'afterTransactionCleanup' handler — run as their own
 * transactions inside the hydration's cleanup, and Yjs emits their 'update'
 * before the hydration `Y.transact` returns. They must still be persisted
 * (a fix that takes the provider's listener off around the hydration
 * transaction has to have it back before those updates are emitted).
 *
 * @param {t.TestCase} tc
 */
export const testWritesMadeInReactionToLoadArePersisted = async tc => {
  await seedRows(tc.testName)
  const doc = new Y.Doc()
  /** @type {IndexeddbPersistence|null} */
  let provider = null
  doc.getMap('m').observe(() => {
    if (!doc.getMap('derived').has('fromObserver')) doc.getMap('derived').set('fromObserver', doc.getMap('m').size)
  })
  doc.on('afterTransactionCleanup', (/** @type {Y.Transaction} */ tr) => {
    if (tr.origin === provider && !doc.getMap('derived').has('fromCleanup')) doc.getMap('derived').set('fromCleanup', true)
  })
  provider = new IndexeddbPersistence(tc.testName, doc)
  await provider.whenSynced
  t.compare(doc.getMap('derived').toJSON(), { fromObserver: BATCHES * PER_BATCH + 1, fromCleanup: true }, 'reactions ran during hydration')
  await provider.flush()
  await provider.destroy()

  const reloaded = new Y.Doc()
  const reloader = new IndexeddbPersistence(tc.testName, reloaded)
  try {
    await reloader.whenSynced
    t.compare(reloaded.getMap('derived').toJSON(), { fromObserver: BATCHES * PER_BATCH + 1, fromCleanup: true }, 'writes made in reaction to the load were persisted')
  } finally {
    await reloader.destroy()
  }
}

/**
 * Guard: as above, for writes from an observeDeep handler and from
 * 'afterTransaction' / 'afterAllTransactions' handlers. Each reaction gets
 * a cold start of its own, so none relies on another one's transaction to
 * have the provider listening again before its update is emitted.
 *
 * @param {t.TestCase} tc
 */
export const testWritesFromDeepObserversAndDocHandlersArePersisted = async tc => {
  await seedRows(tc.testName)
  /**
   * @typedef {(doc: Y.Doc, isLoad: (tr: Y.Transaction) => boolean, write: () => void) => void} React
   * @type {Array<[string, React]>}
   */
  const reactions = [
    ['observeDeep', (doc, isLoad, write) => doc.getMap('m').observeDeep((_events, tr) => { if (isLoad(tr)) write() })],
    ['afterTransaction', (doc, isLoad, write) => doc.on('afterTransaction', (/** @type {Y.Transaction} */ tr) => { if (isLoad(tr)) write() })],
    ['afterAllTransactions', (doc, isLoad, write) => doc.on('afterAllTransactions', (/** @type {Y.Doc} */ _doc, /** @type {Array<Y.Transaction>} */ trs) => { if (trs.some(isLoad)) write() })]
  ]
  for (const [kind, react] of reactions) {
    const doc = new Y.Doc()
    /** @type {IndexeddbPersistence|null} */
    let provider = null
    const derived = doc.getMap('derived')
    react(doc, tr => provider !== null && tr.origin === provider, () => {
      if (!derived.has(kind)) derived.set(kind, true)
    })
    provider = new IndexeddbPersistence(tc.testName, doc)
    await provider.whenSynced
    t.assert(derived.get(kind) === true, `${kind} reaction ran during hydration`)
    await provider.flush()
    await provider.destroy()
  }

  const reloaded = new Y.Doc()
  const reloader = new IndexeddbPersistence(tc.testName, reloaded)
  try {
    await reloader.whenSynced
    t.compare(reloaded.getMap('derived').toJSON(), { observeDeep: true, afterTransaction: true, afterAllTransactions: true }, 'writes made in reaction to the load were persisted')
  } finally {
    await reloader.destroy()
  }
}

/**
 * Guard: an 'update' listener that app code adds in reaction to the load
 * (e.g. a network provider constructed in an observer) comes after the
 * provider's, as it always did: the provider sees every later update first,
 * so a listener that throws cannot keep an edit from being persisted.
 *
 * @param {t.TestCase} tc
 */
export const testListenerAddedDuringHydrationRunsAfterProvider = async tc => {
  await seedRows(tc.testName)
  const doc = new Y.Doc()
  /** @type {IndexeddbPersistence|null} */
  let provider = null
  let added = false
  /** @type {Array<boolean>} */
  const queuedFirst = []
  doc.getMap('m').observe(() => {
    if (added) return
    added = true
    doc.on('update', (/** @type {Uint8Array} */ update, /** @type {any} */ origin) => {
      const p = /** @type {IndexeddbPersistence} */ (/** @type {unknown} */ (provider))
      if (origin !== p) queuedFirst.push(p._pendingUpdates.includes(update))
    })
  })
  provider = new IndexeddbPersistence(tc.testName, doc)
  try {
    await provider.whenSynced
    doc.getMap('after').set('edit', 1)
    t.assert(added, 'the listener was added during hydration')
    t.compare(queuedFirst, [true], 'the provider queued the edit before the listener added during hydration saw it')
  } finally {
    await provider.destroy()
  }
}

/**
 * Guard: an app observer that throws while the rows are applied makes the
 * hydration `Y.transact` throw (see hydration-error.tests.js). The provider
 * must still be listening afterwards: later edits are persisted.
 *
 * @param {t.TestCase} tc
 */
export const testEditsAfterThrowingHydrationObserverArePersisted = async tc => {
  await seedRows(tc.testName)
  const doc = new Y.Doc()
  let thrown = false
  doc.getMap('m').observe(() => {
    if (!thrown) {
      thrown = true
      throw new Error('mirror failed')
    }
  })
  const provider = new IndexeddbPersistence(tc.testName, doc)
  /** @type {Array<any>} */
  const errors = []
  provider.on('error', (/** @type {any} */ err) => { errors.push(err) })
  await provider.whenSynced
  t.assert(thrown, 'observer threw during hydration')
  doc.getMap('after').set('edit', 'kept')
  await provider.flush()
  await provider.destroy()
  t.assert(errors.some(err => err instanceof Error && err.message === 'mirror failed'), 'hydration failure reported')

  const reloaded = new Y.Doc()
  const reloader = new IndexeddbPersistence(tc.testName, reloaded)
  try {
    await reloader.whenSynced
    t.compare(reloaded.getMap('after').get('edit'), 'kept', 'an edit made after the failed hydration was persisted')
  } finally {
    await reloader.destroy()
  }
}

/**
 * Guard: an app observer may destroy the provider in reaction to the load
 * (e.g. tearing the doc down on a schema mismatch). destroy() detaches
 * `_storeUpdate`; nothing may re-attach it once the hydration transaction
 * ends, or the destroyed provider keeps buffering every later edit.
 *
 * @param {t.TestCase} tc
 */
export const testProviderDestroyedDuringHydrationStaysDetached = async tc => {
  await seedRows(tc.testName)
  const doc = new Y.Doc()
  /** @type {(p: Promise<void>) => void} */
  let resolveDestroy = () => {}
  /** @type {Promise<void>} */
  const destroying = new Promise(resolve => { resolveDestroy = resolve })
  /** @type {IndexeddbPersistence|null} */
  let provider = null
  doc.getMap('m').observe(() => {
    if (provider !== null) resolveDestroy(provider.destroy())
  })
  provider = new IndexeddbPersistence(tc.testName, doc)
  const p = provider
  await destroying
  t.compare(doc.getMap('m').size, BATCHES * PER_BATCH + 1, 'the rows were applied before the observer destroyed the provider')
  doc.getMap('later').set('edit', 1)
  t.assert(p._pendingUpdates.length === 0, 'a destroyed provider does not buffer later edits')
  doc.destroy()
}
