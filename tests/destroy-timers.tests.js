import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'

/**
 * Track every timer armed through the global setTimeout while `fn` runs.
 * A timer counts as live until its callback has run or it has been passed to
 * clearTimeout. Whatever is still live when `fn` settles is cancelled with the
 * real clearTimeout, so a leaked timer cannot hold the process open.
 *
 * @param {function(Map<any, number>):Promise<void>} fn receives the live
 *   timers (handle -> delay in ms)
 */
const withTrackedTimers = async fn => {
  const realSetTimeout = globalThis.setTimeout
  const realClearTimeout = globalThis.clearTimeout
  /**
   * @type {Map<any, number>}
   */
  const live = new Map()
  /**
   * @param {function(...any):void} handler
   * @param {number} [ms]
   * @param {...any} args
   */
  const trackedSetTimeout = (handler, ms, ...args) => {
    /**
     * @type {any}
     */
    const handle = realSetTimeout((/** @type {Array<any>} */ ...a) => {
      live.delete(handle)
      handler(...a)
    }, ms, ...args)
    live.set(handle, ms || 0)
    return handle
  }
  /**
   * @param {any} handle
   */
  const trackedClearTimeout = handle => {
    live.delete(handle)
    realClearTimeout(handle)
  }
  globalThis.setTimeout = /** @type {any} */ (trackedSetTimeout)
  globalThis.clearTimeout = /** @type {any} */ (trackedClearTimeout)
  try {
    await fn(live)
  } finally {
    globalThis.setTimeout = realSetTimeout
    globalThis.clearTimeout = realClearTimeout
    live.forEach((_ms, handle) => realClearTimeout(handle))
    live.clear()
  }
}

/**
 * Let already-due work (zero-delay timers, IndexedDB request callbacks) run so
 * only genuinely long-lived timers remain.
 */
const settle = () => new Promise(resolve => setImmediate(resolve))

/**
 * destroy() must tear down every timer the provider armed, including the
 * writeDebounceMs timer scheduled by an edit made inside the debounce window.
 * A surviving timer keeps the destroyed provider, its doc and its buffers
 * reachable until it fires and keeps a Node process alive for up to
 * writeDebounceMs after teardown.
 *
 * @param {t.TestCase} tc
 */
export const testDestroyClearsWriteDebounceTimer = async tc => {
  await clearDocument(tc.testName)
  await withTrackedTimers(async live => {
    const doc = new Y.Doc()
    const persistence = new IndexeddbPersistence(tc.testName, doc, { writeDebounceMs: 60_000 })
    await persistence.whenSynced
    await settle()

    // Edit inside the debounce window, then tear down before it elapses.
    doc.getArray('t').insert(0, ['edit-before-destroy'])
    await persistence.destroy()
    await settle()

    const leaked = Array.from(live.values())
    t.assert(!leaked.includes(60_000), 'writeDebounceMs timer survived destroy()')
    t.assert(
      leaked.length === 0,
      `destroy() must clear all pending timers, but ${leaked.length} survived (delays: ${leaked.join(', ')} ms)`
    )

    // The edit is still persisted by destroy()'s final write.
    const doc2 = new Y.Doc()
    const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
    await persistence2.whenSynced
    t.compare(doc2.getArray('t').toArray(), ['edit-before-destroy'])
    await persistence2.destroy()
  })
}
