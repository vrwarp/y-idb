
import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, PREFERRED_TRIM_SIZE, fetchUpdates } from '../src/y-idb.js'
import * as t from 'lib0/testing.js'
import * as promise from 'lib0/promise.js'
import * as prng from 'lib0/prng.js'
import { runBasicExample } from '../examples/basic.js'
import { runDebounceExample } from '../examples/debounce.js'
import { runErrorHandlingExample } from '../examples/error-handling.js'
import { runDurabilityExample } from '../examples/durability.js'

/**
 * @param {t.TestCase} tc
 */
export const testPerf = async tc => {
  await t.measureTimeAsync('time to create a y-indexeddb instance', async () => {
    const ydoc = new Y.Doc()
    const provider = new IndexeddbPersistence(tc.testName, ydoc)
    await provider.whenSynced
    provider.destroy()
  })
}

/**
 * @param {t.TestCase} tc
 */
export const testIdbUpdateAndMerge = async tc => {
  await clearDocument(tc.testName)
  const doc1 = new Y.Doc()
  const arr1 = doc1.getArray('t')
  const doc2 = new Y.Doc()
  const arr2 = doc2.getArray('t')
  arr1.insert(0, [0])
  const persistence1 = new IndexeddbPersistence(tc.testName, doc1)
  persistence1._storeTimeout = 0
  await persistence1.whenSynced
  arr1.insert(0, [1])
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  persistence2._storeTimeout = 0
  let calledObserver = false
  // @ts-ignore
  arr2.observe((event, tr) => {
    t.assert(!tr.local)
    t.assert(tr.origin === persistence2)
    calledObserver = true
  })
  await persistence2.whenSynced
  t.assert(calledObserver)
  t.assert(arr2.length === 2)
  for (let i = 2; i < PREFERRED_TRIM_SIZE + 1; i++) {
    arr1.insert(i, [i])
  }
  await promise.wait(100)
  await fetchUpdates(persistence2)
  t.assert(arr2.length === PREFERRED_TRIM_SIZE + 1)
  t.assert(persistence1._dbsize === 1) // wait for dbsize === 0. db should be concatenated
  persistence1.destroy()
  persistence2.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testIdbConcurrentMerge = async tc => {
  await clearDocument(tc.testName)
  const doc1 = new Y.Doc()
  const arr1 = doc1.getArray('t')
  const doc2 = new Y.Doc()
  const arr2 = doc2.getArray('t')
  arr1.insert(0, [0])
  const persistence1 = new IndexeddbPersistence(tc.testName, doc1)
  persistence1._storeTimeout = 0
  await persistence1.whenSynced
  arr1.insert(0, [1])
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  persistence2._storeTimeout = 0
  await persistence2.whenSynced
  t.assert(arr2.length === 2)
  arr1.insert(0, ['left'])
  for (let i = 0; i < PREFERRED_TRIM_SIZE + 1; i++) {
    arr1.insert(i, [i])
  }
  arr2.insert(0, ['right'])
  for (let i = 0; i < PREFERRED_TRIM_SIZE + 1; i++) {
    arr2.insert(i, [i])
  }
  await promise.wait(100)
  await fetchUpdates(persistence1)
  await fetchUpdates(persistence2)
  t.assert(persistence1._dbsize < 10)
  t.assert(persistence2._dbsize < 10)
  t.compareArrays(arr1.toArray(), arr2.toArray())
  persistence1.destroy()
  persistence2.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testMetaStorage = async tc => {
  await clearDocument(tc.testName)
  const ydoc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, ydoc)
  persistence.set('a', 4)
  persistence.set(4, 'meta!')
  // @ts-ignore
  persistence.set('obj', { a: 4 })
  const resA = await persistence.get('a')
  t.assert(resA === 4)
  const resB = await persistence.get(4)
  t.assert(resB === 'meta!')
  const resC = await persistence.get('obj')
  t.compareObjects(resC, { a: 4 })
  persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testEarlyDestroy = async tc => {
  let hasbeenSyced = false
  const ydoc = new Y.Doc()
  const indexDBProvider = new IndexeddbPersistence(tc.testName, ydoc)
  indexDBProvider.on('synced', () => {
    hasbeenSyced = true
  })
  indexDBProvider.destroy()
  await new Promise((resolve) => setTimeout(resolve, 500))
  t.assert(!hasbeenSyced)
}

/**
 * @param {t.TestCase} tc
 */
export const testBoundedConcurrencyCoalescing = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const arr = doc.getArray('t')
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  // Spy on transaction creation
  let txCount = 0
  const db = /** @type {IDBDatabase} */ (persistence.db)
  const originalTransaction = db.transaction
  db.transaction = function (storeNames, mode, options) {
    if (storeNames.includes('updates') && mode === 'readwrite') {
      txCount++
    }
    return originalTransaction.call(this, storeNames, mode, options)
  }

  // Perform multiple synchronous updates
  arr.insert(0, [1])
  arr.insert(1, [2])
  arr.insert(2, [3])

  // Wait for the microtask flush to complete
  await new Promise(resolve => setTimeout(resolve, 50))

  t.assert(txCount === 1, 'Should only create 1 transaction for all updates')
  t.assert(persistence._pendingUpdates.length === 0, 'Pending updates should be flushed')

  // Verify contents are correctly written
  const doc2 = new Y.Doc()
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced
  t.compareArrays(doc2.getArray('t').toArray(), [1, 2, 3])

  persistence.destroy()
  persistence2.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testWriteDebounce = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const arr = doc.getArray('t')
  const persistence = new IndexeddbPersistence(tc.testName, doc, { writeDebounceMs: 50 })
  await persistence.whenSynced

  // Spy on transaction creation
  let txCount = 0
  const db = /** @type {IDBDatabase} */ (persistence.db)
  const originalTransaction = db.transaction
  db.transaction = function (storeNames, mode, options) {
    if (storeNames.includes('updates') && mode === 'readwrite') {
      txCount++
    }
    return originalTransaction.call(this, storeNames, mode, options)
  }

  // Perform updates
  arr.insert(0, [1])

  // Wait a short time less than debounce
  await new Promise(resolve => setTimeout(resolve, 20))
  t.assert(txCount === 0, 'Should not have written yet because of debounce')
  t.assert(persistence._pendingUpdates.length === 1, 'Should have 1 pending update')

  // Perform another update
  arr.insert(1, [2])

  // Wait for debounce to expire
  await new Promise(resolve => setTimeout(resolve, 60))
  t.assert(txCount === 1, 'Should have flushed both updates now')
  t.assert(persistence._pendingUpdates.length === 0, 'Pending updates should be empty')

  persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testErrorEmission = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const arr = doc.getArray('t')
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  // Mock transaction failure
  const db = /** @type {IDBDatabase} */ (persistence.db)
  db.transaction = () => {
    throw new Error('Simulated write failure')
  }

  let errorEmitted = false
  persistence.on('error', () => {
    errorEmitted = true
  })

  // Trigger update
  arr.insert(0, [1])

  // Wait for the microtask flush to run
  await new Promise(resolve => setTimeout(resolve, 20))
  t.assert(errorEmitted, 'Should have emitted an error')
  t.assert(persistence._pendingUpdates.length === 1, 'Failed updates should be re-buffered')

  persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testFuzzingLoad = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const arr = doc.getArray('t')
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  const gen = tc.prng
  const numUpdates = 200

  // We perform lots of rapid concurrent updates
  for (let i = 0; i < numUpdates; i++) {
    const op = prng.int31(gen, 0, 2)
    if (op === 0) {
      const len = prng.int31(gen, 1, 5)
      const values = []
      for (let j = 0; j < len; j++) {
        values.push(prng.int31(gen, 0, 1000))
      }
      const index = prng.int31(gen, 0, arr.length)
      arr.insert(index, values)
    } else if (op === 1 && arr.length > 0) {
      const index = prng.int31(gen, 0, arr.length - 1)
      const len = prng.int31(gen, 1, Math.min(5, arr.length - index))
      arr.delete(index, len)
    } else {
      await promise.wait(prng.int31(gen, 0, 5))
    }
  }

  // Wait for all writes to flush
  await promise.wait(100)

  // Verify that the data is correctly retrieved in another persistence instance
  const doc2 = new Y.Doc()
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced

  t.compareArrays(doc.getArray('t').toArray(), doc2.getArray('t').toArray())

  persistence.destroy()
  persistence2.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testFuzzingConcurrentLoad = async tc => {
  await clearDocument(tc.testName)

  const doc1 = new Y.Doc()
  const doc2 = new Y.Doc()
  const arr1 = doc1.getArray('t')
  const arr2 = doc2.getArray('t')

  const persistence1 = new IndexeddbPersistence(tc.testName, doc1)
  await persistence1.whenSynced

  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced

  const gen = tc.prng
  const numUpdates = 100

  // Run two concurrent insertion/deletion loops with random interleaving and document syncing
  /**
   * @param {Y.Doc} doc
   * @param {Y.Array<any>} arr
   * @param {IndexeddbPersistence} persistence
   */
  const runMutations = async (doc, arr, persistence) => {
    for (let i = 0; i < numUpdates; i++) {
      const op = prng.int31(gen, 0, 2)
      if (op === 0) {
        const len = prng.int31(gen, 1, 3)
        const values = []
        for (let j = 0; j < len; j++) {
          values.push(prng.int31(gen, 0, 1000))
        }
        arr.insert(prng.int31(gen, 0, arr.length), values)
      } else if (op === 1 && arr.length > 0) {
        const index = prng.int31(gen, 0, arr.length - 1)
        const len = prng.int31(gen, 1, Math.min(3, arr.length - index))
        arr.delete(index, len)
      }

      // Randomly sync via fetchUpdates to simulate fetching concurrent changes
      if (prng.int31(gen, 0, 10) === 0) {
        await fetchUpdates(persistence)
      }

      // Random delay
      await promise.wait(prng.int31(gen, 0, 10))
    }
  }

  await Promise.all([
    runMutations(doc1, arr1, persistence1),
    runMutations(doc2, arr2, persistence2)
  ])

  // Final sync
  await promise.wait(150)
  await fetchUpdates(persistence1)
  await fetchUpdates(persistence2)

  // Both documents should converge to the exact same state
  t.compareArrays(arr1.toArray(), arr2.toArray())

  persistence1.destroy()
  persistence2.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testExampleBasic = async tc => {
  await runBasicExample(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced
  t.compareArrays(doc.getArray('tasks').toArray(), ['Buy milk', 'Read a book'])
  persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testExampleDebounce = async tc => {
  await runDebounceExample(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced
  t.assert(doc.getText('editor').toString() === 'Hello World')
  persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testExampleErrorHandling = async tc => {
  await runErrorHandlingExample(tc.testName)
}

/**
 * @param {t.TestCase} tc
 */
export const testExampleDurability = async tc => {
  await runDurabilityExample(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced
  t.compareArrays(doc.getArray('durability-store').toArray(), ['Final save state'])
  persistence.destroy()
}
