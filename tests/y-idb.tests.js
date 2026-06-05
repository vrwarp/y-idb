import * as Y from 'yjs'
import { IndexeddbPersistence, clearDocument, PREFERRED_TRIM_SIZE, fetchUpdates, storeState } from '../src/y-idb.js'
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
    await provider.destroy()
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
  await persistence1.destroy()
  await persistence2.destroy()
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
  await persistence1.destroy()
  await persistence2.destroy()
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
  await persistence.destroy()
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
  await indexDBProvider.destroy()
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

  await persistence.destroy()
  await persistence2.destroy()
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

  await persistence.destroy()
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

  await persistence.destroy()
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

  await persistence.destroy()
  await persistence2.destroy()
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

  await persistence1.destroy()
  await persistence2.destroy()
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
  await persistence.destroy()
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
  await persistence.destroy()
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
  await persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testEmptyStoreSync = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced
  t.assert(!isNaN(persistence._dbref), 'dbref should not be NaN')
  t.assert(persistence._dbref === 0, 'dbref should be 0 for empty store')
  await persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testDoubleDestroy = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced
  const p1 = persistence.destroy()
  const p2 = persistence.destroy()
  t.assert(p1 === p2, 'should return the same cached promise')
  await p1
}

/**
 * @param {t.TestCase} tc
 */
export const testClearDataRoundTrip = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced
  await persistence.set('meta', 'value')
  await persistence.destroy()

  // Open, check value, clear data
  const doc2 = new Y.Doc()
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced
  const val = await persistence2.get('meta')
  t.assert(val === 'value')
  await persistence2.clearData()

  // Open again, check value is gone
  const doc3 = new Y.Doc()
  const persistence3 = new IndexeddbPersistence(tc.testName, doc3)
  await persistence3.whenSynced
  const val2 = await persistence3.get('meta')
  t.assert(val2 === undefined)
  await persistence3.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testTrimDeletesOldKeys = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const arr = doc.getArray('t')
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  persistence._storeTimeout = 0
  await persistence.whenSynced

  // Write enough updates to trigger trim
  for (let i = 0; i < PREFERRED_TRIM_SIZE + 10; i++) {
    arr.insert(0, [i])
  }

  // Wait for debounced write and trim to finish
  await promise.wait(150)
  await storeState(persistence, false)

  t.assert(persistence._dbref > PREFERRED_TRIM_SIZE)
  t.assert(persistence._dbsize < 15)
  await persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testErrorRetryBackoff = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const arr = doc.getArray('t')
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  const db = /** @type {IDBDatabase} */ (persistence.db)
  const originalTransaction = db.transaction
  db.transaction = () => {
    throw new Error('Persistent failure')
  }

  let errorCount = 0
  persistence.on('error', () => {
    errorCount++
  })

  arr.insert(0, [1])
  await promise.wait(400)
  t.assert(errorCount > 0)
  t.assert(persistence._pendingUpdates.length === 1)

  // Restore transaction capability to avoid unhandled exceptions during destroy
  db.transaction = originalTransaction
  await persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testDelCustomStore = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced
  await persistence.set('test-key', 'test-value')
  const val = await persistence.get('test-key')
  t.assert(val === 'test-value')
  await persistence.del('test-key')
  const valAfterDel = await persistence.get('test-key')
  t.assert(valAfterDel === undefined)
  await persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testStoreStateAwaitable = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced
  const p = storeState(persistence, true)
  t.assert(p instanceof Promise)
  await p
  await persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testDestroyDuringFetch = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  const pFetch = fetchUpdates(persistence)
  await persistence.destroy()
  await pFetch
}

/**
 * @param {t.TestCase} tc
 */
export const testDurabilityOption = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc, { durability: 'relaxed' })
  t.assert(persistence.durability === 'relaxed')
  await persistence.whenSynced
  doc.getArray('t').insert(0, [1])
  await promise.wait(50)
  await persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testUnloadListenerDirect = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  // Queue an update in memory
  doc.getArray('t').insert(0, [1])
  t.assert(persistence._pendingUpdates.length === 1)

  // Trigger unload listener directly
  persistence._unloadListener()

  // Pending updates should be flushed
  t.assert(persistence._pendingUpdates.length === 0)

  await persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testWriteBeforeDbOpen = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)

  // Insert immediately while db is still null
  doc.getArray('t').insert(0, [1])

  // Now wait for sync
  await persistence.whenSynced

  // Verify it was written successfully
  const doc2 = new Y.Doc()
  const persistence2 = new IndexeddbPersistence(tc.testName, doc2)
  await persistence2.whenSynced
  t.compareArrays(doc2.getArray('t').toArray(), [1])

  await persistence.destroy()
  await persistence2.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testRetryExhaustedEvent = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()
  const persistence = new IndexeddbPersistence(tc.testName, doc)
  await persistence.whenSynced

  // Set low retry count to keep test fast
  persistence._maxRetries = 2

  const db = /** @type {IDBDatabase} */ (persistence.db)
  const originalTransaction = db.transaction

  const mockTx = /** @type {any} */ ({
    objectStore: () => ({
      add: () => ({})
    }),
    error: new Error('Transaction aborted due to quota limits'),
    oncomplete: null,
    onerror: null,
    onabort: null
  })

  db.transaction = () => {
    return mockTx
  }

  let retryExhaustedCount = 0
  /** @type {any} */
  let lastError = null
  persistence.on('retry-exhausted', (/** @type {any} */ err) => {
    retryExhaustedCount++
    lastError = err
  })

  // Trigger initial write
  doc.getArray('t').insert(0, [1])

  // 1. Wait for microtask flush to run and attach onerror
  await promise.wait(50)
  t.assert(typeof mockTx.onerror === 'function')
  if (mockTx.onerror) mockTx.onerror() // retryCount becomes 1, backoff is 200ms

  // 2. Wait for 200ms backoff timeout + microtask to run the next flush
  await promise.wait(250)
  t.assert(typeof mockTx.onerror === 'function')
  if (mockTx.onerror) mockTx.onerror() // retryCount becomes 2, backoff is 400ms

  // 3. Wait for 400ms backoff timeout + microtask to run the next flush
  await promise.wait(450)
  t.assert(typeof mockTx.onerror === 'function')
  if (mockTx.onerror) mockTx.onerror() // retryCount becomes 3 > 2 (exhausted), emits event, resets to 0

  t.assert(retryExhaustedCount === 1)
  t.assert(lastError instanceof Error)
  t.assert(lastError.message === 'Transaction aborted due to quota limits')

  db.transaction = originalTransaction
  await persistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testTransactionRunner = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()

  /** @type {string[]} */
  const calls = []
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = async work => {
    calls.push('start')
    const res = await work()
    calls.push('end')
    return res
  }

  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })

  // Wait for initial sync
  await persistence.whenSynced
  await promise.wait(50)
  // The initial fetchUpdates should have been called!
  t.assert(calls.length === 2, 'transactionRunner should be called during sync')
  t.assert(calls[0] === 'start')
  t.assert(calls[1] === 'end')

  // Clear calls list
  calls.length = 0

  // Test set
  await persistence.set('test-key', 'value')
  t.compareArrays(calls, ['start', 'end'], 'set should be wrapped in transactionRunner')

  // Test del
  calls.length = 0
  await persistence.del('test-key')
  t.compareArrays(calls, ['start', 'end'], 'del should be wrapped in transactionRunner')

  // Test _flush (triggered by update)
  calls.length = 0
  doc.getArray('t').insert(0, [1])
  // Wait for flush to run and resolve
  await promise.wait(50)
  t.compareArrays(calls, ['start', 'end'], '_flush should be wrapped in transactionRunner')

  // Test storeState
  calls.length = 0
  await storeState(persistence, true)
  t.compareArrays(calls, ['start', 'end'], 'storeState should be wrapped in transactionRunner')

  // Test destroy
  calls.length = 0
  // queue an update to ensure destroy flushes
  doc.getArray('t').insert(0, [2])
  await persistence.destroy()
  t.compareArrays(calls, ['start', 'end'], 'destroy flush should be wrapped in transactionRunner')
}

/**
 * @param {t.TestCase} tc
 */
export const testTransactionRunnerSerialization = async tc => {
  await clearDocument(tc.testName)
  const doc = new Y.Doc()

  /** @type {string[]} */
  const executionOrder = []
  let tail = Promise.resolve()
  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runExclusiveIdbWrite = work => {
    const run = tail.then(work, work)
    tail = run.then(() => undefined, () => undefined)
    return run
  }

  /**
   * @template T
   * @param {() => Promise<T>} work
   * @return {Promise<T>}
   */
  const runner = async (work) => {
    return runExclusiveIdbWrite(async () => {
      executionOrder.push('start')
      await promise.wait(10)
      const res = await work()
      executionOrder.push('end')
      return res
    })
  }

  const persistence = new IndexeddbPersistence(tc.testName, doc, { transactionRunner: runner })
  await persistence.whenSynced
  await promise.wait(50)

  executionOrder.length = 0

  // Trigger two concurrent writes (set and del)
  const p1 = persistence.set('key1', 'val1')
  const p2 = persistence.del('key1')

  await Promise.all([p1, p2])

  // They must execute sequentially, i.e., start, end, start, end
  t.compareArrays(executionOrder, ['start', 'end', 'start', 'end'])

  await persistence.destroy()
}
