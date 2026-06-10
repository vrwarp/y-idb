# y-idb

> IndexedDB database provider for Yjs (Fork of [`y-indexeddb`](https://github.com/yjs/y-indexeddb)).
>
> Official documentation: [vrwarp.github.io/y-idb](https://vrwarp.github.io/y-idb/)

Use the IndexedDB database adapter to store your shared data persistently in
the browser. The next time you join the session, your changes will still be
there.

* Minimizes the amount of data exchanged between server and client
* Makes offline editing possible

## Getting Started

You find the complete documentation published online: [API documentation](https://vrwarp.github.io/y-idb/).

```sh
npm i --save y-idb
```

```js
const provider = new IndexeddbPersistence(docName, ydoc)

provider.on('synced', () => {
  console.log('content from the database is loaded')
})
```

## API

<dl>
  <dt><b><code>provider = new IndexeddbPersistence(<br/>
    docName: string,<br/>
    ydoc: Y.Doc,<br/>
    options: {<br/>
      writeDebounceMs?: number,<br/>
      durability?: 'default' | 'relaxed',<br/>
      transactionRunner?: &lt;T&gt;(work: () =&gt; Promise&lt;T&gt;) =&gt; Promise&lt;T&gt;,<br/>
      maxRetries?: number<br/>
    } = {}<br/>
  )</code></b></dt>
  <dd>
Create a y-idb persistence provider. Specify docName as a unique string
that identifies this document. In most cases, you want to use the same identifier
that is used as the room-name in the connection provider.

An optional <code>options.writeDebounceMs</code> (default <code>0</code>, which
coalesces writes on a microtask) can be supplied to debounce and aggregate
updates. Document updates are batched and written by an internal flusher that
keeps at most 1 in-flight flush transaction to reduce WebKit database
transaction hangs and silent write drops. Note that other operations
(<code>set</code>/<code>del</code>, periodic compaction, the page-hide flush,
and the final flush during <code>destroy()</code>) open their own
transactions; supply <code>options.transactionRunner</code> if all writes
must be strictly serialized.

An optional <code>options.durability</code> (default <code>'default'</code>,
which can be set to <code>'relaxed'</code>) controls the transaction
durability guarantee passed to IndexedDB. Using <code>'relaxed'</code> can
significantly improve write performance on some browsers/OS combinations by
allowing the browser to cache transaction writes.

An optional <code>options.transactionRunner</code> can be supplied to wrap
or delegate the execution of internal write transactions (such as syncs,
flushes, custom sets, or deletes) to a custom sequencer or global lock.
This is useful for coordinating serialization across different databases or
stores to prevent WebKit (Safari) transaction deadlocks/hangs.

An optional <code>options.maxRetries</code> (default <code>5</code>) controls
how often a failed write is retried with exponential backoff
(200ms, 400ms, 800ms, ...) before the <code>retry-exhausted</code> event is
emitted.
  </dd>
  <dt><b><code>provider.whenSynced: Promise&lt;IndexeddbPersistence&gt;</code></b></dt>
  <dd>
A promise that resolves once the initial content has been loaded from the
database (i.e. when the "synced" event fires). Note: if the provider is
destroyed before the initial sync completes, this promise never settles.
  </dd>
  <dt><b><code>provider.on('synced', function(idbPersistence: IndexeddbPersistence))</code></b></dt>
  <dd>
The "synced" event is fired when the connection to the database has been established
and all available content has been loaded. The event is also fired if no content
is found for the given doc name.
  </dd>
  <dt><b><code>provider.on('error', function(error: Error))</code></b></dt>
  <dd>
The "error" event is fired when a database transaction or operation fails
(e.g. QuotaExceededError, aborted transaction). Failed update batches are
kept in memory and retried; while the database keeps failing, unwritten
updates accumulate in memory until a write succeeds or the provider is
destroyed.
  </dd>
  <dt><b><code>provider.on('retry-exhausted', function(error: Error))</code></b></dt>
  <dd>
The "retry-exhausted" event is fired when the write retry count has exceeded
the configured <code>maxRetries</code> limit (5 by default) after persistent
database transaction failures.
  </dd>
  <dt><b><code>provider.set(key: any, value: any): Promise&lt;any&gt;</code></b></dt>
  <dd>
Set a custom property on the provider instance. You can use this to store relevant
meta-information for the persisted document. However, the content will not be
synced with other peers.
  </dd>
  <dt><b><code>provider.get(key: any): Promise&lt;any&gt;</code></b></dt>
  <dd>
Retrieve a stored value.
  </dd>
  <dt><b><code>provider.del(key: any): Promise&lt;undefined&gt;</code></b></dt>
  <dd>
Delete a stored value.
  </dd>
  <dt><b><code>provider.destroy(): Promise</code></b></dt>
  <dd>
Close the connection to the database and stop syncing the document. This method is
automatically called when the Yjs document is destroyed (e.g. ydoc.destroy()).
  </dd>
  <dt><b><code>provider.clearData(): Promise</code></b></dt>
  <dd>
Destroy this database and remove the stored document and all related meta-information
from the database.
  </dd>
</dl>

## License

y-idb is licensed under the [MIT License](./LICENSE).

It is a fork of [y-indexeddb](https://github.com/yjs/y-indexeddb) by
Kevin Jahns <kevin.jahns@protonmail.com>.
