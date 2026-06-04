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
  <b><code>provider = new IndexeddbPersistence(<br/>
    docName: string,<br/>
    ydoc: Y.Doc,<br/>
    options: { writeDebounceMs?: number, durability?: 'default' | 'relaxed' } = {}<br/>
  )</code></b>
  <dd>
Create a y-idb persistence provider. Specify docName as a unique string
that identifies this document. In most cases, you want to use the same identifier
that is used as the room-name in the connection provider.

An optional <code>options.writeDebounceMs</code> (default <code>0</code>, which
coalesces writes on a microtask) can be supplied to debounce and aggregate
updates. All updates are serialized to at most 1 in-flight transaction to
prevent WebKit database transaction hangs and silent write drops.

An optional <code>options.durability</code> (default <code>'default'</code>,
which can be set to <code>'relaxed'</code>) controls the transaction
durability guarantee passed to IndexedDB. Using <code>'relaxed'</code> can
significantly improve write performance on some browsers/OS combinations by
allowing the browser to cache transaction writes.
  </dd>
  <b><code>provider.on('synced', function(idbPersistence: IndexeddbPersistence))</code></b>
  <dd>
The "synced" event is fired when the connection to the database has been established
and all available content has been loaded. The event is also fired if no content
is found for the given doc name.
  </dd>
  <b><code>provider.on('error', function(error: Error))</code></b>
  <dd>
The "error" event is fired when a database transaction or operation fails
(e.g. QuotaExceededError, aborted transaction).
  </dd>
  <b><code>provider.on('retry-exhausted', function(error: Error))</code></b>
  <dd>
The "retry-exhausted" event is fired when the write retry count has exceeded
the maximum limit (5 retries by default) after persistent database transaction
failures.
  </dd>
  <b><code>provider.set(key: any, value: any): Promise&lt;any&gt;</code></b>
  <dd>
Set a custom property on the provider instance. You can use this to store relevant
meta-information for the persisted document. However, the content will not be
synced with other peers.
  </dd>
  <b><code>provider.get(key: any): Promise&gt;any&lt;</code></b>
  <dd>
Retrieve a stored value.
  </dd>
  <b><code>provider.del(key: any): Promise&gt;undefined&lt;</code></b>
  <dd>
Delete a stored value.
  </dd>
  <b><code>provider.destroy(): Promise</code></b>
  <dd>
Close the connection to the database and stop syncing the document. This method is
automatically called when the Yjs document is destroyed (e.g. ydoc.destroy()).
  </dd>
  <b><code>provider.clearData(): Promise</code></b>
  <dd>
Destroy this database and remove the stored document and all related meta-information
from the database.
  </dd>
</dl>

## License

Yjs is licensed under the [MIT License](./LICENSE).

<kevin.jahns@protonmail.com>
