if (typeof DOMException === 'undefined') {
  globalThis.DOMException = class DOMException extends Error {
    constructor (message, name) {
      super(message)
      this.name = name || 'DOMException'
    }
  }
}

;(async () => {
  await import('fake-indexeddb/auto')
  await import('./index.js')
})()
