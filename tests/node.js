process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason)
  process.exit(1)
})
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err)
  process.exit(1)
})

// Mock minimal browser globals for coverage of pagehide/visibilitychange listeners
globalThis.addEventListener = () => {}
globalThis.removeEventListener = () => {}
globalThis.document = {
  addEventListener: () => {},
  removeEventListener: () => {},
  visibilityState: 'visible'
}

;(async () => {
  await import('fake-indexeddb/auto')
  await import('./index.js')
})()
