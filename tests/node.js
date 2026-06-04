/* eslint-disable no-extend-native */
if (typeof DOMException === 'undefined') {
  globalThis.DOMException = class DOMException extends Error {
    constructor (message, name) {
      super(message)
      this.name = name || 'DOMException'
    }
  }
}

if (typeof Array.prototype.findLast === 'undefined') {
  Array.prototype.findLast = function (callback, thisArg) {
    for (let i = this.length - 1; i >= 0; i--) {
      if (callback.call(thisArg, this[i], i, this)) {
        return this[i]
      }
    }
    return undefined
  }
}

if (typeof Array.prototype.findLastIndex === 'undefined') {
  Array.prototype.findLastIndex = function (callback, thisArg) {
    for (let i = this.length - 1; i >= 0; i--) {
      if (callback.call(thisArg, this[i], i, this)) {
        return i
      }
    }
    return -1
  }
}

if (typeof globalThis.structuredClone === 'undefined') {
  globalThis.structuredClone = function structuredClone (val) {
    if (val === null || typeof val !== 'object') {
      return val
    }
    if (val instanceof Date) {
      return new Date(val.getTime())
    }
    if (val instanceof RegExp) {
      return new RegExp(val.source, val.flags)
    }
    if (val instanceof ArrayBuffer) {
      return val.slice(0)
    }
    if (ArrayBuffer.isView(val)) {
      const ctor = val.constructor
      // @ts-ignore
      return new ctor(val.buffer.slice(val.byteOffset, val.byteOffset + val.byteLength))
    }
    if (val instanceof Set) {
      const res = new Set()
      val.forEach(item => res.add(structuredClone(item)))
      return res
    }
    if (val instanceof Map) {
      const res = new Map()
      val.forEach((value, key) => res.set(structuredClone(key), structuredClone(value)))
      return res
    }
    if (Array.isArray(val)) {
      return val.map(item => structuredClone(item))
    }
    const res = Object.create(Object.getPrototypeOf(val))
    for (const key of Object.keys(val)) {
      res[key] = structuredClone(val[key])
    }
    return res
  }
}

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason)
  process.exit(1)
})
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err)
  process.exit(1)
})

;(async () => {
  await import('fake-indexeddb/auto')
  await import('./index.js')
})()
