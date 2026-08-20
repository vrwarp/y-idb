/**
 * Versicle-shaped aging workload (plain-JS port)
 *
 * Mirror of y-cinder's `benchmarks/versicle-workload.ts` — keep the two in
 * sync when changing the model. See that file for the faithfulness notes.
 *
 * Summary: simulates versicle's single long-lived Y.Doc (ten top-level
 * Y.Maps: library/progress/annotations/reading-list/vocabulary/lexicon/
 * contentAnalysis/devices/searchHistory/meta) aged through many sessions,
 * each session using a FRESH clientID (versicle constructs a new Y.Doc per
 * page load). Hot paths: page-turn/TTS same-key overwrites in `progress`,
 * readingSessions sawtooth (500 -> 300), heartbeat overwrites in `devices`,
 * wholesale searchHistory array rebuilds, book import/removal churn.
 */
import * as Y from 'yjs'

/** Park-Miller LCG, matching y-cinder's tests/unit/prng.ts */
export class SeededRandom {
  constructor (seed) {
    this.seed = Math.abs(seed % 2147483647) || 1
  }

  next () {
    this.seed = (this.seed * 16807) % 2147483647
    return (this.seed - 1) / 2147483646
  }

  int (min, max) {
    return Math.floor(this.next() * (max - min + 1)) + min
  }

  choice (arr) {
    return arr[this.int(0, arr.length - 1)]
  }

  bool (p = 0.5) {
    return this.next() < p
  }

  string (length) {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
    let result = ''
    for (let i = 0; i < length; i++) result += chars.charAt(this.int(0, chars.length - 1))
    return result
  }
}

export const DEFAULT_SIM = {
  initialBooks: 20,
  maxBooks: 60,
  eventsPerSession: 60,
  devices: 2
}

export function createSim (opts) {
  const full = { ...DEFAULT_SIM, ...opts }
  return {
    rng: new SeededRandom(full.seed),
    opts: full,
    sessionCount: 0,
    bookIds: [],
    nextBookNum: 0,
    totalEvents: 0,
    bytesProduced: 0
  }
}

const cfi = (rng) => `epubcfi(/6/${rng.int(2, 40)}!/4/${rng.int(2, 200)}/${rng.int(1, 30)}:${rng.int(0, 900)})`
const nowFor = (state) => 1700000000000 + state.sessionCount * 43200000 + state.totalEvents * 5000

function importBook (state, doc) {
  const { rng } = state
  const bookId = `book-${state.nextBookNum++}`
  state.bookIds.push(bookId)
  const books = doc.getMap('library').get('books')
  const item = new Y.Map()
  books.set(bookId, item)
  item.set('bookId', bookId)
  item.set('title', 'Title ' + rng.string(rng.int(8, 30)))
  item.set('author', 'Author ' + rng.string(rng.int(6, 20)))
  item.set('addedAt', nowFor(state))
  item.set('lastInteraction', nowFor(state))
  item.set('sourceFilename', bookId + '.epub')
  item.set('status', 'unread')
  item.set('language', rng.bool(0.3) ? 'zh' : 'en')
  const tags = new Y.Array()
  item.set('tags', tags)
  tags.push([rng.string(6)])
  const palette = new Y.Array()
  item.set('coverPalette', palette)
  palette.push([rng.int(0, 65535), rng.int(0, 65535), rng.int(0, 65535), rng.int(0, 65535), rng.int(0, 65535)])
  const perceptual = new Y.Map()
  item.set('perceptualPalette', perceptual)
  perceptual.set('standout', rng.int(0, 0xffffff))
  perceptual.set('background', rng.int(0, 0xffffff))
  perceptual.set('deltaE', rng.next() * 100)

  const sections = doc.getMap('contentAnalysis').get('sections')
  const chapters = rng.int(6, 14)
  for (let c = 0; c < chapters; c++) {
    const s = new Y.Map()
    sections.set(`${bookId}/sec-${c}`, s)
    s.set('title', 'Chapter ' + c + ' ' + rng.string(10))
    s.set('generatedAt', nowFor(state))
    s.set('status', 'done')
    if (rng.bool(0.2)) s.set('referenceStartCfi', cfi(rng))
  }
  const entries = doc.getMap('reading-list').get('entries')
  const e = new Y.Map()
  entries.set(bookId + '.epub', e)
  e.set('filename', bookId + '.epub')
  e.set('bookId', bookId)
  e.set('title', item.get('title'))
  e.set('author', item.get('author'))
  e.set('percentage', 0)
  e.set('lastUpdated', nowFor(state))
}

function removeBook (state, doc) {
  const { rng } = state
  if (state.bookIds.length <= 5) return
  const idx = rng.int(0, state.bookIds.length - 1)
  const bookId = state.bookIds.splice(idx, 1)[0]
  doc.getMap('library').get('books').delete(bookId)
  const sections = doc.getMap('contentAnalysis').get('sections')
  const toDelete = []
  sections.forEach((_v, k) => { if (k.startsWith(bookId + '/')) toDelete.push(k) })
  toDelete.forEach(k => sections.delete(k))
}

function ensureRoots (doc) {
  const ensure = (map, key, mk) => {
    const m = doc.getMap(map)
    if (!m.has(key)) m.set(key, mk())
  }
  ensure('library', 'books', () => new Y.Map())
  ensure('progress', 'progress', () => new Y.Map())
  ensure('annotations', 'annotations', () => new Y.Map())
  ensure('reading-list', 'entries', () => new Y.Map())
  ensure('vocabulary', 'knownCharacters', () => new Y.Map())
  ensure('lexicon', 'rules', () => new Y.Map())
  ensure('contentAnalysis', 'sections', () => new Y.Map())
  ensure('devices', 'devices', () => new Y.Map())
  ensure('searchHistory', 'recentQueries', () => new Y.Array())
  ensure('searchHistory', 'savedQueries', () => new Y.Array())
  const meta = doc.getMap('meta')
  if (!meta.has('schemaVersion')) meta.set('schemaVersion', 9)
}

const deviceIdFor = (state) => `device-${state.sessionCount % state.opts.devices}`

function bootDevice (state, doc) {
  const devices = doc.getMap('devices').get('devices')
  const id = deviceIdFor(state)
  let dev = devices.get(id)
  if (!dev) {
    dev = new Y.Map()
    devices.set(id, dev)
    dev.set('id', id)
    dev.set('name', 'Device ' + id)
    dev.set('platform', id.endsWith('0') ? 'android' : 'web')
    dev.set('browser', 'chrome')
    dev.set('userAgent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 versicle/' + state.rng.string(8))
    dev.set('created', nowFor(state))
    const profile = new Y.Map()
    dev.set('profile', profile)
    profile.set('theme', 'dark')
    profile.set('fontSize', 18)
  }
  dev.set('lastActive', nowFor(state))
  dev.set('appVersion', '1.' + (100 + (state.sessionCount % 40)) + '.0')
}

function progressMapFor (state, doc, bookId) {
  const root = doc.getMap('progress').get('progress')
  let perBook = root.get(bookId)
  if (!perBook) {
    perBook = new Y.Map()
    root.set(bookId, perBook)
  }
  const deviceId = deviceIdFor(state)
  let perDevice = perBook.get(deviceId)
  if (!perDevice) {
    perDevice = new Y.Map()
    perBook.set(deviceId, perDevice)
    perDevice.set('bookId', bookId)
    perDevice.set('percentage', 0)
    perDevice.set('completedRanges', new Y.Array())
    perDevice.set('readingSessions', new Y.Array())
  }
  return perDevice
}

const MAX_READING_SESSIONS = 500
const PRUNED_READING_SESSIONS = 300

function pushReadingSession (state, perDevice, type) {
  const { rng } = state
  const sessions = perDevice.get('readingSessions')
  const s = new Y.Map()
  sessions.push([s])
  s.set('cfiRange', cfi(rng) + ',' + cfi(rng))
  s.set('startTime', nowFor(state) - rng.int(5000, 90000))
  s.set('endTime', nowFor(state))
  s.set('type', type)
  if (rng.bool(0.7)) s.set('label', 'Chapter ' + rng.int(1, 30) + ': ' + rng.string(rng.int(10, 40)))
  if (sessions.length > MAX_READING_SESSIONS) {
    sessions.delete(0, sessions.length - PRUNED_READING_SESSIONS)
  }
}

function pageTurn (state, doc, bookId) {
  const { rng } = state
  const perDevice = progressMapFor(state, doc, bookId)
  perDevice.set('currentCfi', cfi(rng))
  perDevice.set('percentage', Math.min(1, (perDevice.get('percentage') || 0) + rng.next() * 0.01))
  perDevice.set('lastRead', nowFor(state))
  pushReadingSession(state, perDevice, rng.bool(0.8) ? 'page' : 'scroll')
  const ranges = perDevice.get('completedRanges')
  if (rng.bool(0.6) && ranges.length > 0) {
    ranges.delete(ranges.length - 1, 1)
    ranges.push([cfi(rng) + ',' + cfi(rng)])
  } else {
    ranges.push([cfi(rng) + ',' + cfi(rng)])
  }
  const entries = doc.getMap('reading-list').get('entries')
  const entry = entries.get(bookId + '.epub')
  if (entry) {
    entry.set('percentage', perDevice.get('percentage'))
    entry.set('lastUpdated', nowFor(state))
  }
}

function ttsSentence (state, doc, bookId) {
  const { rng } = state
  const perDevice = progressMapFor(state, doc, bookId)
  perDevice.set('lastPlayedCfi', cfi(rng))
  perDevice.set('currentQueueIndex', rng.int(0, 400))
  perDevice.set('lastRead', nowFor(state))
  const ranges = perDevice.get('completedRanges')
  if (ranges.length > 0 && rng.bool(0.8)) {
    ranges.delete(ranges.length - 1, 1)
  }
  ranges.push([cfi(rng) + ',' + cfi(rng)])
  if (rng.bool(0.05)) pushReadingSession(state, perDevice, 'tts')
}

function heartbeat (state, doc) {
  const devices = doc.getMap('devices').get('devices')
  const dev = devices.get(deviceIdFor(state))
  if (dev) dev.set('lastActive', nowFor(state))
}

function addAnnotation (state, doc, bookId) {
  const { rng } = state
  const annotations = doc.getMap('annotations').get('annotations')
  const id = 'ann-' + state.sessionCount + '-' + rng.string(6)
  const a = new Y.Map()
  annotations.set(id, a)
  a.set('id', id)
  a.set('bookId', bookId)
  a.set('cfiRange', cfi(rng) + ',' + cfi(rng))
  a.set('text', rng.string(rng.int(40, 300)))
  a.set('type', rng.bool(0.8) ? 'highlight' : 'note')
  a.set('color', 'yellow')
  if (rng.bool(0.3)) a.set('note', rng.string(rng.int(20, 120)))
  a.set('created', nowFor(state))
}

function deleteAnnotation (state, doc) {
  const { rng } = state
  const annotations = doc.getMap('annotations').get('annotations')
  const keys = []
  annotations.forEach((_v, k) => { keys.push(k) })
  if (keys.length > 20) annotations.delete(keys[rng.int(0, keys.length - 1)])
}

function searchQuery (state, doc) {
  const { rng } = state
  const recent = doc.getMap('searchHistory').get('recentQueries')
  const existing = []
  recent.forEach((m) => {
    existing.push({ query: m.get('query'), lastUsedAt: m.get('lastUsedAt') })
  })
  existing.unshift({ query: rng.string(rng.int(3, 18)), lastUsedAt: nowFor(state) })
  if (existing.length > 20) existing.length = 20
  recent.delete(0, recent.length)
  for (const q of existing) {
    const m = new Y.Map()
    recent.push([m])
    m.set('query', q.query)
    m.set('lastUsedAt', q.lastUsedAt)
    m.set('isSaved', false)
  }
}

function vocabulary (state, doc) {
  const { rng } = state
  const known = doc.getMap('vocabulary').get('knownCharacters')
  const ch = String.fromCharCode(0x4e00 + rng.int(0, 3000))
  known.set(ch, nowFor(state))
}

/**
 * Runs one versicle session on a hydrated doc. Invokes `onEvent()` after
 * each high-level event (one event ~ one debounced save in versicle).
 */
export async function runSession (state, doc, onEvent) {
  const { rng, opts } = state

  // Root containers must be created while persistence listeners are attached
  // (the caller binds the provider before running the session) — losing them
  // from the persisted stream gives every later blob a missing dependency.
  doc.transact(() => ensureRoots(doc))
  if (onEvent) await onEvent()

  if (state.sessionCount === 0) {
    doc.transact(() => {
      for (let i = 0; i < opts.initialBooks; i++) importBook(state, doc)
    })
    if (onEvent) await onEvent()
  }

  doc.transact(() => bootDevice(state, doc))
  if (onEvent) await onEvent()

  const active = []
  const activeCount = Math.min(state.bookIds.length, rng.int(1, 3))
  for (let i = 0; i < activeCount; i++) active.push(rng.choice(state.bookIds))
  const ttsSession = rng.bool(0.3)

  for (let e = 0; e < opts.eventsPerSession; e++) {
    state.totalEvents++
    const r = rng.next()
    if (r < 0.02 && state.bookIds.length < opts.maxBooks) {
      doc.transact(() => importBook(state, doc))
    } else if (r < 0.03) {
      doc.transact(() => removeBook(state, doc))
    } else if (r < 0.06) {
      doc.transact(() => heartbeat(state, doc))
    } else if (r < 0.09) {
      doc.transact(() => searchQuery(state, doc))
    } else if (r < 0.12) {
      doc.transact(() => addAnnotation(state, doc, rng.choice(active)))
    } else if (r < 0.13) {
      doc.transact(() => deleteAnnotation(state, doc))
    } else if (r < 0.18) {
      doc.transact(() => vocabulary(state, doc))
    } else if (ttsSession && r < 0.6) {
      doc.transact(() => ttsSentence(state, doc, active[0]))
    } else {
      doc.transact(() => pageTurn(state, doc, rng.choice(active)))
    }
    if (onEvent) await onEvent()
  }

  state.sessionCount++
}

export const clientIdForSession = (seed, session) => (seed % 1000) * 1000000 + session + 1

export function docStructStats (doc) {
  let items = 0
  let gcStructs = 0
  let deletedItems = 0
  doc.store.clients.forEach((arr) => {
    for (const s of arr) {
      if (s instanceof Y.GC) gcStructs++
      else {
        items++
        if (s.deleted) deletedItems++
      }
    }
  })
  const ds = Y.createDeleteSetFromStructStore(doc.store)
  let dsRanges = 0
  ds.clients.forEach((arr) => { dsRanges += arr.length })
  const svClients = Y.decodeStateVector(Y.encodeStateVector(doc)).size
  return { items, gcStructs, deletedItems, dsRanges, svClients }
}
