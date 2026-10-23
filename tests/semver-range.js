/**
 * Minimal npm semver-range helpers shared by the peer-dependency tests
 * (peer-deps.tests.js, yjs-peer-floor.tests.js).
 */

/**
 * @param {Array<number>} a
 * @param {Array<number>} b
 * @return {number}
 */
export const compareVersions = (a, b) => {
  for (let i = 0; i < 3; i++) {
    if (a[i] !== b[i]) return a[i] - b[i]
  }
  return 0
}

/**
 * Lower bound of a single version (partial versions and x-ranges included:
 * `13`, `13.x`, `13.5`, `13.5.*`), ignoring any prerelease/build suffix.
 *
 * @param {string} version
 * @return {Array<number>}
 */
export const parseLowerVersion = version => {
  const parts = version.split(/[-+]/)[0].split('.')
  return [0, 1, 2].map(i => {
    const n = Number.parseInt(parts[i], 10)
    return Number.isNaN(n) ? 0 : n
  })
}

/**
 * The smallest version a semver range admits, together with whether that
 * bound is exclusive (`>x.y.z`). Supports the npm range grammar used in
 * package.json files: `||` alternatives, space-separated comparator sets,
 * hyphen ranges, `^`, `~`, `>=`, `>`, `<`, `<=`, `=` and x-ranges.
 *
 * @param {string} range
 * @return {{ version: Array<number>, exclusive: boolean }}
 */
export const minAdmittedVersion = range => {
  /** @type {{ version: Array<number>, exclusive: boolean } | null} */
  let min = null
  for (const alternative of range.split('||')) {
    const tokens = alternative.trim().replace(/([<>=~^]+)\s+/g, '$1').split(/\s+/).filter(s => s.length > 0)
    /** @type {{ version: Array<number>, exclusive: boolean }} */
    let lower = { version: [0, 0, 0], exclusive: false }
    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i]
      if (tokens[i + 1] === '-') {
        // hyphen range `a - b`: lower bound is `a`
        i += 2
      }
      const match = /^(<=|>=|<|>|=|\^|~>?|v)?v?(.*)$/.exec(token)
      const op = match ? match[1] || '' : ''
      const rest = match ? match[2] : token
      if (op === '<' || op === '<=' || rest === '*' || rest === '' || /^[xX*]/.test(rest)) {
        continue
      }
      const candidate = { version: parseLowerVersion(rest), exclusive: op === '>' }
      const cmp = compareVersions(candidate.version, lower.version)
      if (cmp > 0 || (cmp === 0 && candidate.exclusive)) {
        lower = candidate
      }
    }
    if (min === null) {
      min = lower
    } else {
      const cmp = compareVersions(lower.version, min.version)
      if (cmp < 0 || (cmp === 0 && !lower.exclusive)) {
        min = lower
      }
    }
  }
  return min || { version: [0, 0, 0], exclusive: false }
}
