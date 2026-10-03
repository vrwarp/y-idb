/**
 * Merges the JSON reports of a sharded mutation run (see shard-checker.mjs)
 * into one report, writes it as reports/mutation/mutation.json and
 * reports/mutation/index.html, and exits non-zero when the overall mutation
 * score is under thresholds.break in stryker.config.json — the same check a
 * single `stryker run` makes.
 *
 *   node scripts/mutation/merge-shards.mjs reports/mutation/shard-*.json
 *
 * Every shard instruments the same sources, so every report must list the same
 * mutants with the same ids, and each mutant must have been tested by exactly
 * the shard that owns it. Anything else means the shards diverged, and the
 * merge fails instead of reporting a score for a different set of mutants.
 * Test ids are renumbered per report, so coverage is remapped by test name.
 */
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import path from 'node:path'
import { SHARD_REASON_PREFIX, ownerOf } from './shard-checker.mjs'

const OUTPUT_DIR = 'reports/mutation'
const DETECTED = new Set(['Killed', 'Timeout'])
const UNDETECTED = new Set(['NoCoverage', 'Survived'])

const fail = (message) => {
  console.error(`merge-shards: ${message}`)
  process.exit(1)
}

const reportPaths = process.argv.slice(2)

if (reportPaths.length === 0) {
  fail('no shard reports given')
}

const count = reportPaths.length

const isSkipped = (mutant) => mutant.status === 'CompileError' && mutant.statusReason?.startsWith(SHARD_REASON_PREFIX)

// A report belongs to the shard that owns the mutants it tested, so the
// reports may be given in any order.
const reports = Array.from({ length: count })

for (const reportPath of reportPaths) {
  const report = JSON.parse(readFileSync(reportPath, 'utf8'))
  const owners = new Set()

  for (const file of Object.values(report.files)) {
    for (const mutant of file.mutants) {
      if (!isSkipped(mutant)) {
        owners.add(ownerOf(mutant.id, count))
      }
    }
  }

  if (owners.size !== 1) {
    fail(`${reportPath} tested mutants of ${owners.size} shards out of ${count}; expected those of exactly one`)
  }

  const [shard] = owners

  if (reports[shard - 1]) {
    fail(`${reportPath} is a second report for shard ${shard}`)
  }

  reports[shard - 1] = report
}

// Test ids are indices into each report's own test list: key tests by file and
// name, and give every distinct test one id in the merged report.
const mergedTestFiles = {}
const mergedTestIds = new Map()
const testIdMaps = reports.map((report) => {
  const local = new Map()

  for (const [fileName, testFile] of Object.entries(report.testFiles ?? {})) {
    mergedTestFiles[fileName] ??= { ...testFile, tests: [] }

    for (const test of testFile.tests) {
      const key = `${fileName}\u0000${test.name}`

      if (!mergedTestIds.has(key)) {
        const id = String(mergedTestIds.size)

        mergedTestIds.set(key, id)
        mergedTestFiles[fileName].tests.push({ ...test, id })
      }

      local.set(test.id, mergedTestIds.get(key))
    }
  }

  return local
})

const remap = (ids, shardIndex) => ids?.map((id) => testIdMaps[shardIndex].get(id) ?? fail(`shard ${shardIndex + 1} refers to unknown test id ${id}`))

const signature = (mutant) => JSON.stringify([mutant.mutatorName, mutant.replacement, mutant.location])

const [first] = reports
const mutantsById = reports.map((report) => new Map(Object.entries(report.files).map(([fileName, file]) => [fileName, new Map(file.mutants.map((mutant) => [mutant.id, mutant]))])))
const mergedFiles = {}
const totals = new Map()
const perFile = []

for (const [fileName, file] of Object.entries(first.files)) {
  const mutants = file.mutants.map((mutant) => {
    const owner = ownerOf(mutant.id, count)
    let result

    mutantsById.forEach((files, shardIndex) => {
      const candidate = files.get(fileName)?.get(mutant.id)

      if (!candidate || signature(candidate) !== signature(mutant)) {
        fail(`shard ${shardIndex + 1} has a different mutant ${mutant.id} in ${fileName}; the shards did not instrument the same sources`)
      }

      if (shardIndex + 1 === owner) {
        if (isSkipped(candidate)) {
          fail(`mutant ${mutant.id} in ${fileName} was not tested by its own shard ${owner}`)
        }

        result = { ...candidate, coveredBy: remap(candidate.coveredBy, shardIndex), killedBy: remap(candidate.killedBy, shardIndex) }
      } else if (!isSkipped(candidate)) {
        fail(`mutant ${mutant.id} in ${fileName} was also tested by shard ${shardIndex + 1}, not only by its owner ${owner}`)
      }
    })

    return result
  })

  for (const report of reports) {
    if (report.files[fileName]?.mutants.length !== mutants.length) {
      fail(`the shards list a different number of mutants in ${fileName}`)
    }
  }

  mergedFiles[fileName] = { ...file, mutants }

  const counts = new Map()

  for (const { status } of mutants) {
    counts.set(status, (counts.get(status) ?? 0) + 1)
    totals.set(status, (totals.get(status) ?? 0) + 1)
  }

  perFile.push([fileName, counts])
}

for (const [shardIndex, report] of reports.entries()) {
  if (Object.keys(report.files).length !== Object.keys(first.files).length) {
    fail(`shard ${shardIndex + 1} mutated a different set of files`)
  }
}

// Stryker's own definition (mutation-testing-metrics): detected over valid,
// where valid excludes compile errors, runtime errors and ignored mutants.
const sum = (counts, statuses) => [...statuses].reduce((total, status) => total + (counts.get(status) ?? 0), 0)
const score = (counts) => {
  const detected = sum(counts, DETECTED)
  const valid = detected + sum(counts, UNDETECTED)

  return valid > 0 ? (detected / valid) * 100 : Number.NaN
}

const merged = { ...first, files: mergedFiles, testFiles: mergedTestFiles }

mkdirSync(OUTPUT_DIR, { recursive: true })
writeFileSync(path.join(OUTPUT_DIR, 'mutation.json'), JSON.stringify(merged))

// The same page Stryker's html reporter writes, from the copy of
// mutation-testing-elements that @stryker-mutator/core resolves.
const requireFromCore = createRequire(createRequire(import.meta.url).resolve('@stryker-mutator/core/package.json'))
const elements = readFileSync(requireFromCore.resolve('mutation-testing-elements/dist/mutation-test-elements.js'), 'utf8')
const reportJson = JSON.stringify(merged).replaceAll('<', String.raw`<"+"`)

writeFileSync(path.join(OUTPUT_DIR, 'index.html'), `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <script>
    ${elements}
  </script>
</head>
<body>
  <mutation-test-report-app titlePostfix="Stryker"></mutation-test-report-app>
  <script>
    const app = document.querySelector('mutation-test-report-app');
    app.report = ${reportJson};
    function updateTheme() {
      document.body.style.backgroundColor = app.themeBackgroundColor;
    }
    app.addEventListener('theme-changed', updateTheme);
    updateTheme();
  </script>
</body>
</html>
`)

const columns = ['Killed', 'Timeout', 'Survived', 'NoCoverage', 'Ignored', 'RuntimeError', 'CompileError']

console.log(['file', 'score', ...columns].join('\t'))

for (const [fileName, counts] of [...perFile, ['All files', totals]]) {
  console.log([fileName, score(counts).toFixed(2), ...columns.map((status) => counts.get(status) ?? 0)].join('\t'))
}

const total = score(totals)
const config = JSON.parse(readFileSync('stryker.config.json', 'utf8'))
const breaking = config.thresholds?.break

console.log(`\nMerged ${count} shard report(s) into ${OUTPUT_DIR}/index.html`)

if (typeof breaking === 'number' && !(total >= breaking)) {
  fail(`final mutation score ${total.toFixed(2)} is under the break threshold ${breaking}`)
}

console.log(typeof breaking === 'number'
  ? `Final mutation score of ${total.toFixed(2)} is greater than or equal to break threshold ${breaking}`
  : `Final mutation score of ${total.toFixed(2)} (no break threshold configured)`)
