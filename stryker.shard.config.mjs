/**
 * One shard of the CI mutation run: stryker.config.json plus the shard
 * checker (scripts/mutation/shard-checker.mjs). Each shard writes a JSON
 * report and never applies the break threshold itself; the workflow merges
 * the shard reports with scripts/mutation/merge-shards.mjs, which does.
 *
 *   STRYKER_SHARD=1 STRYKER_SHARD_COUNT=4 npx stryker run stryker.shard.config.mjs
 *
 * `npm run test:mutation` still runs the whole suite in one process.
 */
import { readFileSync } from 'node:fs'

const base = JSON.parse(readFileSync(new URL('stryker.config.json', import.meta.url), 'utf8'))
const { $schema: _schema, _comment: _note, ...options } = base

export default {
  ...options,
  appendPlugins: ['./scripts/mutation/shard-checker.mjs'],
  checkers: ['shard'],
  jsonReporter: { fileName: `reports/mutation/shard-${process.env.STRYKER_SHARD}.json` },
  reporters: ['json', 'clear-text', 'progress'],
  thresholds: { ...options.thresholds, break: null }
}
