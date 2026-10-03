/**
 * A Stryker checker that splits one mutation run into parallel shards.
 *
 * Mutant ids are sequential, so mutant N belongs to shard (N mod count) + 1:
 * neighbouring mutants, which tend to be covered by the same tests, land in
 * different shards and every shard gets a similar share of the work. Mutants
 * owned by another shard fail the check as a CompileError, which Stryker
 * neither tests nor counts in the score. merge-shards.mjs takes each mutant's
 * result from its owner and applies the break threshold to the whole report.
 *
 * Shards are selected with STRYKER_SHARD (1-based) and STRYKER_SHARD_COUNT;
 * see stryker.shard.config.mjs.
 */
import { CheckStatus } from '@stryker-mutator/api/check'
import { PluginKind, declareClassPlugin } from '@stryker-mutator/api/plugin'

export const SHARD_REASON_PREFIX = 'Tested by mutation shard '

const readShard = () => {
  const shard = Number(process.env.STRYKER_SHARD)
  const count = Number(process.env.STRYKER_SHARD_COUNT)

  if (!Number.isInteger(count) || count < 1 || !Number.isInteger(shard) || shard < 1 || shard > count) {
    throw new Error(`STRYKER_SHARD=${process.env.STRYKER_SHARD} and STRYKER_SHARD_COUNT=${process.env.STRYKER_SHARD_COUNT} must be integers with 1 <= STRYKER_SHARD <= STRYKER_SHARD_COUNT`)
  }

  return { count, shard }
}

export const ownerOf = (mutantId, count) => {
  const id = Number(mutantId)

  if (!Number.isInteger(id) || id < 0) {
    throw new Error(`Mutant id ${mutantId} is not a sequential number; the shard checker cannot place it`)
  }

  return (id % count) + 1
}

class ShardChecker {
  async init () {
    readShard()
  }

  async group (mutants) {
    return [mutants.map((mutant) => mutant.id)]
  }

  async check (mutants) {
    const { count, shard } = readShard()

    return Object.fromEntries(mutants.map((mutant) => {
      const owner = ownerOf(mutant.id, count)

      return [
        mutant.id,
        owner === shard
          ? { status: CheckStatus.Passed }
          : { reason: `${SHARD_REASON_PREFIX}${owner} of ${count}`, status: CheckStatus.CompileError }
      ]
    }))
  }
}

export const strykerPlugins = [declareClassPlugin(PluginKind.Checker, 'shard', ShardChecker)]
