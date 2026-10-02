import * as indexeddb from './y-idb.tests.js'
import * as destroyBeforeOpen from './destroy-before-open.tests.js'
import * as hydrationInitstate from './hydration-initstate.tests.js'
import * as dbrefAbort from './dbref-abort.tests.js'
import * as runnerSettle from './runner-settle.tests.js'
import * as runnerSyncThrow from './runner-sync-throw.tests.js'
import * as snapshotAtomicity from './snapshot-atomicity.tests.js'
import * as openFailure from './open-failure.tests.js'
import * as hydrationError from './hydration-error.tests.js'
import * as unloadWrite from './unload-write.tests.js'
import * as errorPayload from './error-payload.tests.js'
import * as errorListener from './error-listener.tests.js'
import * as pendingStructs from './pending-structs.tests.js'
import * as corruptRow from './corrupt-row.tests.js'
import * as snapshotErrors from './snapshot-errors.tests.js'
import * as peerDeps from './peer-deps.tests.js'
import * as lifecycleRunner from './lifecycle-runner.tests.js'
import * as destroyTimers from './destroy-timers.tests.js'
import * as flushBackoff from './flush-backoff.tests.js'
import * as hydrationEncode from './hydration-encode.tests.js'

import { runTests } from 'lib0/testing.js'
import { isBrowser, isNode } from 'lib0/environment.js'
import * as log from 'lib0/logging.js'

if (isBrowser) {
  log.createVConsole(document.body)
}
runTests({
  indexeddb,
  destroyBeforeOpen,
  hydrationInitstate,
  dbrefAbort,
  runnerSettle,
  runnerSyncThrow,
  snapshotAtomicity,
  openFailure,
  hydrationError,
  unloadWrite,
  errorPayload,
  errorListener,
  pendingStructs,
  corruptRow,
  snapshotErrors,
  peerDeps,
  lifecycleRunner,
  destroyTimers,
  flushBackoff,
  hydrationEncode
}).then(success => {
  /* istanbul ignore next */
  if (isNode) {
    process.exit(success ? 0 : 1)
  }
})
