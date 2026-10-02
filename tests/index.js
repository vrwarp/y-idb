import * as indexeddb from './y-idb.tests.js'
import * as destroyBeforeOpen from './destroy-before-open.tests.js'
import * as hydrationInitstate from './hydration-initstate.tests.js'
import * as dbrefAbort from './dbref-abort.tests.js'
import * as runnerSettle from './runner-settle.tests.js'

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
  runnerSettle
}).then(success => {
  /* istanbul ignore next */
  if (isNode) {
    process.exit(success ? 0 : 1)
  }
})
