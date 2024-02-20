// @ts-ignore
const multicb = require('multicb')
const makeDebug = require('debug')

/**
 * @typedef {ReturnType<import('ppppp-db').init>} PPPPPDB
 * @typedef {ReturnType<import('ppppp-goals').init>} PPPPPGoal
 * @typedef {{
 *   gc: {
 *     maxLogBytes: number
 *     compactionInterval?: number
 *   }
 * }} ExpectedConfig
 * @typedef {{gc?: Partial<ExpectedConfig['gc']>}} Config
 */

/**
 * @template T
 * @typedef {T extends void ?
 *   (...args: [Error] | []) => void :
 *   (...args: [Error] | [null, T]) => void
 * } CB
 */

/**
 * @param {{ db: PPPPPDB, goals: PPPPPGoal }} peer
 * @param {Config} config
 */
function initGC(peer, config) {
  // State
  const debug = makeDebug('ppppp:gc')
  let stopMonitoringLogSize = /** @type {CallableFunction | null} */ (null)
  let hasCleanupScheduled = false

  /**
   * @param {Error} err
   */
  function flattenCauseChain(err) {
    let str = ''
    while (err?.message ?? err) {
      str += (err.message ?? err) + ': '
      err = /**@type {Error}*/ (err.cause)
    }
    return str
  }

  /**
   * Deletes messages that don't correspond to any goal.
   * @private
   * @param {CB<void>} cb
   */
  function cleanup(cb) {
    debug('Cleanup started')
    const startTime = Date.now()
    const done = multicb({ pluck: 1 })

    /**
     * @param {string} errExplanation
     */
    function makeRecCB(errExplanation) {
      const cb = done()
      return (/**@type {Error=}*/ err) => {
        if (err) debug('%s: %s', errExplanation, flattenCauseChain(err))
        cb()
      }
    }

    let waiting = false
    for (const rec of peer.db.records()) {
      if (!rec.msg) continue
      const { id: msgID, msg } = rec
      const [purpose, details] = peer.goals.getMsgPurpose(msgID, msg)
      switch (purpose) {
        case 'goal': {
          continue // don't cleanup
        }
        case 'none': {
          const recCB = makeRecCB('Failed to delete msg when cleaning up')
          debug('Deleting msg %s with purpose=none', msgID)
          peer.db.del(msgID, recCB)
          waiting = true
          continue
        }
        case 'ghost': {
          const { tangleID, span } = details
          const recCB = makeRecCB('Failed to delete ghost msg when cleaning up')
          // TODO: Could one msg be a ghostable in MANY tangles? Or just one?
          debug('Deleting and ghosting msg %s with purpose=ghost', msgID)
          peer.db.ghosts.add({ tangleID, msgID, span }, (err) => {
            if (err) return recCB(err)
            peer.db.del(msgID, recCB)
          })
          waiting = true
          continue
        }
        case 'trail': {
          if (!msg.data) continue // it's already erased
          const recCB = makeRecCB('Failed to erase trail msg when cleaning up')
          debug('Erasing msg %s with purpose=trail', msgID)
          peer.db.erase(msgID, recCB)
          waiting = true
          continue
        }
        default: {
          cb(new Error('Unreachable'))
          return
        }
      }
    }

    if (waiting) done(whenEnded)
    else whenEnded()

    /** @param {Error=} err */
    function whenEnded(err) {
      const duration = Date.now() - startTime
      if (err) debug('Cleanup ended with an error %s', err.message ?? err)
      else debug('Cleanup completed in %sms', duration)
      cb()
    }
  }

  /**
   * Recreates the log so it has no empty space.
   * @private
   * @param {CB<void>} cb
   */
  function compact(cb) {
    debug('Compaction started')
    const startTime = Date.now()
    peer.db.log.compact((err) => {
      const duration = Date.now() - startTime
      if (err) debug('Compaction ended with an error %s', err.message ?? err)
      else debug('Compaction completed in %sms', duration)
      cb()
    })
  }

  /**
   * @param {number} percentUsed
   * @param {number} maxLogBytes
   * @param {{ totalBytes: number; }} stats
   */
  function reportCleanupNeed(percentUsed, maxLogBytes, stats) {
    const bytesRemaining = maxLogBytes - stats.totalBytes
    const kbRemaining = bytesRemaining >> 10
    const mbRemaining = bytesRemaining >> 20
    const remaining =
      mbRemaining > 0
        ? `${mbRemaining}MB`
        : kbRemaining > 0
        ? `${kbRemaining}KB`
        : `${bytesRemaining}B`
    if (bytesRemaining > 0) {
      // prettier-ignore
      debug('Log is %s% full (only %s of free space), needs cleanup', percentUsed.toFixed(0), remaining)
    } else if (bytesRemaining === 0) {
      debug('Log is full, needs cleanup')
    } else {
      // prettier-ignore
      debug('Log is %s% full (%s beyond limit), needs cleanup', percentUsed.toFixed(0), remaining.slice(1))
    }
  }

  /**
   * @param {number} percentDeleted
   * @param {{ deletedBytes: any; }} stats
   */
  function reportCompactionNeed(percentDeleted, stats) {
    const unusedBytes = stats.deletedBytes
    const kbUnused = unusedBytes >> 10
    const mbUnused = unusedBytes >> 20
    const unused =
      mbUnused > 0
        ? `${mbUnused}MB`
        : kbUnused > 0
        ? `${kbUnused}KB`
        : `${unusedBytes}B`
    // prettier-ignore
    debug('Log is %s% deleted (%s of unused space), needs compaction', percentDeleted.toFixed(0), unused)
  }

  /**
   * Monitor the log size and schedule compaction and/or cleanup.
   *
   * @param {number} maxLogBytes
   */
  function monitorLogSize(maxLogBytes) {
    /** Number of records that match roughly 1% of the max log size */
    const CHECKPOINT = Math.floor((maxLogBytes * 0.01) / 500) // assuming 1 record = 500 bytes

    function checkLogSize() {
      peer.db.log.stats((err, stats) => {
        if (err) return
        const percentUsed = (stats.totalBytes / maxLogBytes) * 100
        const percentDeleted = (stats.deletedBytes / stats.totalBytes) * 100
        const needsCleanup = percentUsed > 80
        const needsCompaction = percentDeleted > 30

        // Schedule clean up
        if ((needsCleanup || needsCompaction) && !hasCleanupScheduled) {
          if (needsCleanup) reportCleanupNeed(percentUsed, maxLogBytes, stats)
          if (needsCompaction) reportCompactionNeed(percentDeleted, stats)
          hasCleanupScheduled = true
          if (needsCleanup) {
            cleanup(() => {
              compact(() => {
                hasCleanupScheduled = false
              })
            })
          } else {
            compact(() => {
              hasCleanupScheduled = false
            })
          }
        }
      })
    }

    let count = 0
    stopMonitoringLogSize = peer.db.onRecordAdded(() => {
      count += 1
      if (count >= CHECKPOINT) {
        count = 0
        checkLogSize()
      }
    })
    checkLogSize()
  }

  /**
   * @param {number?} maxLogBytes
   */
  function start(maxLogBytes) {
    stop()
    const actualMaxLogBytes = maxLogBytes ?? config.gc?.maxLogBytes ?? null
    // prettier-ignore
    if (!actualMaxLogBytes) throw new Error('gc plugin requires maxLogBytes via start() argument or config.gc.maxLogBytes')
    if (!stopMonitoringLogSize) {
      monitorLogSize(actualMaxLogBytes)
    }
  }

  function stop() {
    if (stopMonitoringLogSize) {
      stopMonitoringLogSize()
      stopMonitoringLogSize = null
    }
  }

  /**
   * @param {CB<void>} cb
   */
  function forceImmediately(cb) {
    debug('Force clean and compact immediately')
    cleanup(() => {
      compact(cb)
    })
  }

  return {
    start,
    stop,
    forceImmediately,
  }
}

exports.name = 'gc'
exports.needs = ['db', 'goals']
exports.init = initGC
