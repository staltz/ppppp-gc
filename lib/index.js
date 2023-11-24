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
 * @param {{ db: PPPPPDB | null }} peer
 * @returns {asserts peer is { db: PPPPPDB }}
 */
function assertDBPlugin(peer) {
  if (!peer.db) throw new Error('gc plugin requires ppppp-db plugin')
}

/**
 * @param {{ goals: PPPPPGoal | null }} peer
 * @returns {asserts peer is { goals: PPPPPGoal }}
 */
function assertGoalsPlugin(peer) {
  if (!peer.goals) throw new Error('gc plugin requires ppppp-goals plugin')
}

/**
 * @param {Config} config
 * @returns {asserts config is ExpectedConfig}
 */
function assertValidConfig(config) {
  if (typeof config.gc?.maxLogBytes !== 'number') {
    throw new Error('gc requires config.gc.maxLogBytes')
  }
}

/**
 * @param {{ db: PPPPPDB | null, goals: PPPPPGoal | null }} peer
 * @param {Config} config
 */
function initGC(peer, config) {
  // Assertions
  assertDBPlugin(peer)
  assertGoalsPlugin(peer)
  assertValidConfig(config)

  const MAX_LOG_BYTES = config.gc.maxLogBytes

  /** Number of records that match roughly 1% of the max log size */
  const CHECKPOINT = Math.floor((MAX_LOG_BYTES * 0.01) / 500) // assuming 1 record = 500 bytes

  // State
  const debug = makeDebug('ppppp:gc')
  let stopMonitoringLogSize = /** @type {CallableFunction | null} */ (null)
  let hasCleanupScheduled = false

  /**
   * Deletes messages that don't correspond to any goal.
   * @private
   * @param {CB<void>} cb
   */
  function cleanup(cb) {
    assertDBPlugin(peer)
    assertGoalsPlugin(peer)
    debug('Cleanup started')
    const startTime = Date.now()

    const done = multicb({ pluck: 1 })
    let waiting = false
    for (const rec of peer.db.records()) {
      if (!rec.msg) continue
      const { id: msgID, msg } = rec
      const [purpose, details] = peer.goals.getMsgPurpose(msgID, msg)
      if (purpose === 'none') {
        peer.db.del(msgID, done())
        waiting = true
      } else if (purpose === 'ghost') {
        const { tangleID, span } = details
        const cb = done()
        // TODO: Could one msg be a ghostable in MANY tangles? Or just one?
        peer.db.ghosts.add({ tangleID, msgID, span }, (err) => {
          // prettier-ignore
          if (err) return cb(new Error('gc failed to add ghost', { cause: err }))
          peer.db.del(msgID, cb)
        })
        waiting = true
      } else if (purpose === 'trail') {
        peer.db.erase(msgID, done())
        waiting = true
      }
    }
    /** @param {Error=} err */
    function whenEnded(err) {
      const duration = Date.now() - startTime
      if (err) debug('Cleanup ended with an error %s', err.message ?? err)
      else debug('Cleanup completed in %sms', duration)
      cb()
    }
    if (waiting) done(whenEnded)
    else whenEnded()
  }

  /**
   * Recreates the log so it has no empty space.
   * @private
   * @param {CB<void>} cb
   */
  function compact(cb) {
    assertDBPlugin(peer)
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
   * @param {{ totalBytes: number; }} stats
   */
  function reportCleanupNeed(percentUsed, stats) {
    const bytesRemaining = MAX_LOG_BYTES - stats.totalBytes
    const kbRemaining = bytesRemaining >> 10
    const mbRemaining = bytesRemaining >> 20
    const remaining =
      mbRemaining > 0
        ? `${mbRemaining}MB`
        : kbRemaining > 0
        ? `${kbRemaining}KB`
        : `${bytesRemaining}B`
    // prettier-ignore
    debug('Log is %s% full (only %s of free space), needs cleanup', percentUsed.toFixed(0), remaining)
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
   */
  function monitorLogSize() {
    assertDBPlugin(peer)
    function checkLogSize() {
      assertDBPlugin(peer)
      peer.db.log.stats((err, stats) => {
        if (err) return
        const percentUsed = (stats.totalBytes / MAX_LOG_BYTES) * 100
        const percentDeleted = (stats.deletedBytes / stats.totalBytes) * 100
        const needsCleanup = percentUsed > 80
        const needsCompaction = percentDeleted > 30

        // Schedule clean up
        if ((needsCleanup || needsCompaction) && !hasCleanupScheduled) {
          if (needsCleanup) reportCleanupNeed(percentUsed, stats)
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

  function start() {
    if (!stopMonitoringLogSize) {
      monitorLogSize()
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
exports.init = initGC
