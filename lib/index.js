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
function assertDBExists(peer) {
  if (!peer.db) throw new Error('gc plugin requires ppppp-db plugin')
}

/**
 * @param {{ goals: PPPPPGoal | null }} peer
 * @returns {asserts peer is { goals: PPPPPGoal }}
 */
function assertGoalsExists(peer) {
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
  assertDBExists(peer)
  assertGoalsExists(peer)
  assertValidConfig(config)

  // Constants
  const COMPACTION_INTERVAL = config.gc?.compactionInterval ?? 120e3

  // State
  const debug = makeDebug('ppppp:gc')
  let lastCompacted = Date.now()
  let stopMonitoringLogSize = /** @type {CallableFunction | null} */ (null)
  let hasCompactionScheduled = false
  let hasCleanupScheduled = false

  /**
   * Deletes messages that don't correspond to any goal.
   * @private
   * @param {CB<void>} cb
   */
  function cleanup(cb) {
    assertDBExists(peer)
    assertGoalsExists(peer)
    debug('cleanup-per-purpose started')

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
      // prettier-ignore
      if (err) debug('cleanup-per-purpose ended with an error %s', err.message ?? err)
      else debug('cleanup-per-purpose ended')
      cb()
    }
    if (waiting) done(whenEnded)
    else whenEnded()
  }

  /**
   * Compact the log (remove deleted records by filling in all the blanks).
   * @private
   * @param {number} waitPeriod
   * @param {CB<void>} cb
   */
  function compact(waitPeriod, cb) {
    assertDBExists(peer)
    const log = peer.db._getLog() // TODO: use public API?
    debug('compaction started')
    /** @param {Error=} err */
    function whenEnded(err) {
      if (err) debug('compaction ended with an error %s', err.message ?? err)
      else debug('compaction ended')
      cb()
    }
    if (waitPeriod > 0) {
      setTimeout(log.compact, waitPeriod, whenEnded)
    } else {
      log.compact(whenEnded)
    }
  }

  /**
   * Monitor the log size and schedule compaction and/or cleanup.
   */
  function monitorLogSize() {
    assertDBExists(peer)
    function checkLogSize() {
      assertDBExists(peer)
      assertValidConfig(config)
      peer.db.logStats((err, stats) => {
        if (err) return
        const percentUsed = (stats.totalBytes / config.gc.maxLogBytes) * 100
        const needsCompaction = stats.deletedBytes > 0

        // Schedule compaction
        if (needsCompaction && !hasCompactionScheduled) {
          const nextCompacted = lastCompacted + COMPACTION_INTERVAL
          const waitPeriod = Math.max(0, nextCompacted - Date.now())
          hasCompactionScheduled = true
          compact(waitPeriod, () => {
            hasCompactionScheduled = false
          })
        }

        // Schedule clean up
        if (percentUsed > 80 && !hasCleanupScheduled) {
          hasCleanupScheduled = true
          cleanup(() => {
            hasCleanupScheduled = false
          })
        }
      })
    }

    let count = 0
    stopMonitoringLogSize = peer.db.onRecordAdded(() => {
      count += 1
      if (count >= 1000) {
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
    debug('force immediately')
    cleanup(cb)
  }

  return {
    start,
    stop,
    forceImmediately,
  }
}

exports.name = 'gc'
exports.init = initGC
