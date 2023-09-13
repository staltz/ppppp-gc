const makeDebug = require('debug')
const multicb = require('multicb')

module.exports = {
  name: 'gc',
  manifest: {},
  permissions: {
    anonymous: {},
  },

  /**
   * @param {any} peer
   * @param {{
   *   gc?: {
   *     maxLogBytes?: number
   *   }
   * }} config
   */
  init(peer, config) {
    // Assertions
    if (!peer.goals) throw new Error('gc requires the goals plugin')
    if (typeof config.gc?.maxLogBytes !== 'number') {
      throw new Error('gc requires config.gc.maxLogBytes')
    }

    // State
    const debug = makeDebug('ppppp:gc')

    function cleanup(cb) {
      debug('cleanup goalless started')
      const done = multicb({ pluck: 1 })
      let waitingForDels = false
      for (const rec of peer.db.records()) {
        if (!rec.msg) continue
        const goals = peer.goals.getByRec(rec)
        if (goals.length === 0) {
          peer.db.del(rec.id, done())
          waitingForDels = true
        } else {
        }
      }
      function whenEnded(err) {
        // prettier-ignore
        if (err) debug('cleanup goalless ended with an error %s', err.message ?? err)
        else debug('cleanup goalless ended')
        cb()
      }
      if (waitingForDels) done(whenEnded)
      else whenEnded()
    }

    function forceImmediately(cb) {
      debug('force immediately')
      cleanup(cb)
    }

    return {
      initiate,
      forceImmediately,
    }
  },
}
