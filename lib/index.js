const makeDebug = require('debug')
const multicb = require('multicb')

module.exports = {
  name: 'gc',
  manifest: {},
  permissions: {
    anonymous: {},
  },
  init(peer, config) {
    if (!peer.goals) throw new Error('gc requires the goals plugin')
    const debug = makeDebug('ppppp:gc')

    function purgeGoallessMsgs(cb) {
      debug('purge goalless msgs')
      const done = multicb()
      let waitingForDels = false
      for (const rec of peer.db.records()) {
        if (!rec.msg) continue
        const goals = peer.goals.getByRec(rec)
        if (goals.length === 0) {
          peer.db.del(rec.id, done())
          waitingForDels = true
        }
      }
      if (waitingForDels) done(cb)
      else cb()
    }

    function initiate() {}

    function forceImmediately(cb) {
      debug('force immediately')
      purgeGoallessMsgs(cb)
    }

    return {
      initiate,
      forceImmediately,
    }
  },
}
