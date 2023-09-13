const test = require('node:test')
const assert = require('node:assert')
const p = require('node:util').promisify
const { createPeer } = require('./util')

function getTexts(msgs) {
  return msgs.filter((msg) => msg.data?.text).map((msg) => msg.data.text)
}

test('feed decay', async (t) => {
  const alice = createPeer({
    name: 'alice',
    gc: { maxLogBytes: 100 * 1024 * 1024 },
  })

  await alice.db.loaded()

  // Alice creates her own account
  const aliceID = await p(alice.db.account.create)({
    domain: 'account',
    _nonce: 'alice',
  })

  for (let i = 0; i < 5; i++) {
    await p(alice.db.feed.publish)({
      account: aliceID,
      domain: 'post',
      data: { text: 'A' + i },
    })
  }

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['A0', 'A1', 'A2', 'A3', 'A4'],
    'alice has the whole feed'
  )

  alice.goals.set(aliceID, 'all') // alice wants her account tangle
  const postFeedID = alice.db.feed.getID(aliceID, 'post')
  alice.goals.set(postFeedID, 'newest-3')
  assert('alice set a goal for newest-3 of post feed')

  await p(alice.gc.forceImmediately)()

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['A2', 'A3', 'A4'],
    'alice has only latest 3 msgs in the feed'
  )

  await p(alice.close)(true)
})
