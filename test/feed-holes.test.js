const test = require('node:test')
const assert = require('node:assert')
const p = require('node:util').promisify
const { createPeer } = require('./util')

function getTexts(msgs) {
  return msgs.filter((msg) => msg.data?.text).map((msg) => msg.data.text)
}

test('Feed holes', async (t) => {
  const alice = createPeer({
    name: 'alice',
    gc: { maxLogBytes: 100 * 1024 * 1024 },
  })

  await alice.db.loaded()

  // Alice creates her own account
  const aliceID = await p(alice.db.account.create)({
    subdomain: 'account',
    _nonce: 'alice',
  })

  const posts = []
  for (let i = 0; i < 10; i++) {
    const rec = await p(alice.db.feed.publish)({
      account: aliceID,
      domain: 'post',
      data: { text: 'A' + i },
    })
    posts.push(rec.id)
  }

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['A0', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9'],
    'alice has the whole feed'
  )

  await p(alice.db.del)(posts[3])
  await p(alice.db.del)(posts[4])
  await p(alice.db.del)(posts[5])
  await p(alice.db.erase)(posts[6]) // vital as trail from A7
  assert('alice deleted the middle part of the feed')

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['A0', 'A1', 'A2', /*                   */ 'A7', 'A8', 'A9'],
    'alice has the beginning and the end of the feed'
  )

  alice.goals.set(aliceID, 'all') // alice wants her account tangle
  const postFeedID = alice.db.feed.getID(aliceID, 'post')
  // notice 4 on purpose, because we want to make sure A2 is deleted
  alice.goals.set(postFeedID, 'newest-4')
  assert('alice set a goal for newest-4 of post feed')

  await p(alice.gc.forceImmediately)()

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    [/*                                     */ 'A7', 'A8', 'A9'],
    'alice has only the end of the feed'
  )

  await p(alice.close)(true)
})
