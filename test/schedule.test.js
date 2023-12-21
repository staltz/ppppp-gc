const test = require('node:test')
const assert = require('node:assert')
const p = require('node:util').promisify
const { createPeer } = require('./util')

function getTexts(msgs) {
  return msgs.filter((msg) => msg.data?.text).map((msg) => msg.data.text)
}

test('Cleanup is scheduled automatically', async (t) => {
  const alice = createPeer({ name: 'alice' })
  await alice.db.loaded()

  // Alice creates her own account
  const aliceID = await p(alice.db.account.create)({
    subdomain: 'account',
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

  alice.gc.start(4 * 1024) // 4kB, approximately 8 messages
  await p(setTimeout)(3000)

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['A2', 'A3', 'A4'],
    'alice has only latest 3 msgs in the feed'
  )

  await p(alice.close)(true)
})

test('Compaction is scheduled automatically', async (t) => {
  const alice = createPeer({ name: 'alice' })
  await alice.db.loaded()

  // Alice creates her own account
  const aliceID = await p(alice.db.account.create)({
    subdomain: 'account',
    _nonce: 'alice',
  })

  const msgIDs = []
  for (let i = 0; i < 5; i++) {
    const rec = await p(alice.db.feed.publish)({
      account: aliceID,
      domain: 'post' + i,
      data: { text: String.fromCharCode(65 + i) },
    })
    msgIDs.push(rec.id)
  }

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['A', 'B', 'C', 'D', 'E'],
    'alice has 5 messages'
  )

  await p(alice.db.del)(msgIDs[0])
  await p(alice.db.del)(msgIDs[1])
  await p(alice.db.del)(msgIDs[3])
  await p(alice.db.del)(msgIDs[4])

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['C'],
    'alice has 1 message before compaction'
  )

  alice.goals.set(aliceID, 'all') // alice wants her account tangle
  alice.goals.set(alice.db.feed.getID(aliceID, 'post0'), 'all')
  alice.goals.set(alice.db.feed.getID(aliceID, 'post1'), 'all')
  alice.goals.set(alice.db.feed.getID(aliceID, 'post2'), 'all')
  alice.goals.set(alice.db.feed.getID(aliceID, 'post3'), 'all')
  alice.goals.set(alice.db.feed.getID(aliceID, 'post4'), 'all')

  alice.gc.start(6 * 1024) // 6kB, approximately 12 messages
  await p(setTimeout)(3000)

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['C'],
    'alice has 1 message after compaction'
  )

  await p(alice.close)(true)
})
