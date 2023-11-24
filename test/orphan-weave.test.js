const test = require('node:test')
const assert = require('node:assert')
const p = require('node:util').promisify
const Keypair = require('ppppp-keypair')
const { createPeer } = require('./util')

const bobKeypair = Keypair.generate('ed25519', 'bob')
const carolKeypair = Keypair.generate('ed25519', 'carol')

function getTexts(msgs) {
  return msgs.filter((msg) => msg.data?.text).map((msg) => msg.data.text)
}

test('Orphan weave msgs', async (t) => {
  const alice = createPeer({
    name: 'alice',
  })

  await alice.db.loaded()

  // Alice creates her own account
  const aliceID = await p(alice.db.account.create)({
    subdomain: 'account',
    _nonce: 'alice',
  })
  // Alice creates Bob
  const bobID = await p(alice.db.account.create)({
    subdomain: 'account',
    keypair: bobKeypair,
    _nonce: 'bob',
  })
  // Alice creates Carol
  const carolID = await p(alice.db.account.create)({
    subdomain: 'account',
    keypair: carolKeypair,
    _nonce: 'carol',
  })

  const threadRoot = await p(alice.db.feed.publish)({
    account: bobID,
    keypair: bobKeypair,
    domain: 'post',
    data: { text: 'B0' },
  })
  const threadReply1 = await p(alice.db.feed.publish)({
    account: aliceID,
    domain: 'post',
    data: { text: 'A1' },
    tangles: [threadRoot.id],
  })
  const threadReply2 = await p(alice.db.feed.publish)({
    account: carolID,
    domain: 'post',
    data: { text: 'C1' },
    tangles: [threadRoot.id],
  })

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['B0', 'A1', 'C1'],
    'alice has the full thread'
  )

  await p(alice.db.del)(threadRoot.id)
  assert('alice deleted the root')

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['A1', 'C1'],
    'alice has only thread replies'
  )

  alice.goals.set(aliceID, 'all') // alice wants her account tangle
  alice.goals.set(bobID, 'all') // alice wants bob's account tangle
  alice.goals.set(carolID, 'all') // alice wants carol's account tangle
  const postFeedID = alice.db.feed.getID(aliceID, 'post')
  alice.goals.set(postFeedID, 'all') // alice wants her post feed

  await p(alice.gc.forceImmediately)()

  assert.deepEqual(
    getTexts([...alice.db.msgs()]),
    ['A1'],
    'alice does not have the thread, except her own reply'
  )

  await p(alice.close)(true)
})
