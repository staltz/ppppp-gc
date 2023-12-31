const test = require('node:test')
const assert = require('node:assert')
const p = require('node:util').promisify
const { createPeer } = require('./util')

function getFields(msgs) {
  return msgs
    .map((msg) => msg.data?.update)
    .filter((x) => !!x)
    .map((x) => x.age ?? x.name)
}

function isErased(msg) {
  return !msg.data
}

function isDeleted(msg) {
  return !msg
}

function isPresent(msg) {
  return !!msg.data.update
}

test('Dict ghosts', async (t) => {
  const alice = createPeer({
    name: 'alice',
    dict: { ghostSpan: 2 },
  })

  await alice.db.loaded()

  // Alice creates her own account
  const aliceID = await p(alice.db.account.create)({
    subdomain: 'account',
    _nonce: 'alice',
  })

  // Alice constructs a dict
  await p(alice.dict.load)(aliceID)
  await p(alice.dict.update)('profile', { name: 'alice' })
  await p(alice.dict.update)('profile', { age: 24 })
  await p(alice.dict.update)('profile', { name: 'Alice' })
  await p(alice.dict.update)('profile', { age: 25 })
  await p(alice.dict.update)('profile', { name: 'ALICE' })
  const dictID = alice.dict.getFeedID('profile')

  let mootID
  let msgID1
  let msgID2
  let msgID3
  let msgID4
  let msgID5
  for (const rec of alice.db.records()) {
    if (rec.msg.metadata.dataSize === 0) mootID = rec.id
    if (rec.msg.data?.update?.name === 'alice') msgID1 = rec.id
    if (rec.msg.data?.update?.age === 24) msgID2 = rec.id
    if (rec.msg.data?.update?.name === 'Alice') msgID3 = rec.id
    if (rec.msg.data?.update?.age === 25) msgID4 = rec.id
    if (rec.msg.data?.update?.name === 'ALICE') msgID5 = rec.id
  }

  // Assert situation before GC
  assert.deepEqual(
    getFields([...alice.db.msgs()]),
    ['alice', 24, 'Alice', 25, 'ALICE'],
    'has all dict msgs'
  )
  assert.ok(isErased(alice.db.get(mootID)), 'moot by def erased')
  assert.ok(isPresent(alice.db.get(msgID1)), 'msg1 exists')
  assert.ok(isPresent(alice.db.get(msgID2)), 'msg2 exists')
  assert.ok(isPresent(alice.db.get(msgID3)), 'msg3 exists')
  assert.ok(isPresent(alice.db.get(msgID4)), 'msg4 exists')
  assert.ok(isPresent(alice.db.get(msgID5)), 'msg5 exists')

  assert.deepEqual(
    await p(alice.db.log.stats)(),
    { totalBytes: 4520, deletedBytes: 0 },
    'log stats before'
  )

  // Perform garbage collection
  alice.goals.set(aliceID, 'all')
  alice.goals.set(dictID, 'dict')
  await p(alice.gc.forceImmediately)()

  // Assert situation after GC
  assert.deepEqual(
    getFields([...alice.db.msgs()]),
    [25, 'ALICE'],
    'alice has only field root msgs'
  )

  assert.deepEqual(
    await p(alice.db.log.stats)(),
    { totalBytes: 3121, deletedBytes: 0 },
    'log stats after'
  )

  assert.ok(isErased(alice.db.get(mootID)), 'moot by def erased')
  assert.ok(isDeleted(alice.db.get(msgID1)), 'msg1 deleted')
  assert.ok(isDeleted(alice.db.get(msgID2)), 'msg2 deleted') // ghost!
  assert.ok(isErased(alice.db.get(msgID3)), 'msg3 erased')
  assert.ok(isPresent(alice.db.get(msgID4)), 'msg4 exists')
  assert.ok(isPresent(alice.db.get(msgID5)), 'msg5 exists')

  assert.deepEqual(alice.db.ghosts.get(dictID), [msgID2])

  await p(alice.close)(true)
})
