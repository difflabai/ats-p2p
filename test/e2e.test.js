'use strict'

/**
 * End-to-end test: local P2P file transfer between two nodes.
 *
 * Uses a local DHT bootstrap node so two Hyperswarm instances
 * on the same machine can discover each other immediately.
 *
 * Run: node test/e2e.test.js
 */

const fs = require('fs')
const os = require('os')
const path = require('path')
const crypto = require('crypto')
const createTestnet = require('hyperdht/testnet')
const { sendFile, receiveFile } = require('../src/transfer')
const { hashFile, verifyFile } = require('../src/verify')

const TEMP_DIR = path.join(os.tmpdir(), `ats-p2p-test-${Date.now()}`)
const TEST_FILE = path.join(TEMP_DIR, 'test-send.bin')
const RECV_FILE = path.join(TEMP_DIR, 'test-recv.bin')

let passed = 0
let failed = 0
let bootstrap = null
let testnet = null

function assert (condition, msg) {
  if (condition) {
    passed++
    console.log(`  PASS: ${msg}`)
  } else {
    failed++
    console.error(`  FAIL: ${msg}`)
  }
}

async function setupDHT () {
  testnet = await createTestnet(3)
  bootstrap = testnet.bootstrap
  console.log(`  Local testnet with ${bootstrap.length} bootstrap nodes`)
}

async function teardownDHT () {
  if (testnet) await testnet.destroy()
}

async function testHashVerification () {
  console.log('\n--- Test: Hash Verification ---')

  const data = crypto.randomBytes(1024)
  fs.writeFileSync(TEST_FILE, data)

  const hash = await hashFile(TEST_FILE)
  assert(typeof hash === 'string' && hash.length === 64, 'hashFile returns 64-char hex string')

  const result = await verifyFile(TEST_FILE, hash)
  assert(result.valid === true, 'verifyFile returns valid=true for correct hash')

  const badResult = await verifyFile(TEST_FILE, 'deadbeef'.repeat(8))
  assert(badResult.valid === false, 'verifyFile returns valid=false for wrong hash')
}

async function testP2PTransfer () {
  console.log('\n--- Test: P2P Transfer via Hyperswarm ---')

  const fileContent = crypto.randomBytes(256 * 1024) // 256KB
  fs.writeFileSync(TEST_FILE, fileContent)

  const fileHash = await hashFile(TEST_FILE)
  const fileSize = fs.statSync(TEST_FILE).size
  const transferId = `test-${Date.now()}`

  console.log(`  File: ${fileSize} bytes, hash: ${fileHash.slice(0, 16)}...`)

  let senderProgress = 0
  let receiverProgress = 0

  // Sender announces first (as server), then receiver looks up (as client).
  // In production, ATS signaling creates this natural delay.
  const sendPromise = sendFile({
    filePath: TEST_FILE,
    transferId,
    fileHash,
    fileSize,
    timeout: 30000,
    bootstrap,
    onProgress: ({ bytesSent, totalBytes }) => {
      senderProgress = bytesSent
    }
  })

  // Give sender time to announce on the DHT before receiver looks up
  await new Promise(r => setTimeout(r, 2000))

  const recvPromise = receiveFile({
    transferId,
    savePath: RECV_FILE,
    expectedHash: fileHash,
    expectedSize: fileSize,
    timeout: 30000,
    bootstrap,
    onProgress: ({ bytesReceived, totalBytes }) => {
      receiverProgress = bytesReceived
    }
  })

  const [sendResult, recvResult] = await Promise.all([sendPromise, recvPromise])

  assert(sendResult.success === true, 'Sender reports success')
  assert(recvResult.success === true, 'Receiver reports success')
  assert(recvResult.hash === fileHash, 'Received file hash matches original')
  assert(fs.existsSync(RECV_FILE), 'Received file exists on disk')

  // Verify the received file independently
  const verification = await verifyFile(RECV_FILE, fileHash)
  assert(verification.valid === true, 'Independent hash verification passes')

  // Compare file contents byte-by-byte
  const original = fs.readFileSync(TEST_FILE)
  const received = fs.readFileSync(RECV_FILE)
  assert(original.equals(received), 'File contents are byte-identical')

  assert(senderProgress === fileSize, 'Sender progress reached 100%')
  assert(receiverProgress === fileSize, 'Receiver progress reached 100%')
}

async function testHashMismatchRejection () {
  console.log('\n--- Test: Hash Mismatch Rejection ---')

  const fileContent = crypto.randomBytes(32 * 1024) // 32KB
  fs.writeFileSync(TEST_FILE, fileContent)

  const fileHash = await hashFile(TEST_FILE)
  const fileSize = fs.statSync(TEST_FILE).size
  const transferId = `test-mismatch-${Date.now()}`
  const wrongHash = 'a'.repeat(64)

  const sendPromise = sendFile({
    filePath: TEST_FILE,
    transferId,
    fileHash,
    fileSize,
    timeout: 30000,
    bootstrap
  })

  await new Promise(r => setTimeout(r, 2000))

  const recvPromise = receiveFile({
    transferId,
    savePath: RECV_FILE + '.bad',
    expectedHash: wrongHash,
    expectedSize: fileSize,
    timeout: 30000,
    bootstrap
  })

  const [sendResult, recvResult] = await Promise.all([sendPromise, recvPromise])

  assert(sendResult.success === false, 'Sender gets NACK on hash mismatch')
  assert(recvResult.success === false, 'Receiver reports failure on hash mismatch')
  assert(recvResult.reason && recvResult.reason.includes('mismatch'), 'Failure reason mentions mismatch')
}

async function cleanup () {
  try { fs.rmSync(TEMP_DIR, { recursive: true, force: true }) } catch {}
}

async function run () {
  console.log('ats-p2p E2E Tests')
  console.log('=================')

  fs.mkdirSync(TEMP_DIR, { recursive: true })

  try {
    await setupDHT()
    await testHashVerification()
    await testP2PTransfer()
    await testHashMismatchRejection()
  } catch (err) {
    console.error(`\nUNEXPECTED ERROR: ${err.message}`)
    console.error(err.stack)
    failed++
  } finally {
    await teardownDHT()
    await cleanup()
  }

  console.log(`\n=================`)
  console.log(`Results: ${passed} passed, ${failed} failed`)

  if (failed > 0) {
    process.exit(1)
  } else {
    console.log('All tests passed!')
  }
}

run()
