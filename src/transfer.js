'use strict'

const Hyperswarm = require('hyperswarm')
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const b4a = require('b4a')
const { hashBuffer } = require('./verify')

// Protocol message types
const MSG = {
  META: 0x01,     // File metadata: { name, size, hash }
  DATA: 0x02,     // File chunk
  DONE: 0x03,     // Transfer complete
  ACK: 0x04,      // Receiver acknowledges (hash verified)
  NACK: 0x05,     // Receiver rejects (hash mismatch)
  ERROR: 0x06     // Error message
}

const CHUNK_SIZE = 64 * 1024 // 64KB chunks

/**
 * Derive a Hyperswarm topic from a transfer/task ID.
 * Both peers derive the same topic to find each other.
 */
function deriveTopic (transferId) {
  return crypto.createHash('sha256').update(`ats-p2p:${transferId}`).digest()
}

/**
 * Encode a protocol message: [1 byte type][4 byte length][payload]
 */
function encodeMessage (type, payload) {
  const data = Buffer.isBuffer(payload) ? payload : Buffer.from(JSON.stringify(payload))
  const header = Buffer.alloc(5)
  header[0] = type
  header.writeUInt32BE(data.length, 1)
  return Buffer.concat([header, data])
}

/**
 * MessageReader: accumulates data from a stream and yields complete protocol messages.
 */
class MessageReader {
  constructor () {
    this.buffer = Buffer.alloc(0)
  }

  /**
   * Feed data and return array of parsed messages.
   */
  feed (chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk])
    const messages = []
    while (this.buffer.length >= 5) {
      const type = this.buffer[0]
      const len = this.buffer.readUInt32BE(1)
      if (this.buffer.length < 5 + len) break // incomplete
      const payload = this.buffer.subarray(5, 5 + len)
      this.buffer = this.buffer.subarray(5 + len)
      messages.push({ type, payload })
    }
    return messages
  }
}

/**
 * Send a file over a Hyperswarm connection.
 *
 * @param {Object} opts
 * @param {string} opts.filePath - Path to file to send
 * @param {string} opts.transferId - Unique transfer ID (used to derive topic)
 * @param {string} opts.fileHash - Pre-computed SHA-256 hash of the file
 * @param {number} opts.fileSize - File size in bytes
 * @param {string[]} [opts.allowedPeers] - Allowed remote public keys (hex)
 * @param {number} [opts.timeout=60000] - Connection timeout ms
 * @param {Function} [opts.onProgress] - Progress callback ({ bytesSent, totalBytes })
 * @returns {Promise<{ success: boolean, peerKey: string }>}
 */
async function sendFile (opts) {
  const {
    filePath, transferId, fileHash, fileSize,
    allowedPeers, timeout = 60000, onProgress,
    bootstrap // optional DHT bootstrap nodes for testing
  } = opts

  const topic = deriveTopic(transferId)
  const swarmOpts = bootstrap ? { bootstrap } : {}
  const swarm = new Hyperswarm(swarmOpts)

  return new Promise((resolve, reject) => {
    let done = false
    const timer = setTimeout(() => {
      if (!done) {
        done = true
        swarm.destroy().catch(() => {})
        reject(new Error(`Send timeout after ${timeout}ms`))
      }
    }, timeout)

    swarm.on('connection', (conn, info) => {
      if (done) { conn.destroy(); return }

      // Peer allowlist check
      const peerKey = b4a.toString(conn.remotePublicKey, 'hex')
      if (allowedPeers && allowedPeers.length > 0 && !allowedPeers.includes(peerKey)) {
        conn.destroy()
        return
      }

      const reader = new MessageReader()
      let doneSent = false // tracks whether we sent the DONE message

      conn.on('data', chunk => {
        const messages = reader.feed(chunk)
        for (const msg of messages) {
          if (msg.type === MSG.ACK) {
            done = true
            clearTimeout(timer)
            conn.destroy()
            swarm.destroy().catch(() => {})
            resolve({ success: true, peerKey })
          } else if (msg.type === MSG.NACK) {
            done = true
            clearTimeout(timer)
            conn.destroy()
            swarm.destroy().catch(() => {})
            resolve({ success: false, peerKey, reason: msg.payload.toString() })
          }
        }
      })

      conn.on('error', err => {
        if (!done) {
          done = true
          clearTimeout(timer)
          swarm.destroy().catch(() => {})
          // If we already sent the file and the connection was reset,
          // it likely means the receiver NACK'd and closed before we read it.
          if (doneSent) {
            resolve({ success: false, peerKey, reason: 'Connection closed by receiver (likely rejected)' })
          } else {
            reject(err)
          }
        }
      })

      conn.on('close', () => {
        if (!done && doneSent) {
          // Connection closed after we sent everything but before ACK/NACK — treat as rejection
          done = true
          clearTimeout(timer)
          swarm.destroy().catch(() => {})
          resolve({ success: false, peerKey, reason: 'Connection closed by receiver' })
        }
      })

      // Send metadata
      const fileName = path.basename(filePath)
      conn.write(encodeMessage(MSG.META, { name: fileName, size: fileSize, hash: fileHash }))

      // Stream file in chunks
      const stream = fs.createReadStream(filePath, { highWaterMark: CHUNK_SIZE })
      let bytesSent = 0

      stream.on('data', chunk => {
        conn.write(encodeMessage(MSG.DATA, chunk))
        bytesSent += chunk.length
        if (onProgress) onProgress({ bytesSent, totalBytes: fileSize })
      })

      stream.on('end', () => {
        doneSent = true
        conn.write(encodeMessage(MSG.DONE, {}))
      })

      stream.on('error', err => {
        conn.write(encodeMessage(MSG.ERROR, { message: err.message }))
        if (!done) {
          done = true
          clearTimeout(timer)
          conn.destroy()
          swarm.destroy().catch(() => {})
          reject(err)
        }
      })
    })

    // Sender announces as server — receiver will look up as client
    const discovery = swarm.join(topic, { server: true, client: false })
    discovery.flushed().catch(err => {
      if (!done) {
        done = true
        clearTimeout(timer)
        swarm.destroy().catch(() => {})
        reject(err)
      }
    })
  })
}

/**
 * Receive a file over a Hyperswarm connection.
 *
 * @param {Object} opts
 * @param {string} opts.transferId - Transfer ID to derive topic from
 * @param {string} opts.savePath - Where to save the received file
 * @param {string} opts.expectedHash - Expected SHA-256 hash
 * @param {number} opts.expectedSize - Expected file size
 * @param {string[]} [opts.allowedPeers] - Allowed remote public keys (hex)
 * @param {number} [opts.timeout=60000] - Connection timeout ms
 * @param {number} [opts.maxFileSize] - Maximum allowed file size in bytes
 * @param {Function} [opts.onProgress] - Progress callback ({ bytesReceived, totalBytes })
 * @returns {Promise<{ success: boolean, filePath: string, peerKey: string }>}
 */
async function receiveFile (opts) {
  const {
    transferId, savePath, expectedHash, expectedSize,
    allowedPeers, timeout = 60000, maxFileSize, onProgress,
    bootstrap // optional DHT bootstrap nodes for testing
  } = opts

  const topic = deriveTopic(transferId)
  const swarmOpts = bootstrap ? { bootstrap } : {}
  const swarm = new Hyperswarm(swarmOpts)

  return new Promise((resolve, reject) => {
    let done = false
    const timer = setTimeout(() => {
      if (!done) {
        done = true
        swarm.destroy().catch(() => {})
        reject(new Error(`Receive timeout after ${timeout}ms`))
      }
    }, timeout)

    swarm.on('connection', (conn, info) => {
      if (done) { conn.destroy(); return }

      const peerKey = b4a.toString(conn.remotePublicKey, 'hex')
      if (allowedPeers && allowedPeers.length > 0 && !allowedPeers.includes(peerKey)) {
        conn.destroy()
        return
      }

      let meta = null
      const chunks = []
      let bytesReceived = 0
      const reader = new MessageReader()

      conn.on('data', chunk => {
        const messages = reader.feed(chunk)
        for (const msg of messages) {
          if (msg.type === MSG.META) {
            meta = JSON.parse(msg.payload.toString())
            if (maxFileSize && meta.size > maxFileSize) {
              conn.write(encodeMessage(MSG.NACK, `File too large: ${meta.size} > ${maxFileSize}`))
              done = true
              clearTimeout(timer)
              conn.destroy()
              swarm.destroy().catch(() => {})
              resolve({ success: false, reason: 'File too large' })
              return
            }
          } else if (msg.type === MSG.DATA) {
            chunks.push(Buffer.from(msg.payload))
            bytesReceived += msg.payload.length
            if (onProgress && meta) onProgress({ bytesReceived, totalBytes: meta.size })
          } else if (msg.type === MSG.DONE) {
            // Reassemble and verify
            const fileData = Buffer.concat(chunks)
            const actualHash = hashBuffer(fileData)

            if (actualHash === expectedHash) {
              // Ensure save directory exists
              const dir = path.dirname(savePath)
              fs.mkdirSync(dir, { recursive: true })
              fs.writeFileSync(savePath, fileData)

              conn.write(encodeMessage(MSG.ACK, { hash: actualHash }))
              done = true
              clearTimeout(timer)
              // Small delay so ACK gets sent before connection closes
              setTimeout(() => {
                conn.destroy()
                swarm.destroy().catch(() => {})
                resolve({ success: true, filePath: savePath, peerKey, hash: actualHash })
              }, 200)
            } else {
              conn.write(encodeMessage(MSG.NACK, `Hash mismatch: expected ${expectedHash}, got ${actualHash}`))
              done = true
              clearTimeout(timer)
              conn.destroy()
              swarm.destroy().catch(() => {})
              resolve({
                success: false,
                reason: `Hash mismatch: expected ${expectedHash}, got ${actualHash}`,
                peerKey
              })
            }
          } else if (msg.type === MSG.ERROR) {
            done = true
            clearTimeout(timer)
            conn.destroy()
            swarm.destroy().catch(() => {})
            reject(new Error(`Remote error: ${msg.payload.toString()}`))
          }
        }
      })

      conn.on('error', err => {
        if (!done) {
          done = true
          clearTimeout(timer)
          swarm.destroy().catch(() => {})
          reject(err)
        }
      })
    })

    // Receiver looks up as client — sender announces as server
    const discovery = swarm.join(topic, { server: false, client: true })
    discovery.flushed().catch(err => {
      if (!done) {
        done = true
        clearTimeout(timer)
        swarm.destroy().catch(() => {})
        reject(err)
      }
    })
  })
}

module.exports = { sendFile, receiveFile, deriveTopic, encodeMessage, MessageReader, MSG, CHUNK_SIZE }
