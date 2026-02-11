'use strict'

const crypto = require('crypto')
const fs = require('fs')

/**
 * Compute SHA-256 hash of a file.
 * Returns hex-encoded hash string.
 */
async function hashFile (filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256')
    const stream = fs.createReadStream(filePath)
    stream.on('data', chunk => hash.update(chunk))
    stream.on('end', () => resolve(hash.digest('hex')))
    stream.on('error', reject)
  })
}

/**
 * Compute SHA-256 hash of a Buffer.
 */
function hashBuffer (buf) {
  return crypto.createHash('sha256').update(buf).digest('hex')
}

/**
 * Verify a file matches the expected SHA-256 hash.
 */
async function verifyFile (filePath, expectedHash) {
  const actual = await hashFile(filePath)
  return { valid: actual === expectedHash, actual, expected: expectedHash }
}

module.exports = { hashFile, hashBuffer, verifyFile }
