'use strict'

const { EventEmitter } = require('events')
const { execSync, exec } = require('child_process')
const fs = require('fs')
const path = require('path')
const { hashFile, verifyFile } = require('./verify')
const { sendFile, receiveFile } = require('./transfer')

const DEFAULT_ATS_URL = 'https://ats.difflab.ai'

/**
 * P2PNode — a file-sharing peer in the ATS-P2P network.
 *
 * Uses ATS for signaling (file offers/accepts) and Hyperswarm for
 * encrypted P2P byte transfer.
 */
class P2PNode extends EventEmitter {
  /**
   * @param {Object} opts
   * @param {string} [opts.atsUrl] - ATS server URL
   * @param {string} opts.actorId - This node's actor ID
   * @param {string} [opts.actorName] - Display name
   * @param {string[]} [opts.allowedPeers] - Allowed peer actor IDs
   * @param {number} [opts.maxFileSize] - Max file size in bytes (0 = unlimited)
   * @param {number} [opts.timeout=120000] - Transfer timeout ms
   */
  constructor (opts = {}) {
    super()
    this.atsUrl = opts.atsUrl || process.env.ATS_URL || DEFAULT_ATS_URL
    this.actorId = opts.actorId || process.env.ATS_ACTOR_ID || 'p2p-node'
    this.actorName = opts.actorName || opts.actorId || 'P2P Node'
    this.allowedPeers = opts.allowedPeers || []
    this.maxFileSize = opts.maxFileSize || 0
    this.timeout = opts.timeout || 120000
    this.transfers = new Map() // transferId -> status
    this._watching = false
    this._watchProcess = null
  }

  /**
   * Execute an ATS CLI command and return parsed JSON output.
   */
  _ats (args) {
    const flags = `--url ${this.atsUrl} --actor-type agent --actor-id ${this.actorId} --actor-name "${this.actorName}" -f json`
    const cmd = `ats ${args} ${flags}`
    const result = execSync(cmd, { encoding: 'utf8', timeout: 15000 })
    try {
      return JSON.parse(result)
    } catch {
      return result.trim()
    }
  }

  /**
   * Execute an ATS CLI command asynchronously.
   */
  _atsAsync (args) {
    return new Promise((resolve, reject) => {
      const flags = `--url ${this.atsUrl} --actor-type agent --actor-id ${this.actorId} --actor-name "${this.actorName}" -f json`
      const cmd = `ats ${args} ${flags}`
      exec(cmd, { encoding: 'utf8', timeout: 15000 }, (err, stdout) => {
        if (err) return reject(err)
        try { resolve(JSON.parse(stdout)) } catch { resolve(stdout.trim()) }
      })
    })
  }

  /**
   * Send a file to another agent.
   *
   * Flow:
   * 1. Hash the file
   * 2. Create a file_offer task on ATS with metadata
   * 3. Wait for receiver to accept (creates file_accept task)
   * 4. Both derive Hyperswarm topic from task ID and transfer P2P
   * 5. Return transfer result
   *
   * @param {Object} opts
   * @param {string} opts.filePath - Path to file
   * @param {string} opts.recipientId - Target agent's actor ID
   * @param {string} [opts.channel='p2p-transfer'] - ATS channel
   * @param {Function} [opts.onProgress] - Progress callback
   * @returns {Promise<{ transferId: string, taskId: number, success: boolean }>}
   */
  async send (opts) {
    const { filePath, recipientId, channel = 'p2p-transfer', onProgress } = opts

    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`)
    }

    const stat = fs.statSync(filePath)
    const fileSize = stat.size
    const fileName = path.basename(filePath)
    const fileHash = await hashFile(filePath)

    const transferId = `${this.actorId}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

    this.transfers.set(transferId, {
      status: 'offering',
      direction: 'send',
      fileName,
      fileSize,
      fileHash,
      recipientId,
      startedAt: new Date().toISOString()
    })

    // Create file_offer task
    const payload = JSON.stringify({
      transfer_id: transferId,
      sender_id: this.actorId,
      recipient_id: recipientId,
      file_name: fileName,
      file_size: fileSize,
      file_hash: fileHash
    })

    const task = this._ats(`create "${fileName}" --type file_offer --channel ${channel} --description "P2P file offer: ${fileName} (${fileSize} bytes)" --payload '${payload}'`)
    const taskId = task.id || task

    this.transfers.get(transferId).taskId = taskId
    this.transfers.get(transferId).status = 'waiting_accept'
    this.emit('offer_sent', { transferId, taskId, fileName, recipientId })

    // Poll for acceptance (receiver claims the task)
    const accepted = await this._waitForAccept(taskId)
    if (!accepted) {
      this.transfers.get(transferId).status = 'rejected'
      return { transferId, taskId, success: false, reason: 'Not accepted' }
    }

    this.transfers.get(transferId).status = 'transferring'
    this.emit('transfer_start', { transferId, taskId })

    // Transfer via Hyperswarm
    const result = await sendFile({
      filePath,
      transferId,
      fileHash,
      fileSize,
      timeout: this.timeout,
      onProgress
    })

    if (result.success) {
      this.transfers.get(transferId).status = 'completed'
      this.transfers.get(transferId).completedAt = new Date().toISOString()
      this.emit('transfer_complete', { transferId, taskId })
    } else {
      this.transfers.get(transferId).status = 'failed'
      this.transfers.get(transferId).reason = result.reason
      this.emit('transfer_failed', { transferId, taskId, reason: result.reason })
    }

    return { transferId, taskId, success: result.success }
  }

  /**
   * Poll ATS for task to be claimed (accepted by receiver).
   */
  async _waitForAccept (taskId, maxWait = 60000) {
    const start = Date.now()
    while (Date.now() - start < maxWait) {
      try {
        const task = await this._atsAsync(`get ${taskId}`)
        if (task.status === 'in_progress') return true
        if (task.status === 'cancelled' || task.status === 'rejected' || task.status === 'failed') return false
      } catch { /* retry */ }
      await new Promise(r => setTimeout(r, 1000))
    }
    return false
  }

  /**
   * Accept and receive a file from a file_offer task.
   *
   * @param {Object} opts
   * @param {Object} opts.task - The ATS task object (file_offer)
   * @param {string} opts.savePath - Where to save the file
   * @param {Function} [opts.onProgress] - Progress callback
   * @returns {Promise<{ success: boolean, filePath: string, hash: string }>}
   */
  async receive (opts) {
    const { task, savePath, onProgress } = opts
    const payload = typeof task.payload === 'string' ? JSON.parse(task.payload) : task.payload
    const { transfer_id: transferId, file_hash: expectedHash, file_size: expectedSize, sender_id: senderId, file_name: fileName } = payload

    if (this.maxFileSize && expectedSize > this.maxFileSize) {
      this._ats(`reject ${task.id} --reason "File too large: ${expectedSize} bytes exceeds limit of ${this.maxFileSize}"`)
      return { success: false, reason: 'File too large' }
    }

    // Claim the task (signals acceptance to sender)
    this._ats(`claim ${task.id} --lease 120000`)

    this.transfers.set(transferId, {
      status: 'transferring',
      direction: 'receive',
      fileName,
      fileSize: expectedSize,
      fileHash: expectedHash,
      senderId,
      taskId: task.id,
      startedAt: new Date().toISOString()
    })

    this.emit('transfer_start', { transferId, taskId: task.id, senderId })

    const finalPath = savePath.endsWith(path.sep)
      ? path.join(savePath, fileName)
      : savePath

    // Receive via Hyperswarm
    const result = await receiveFile({
      transferId,
      savePath: finalPath,
      expectedHash,
      expectedSize,
      maxFileSize: this.maxFileSize || undefined,
      timeout: this.timeout,
      onProgress
    })

    if (result.success) {
      // Complete the ATS task with confirmation
      const outputs = JSON.stringify({
        transfer_id: transferId,
        received_hash: result.hash,
        verified: true,
        saved_to: result.filePath
      })
      this._ats(`complete ${task.id} --outputs '${outputs}'`)

      this.transfers.get(transferId).status = 'completed'
      this.transfers.get(transferId).completedAt = new Date().toISOString()
      this.emit('transfer_complete', { transferId, taskId: task.id, filePath: result.filePath })
    } else {
      this._ats(`fail ${task.id} --reason "${result.reason}"`)
      this.transfers.get(transferId).status = 'failed'
      this.transfers.get(transferId).reason = result.reason
      this.emit('transfer_failed', { transferId, taskId: task.id, reason: result.reason })
    }

    return result
  }

  /**
   * Watch a channel for incoming file offers.
   * Emits 'file' events for each offer.
   *
   * @param {Object} opts
   * @param {string} [opts.channel='p2p-transfer'] - Channel to watch
   * @param {string} [opts.saveDir] - Auto-save directory (if set, auto-accepts)
   */
  watch (opts = {}) {
    const { channel = 'p2p-transfer', saveDir } = opts

    if (this._watching) return

    this._watching = true
    this._pollChannel(channel, saveDir)
  }

  async _pollChannel (channel, saveDir) {
    const seen = new Set()

    while (this._watching) {
      try {
        const tasks = this._ats(`list --channel ${channel} --status pending`)
        const offers = Array.isArray(tasks) ? tasks.filter(t => {
          if (seen.has(t.id)) return false
          const p = typeof t.payload === 'string' ? JSON.parse(t.payload) : (t.payload || {})
          return p.recipient_id === this.actorId || !p.recipient_id
        }) : []

        for (const task of offers) {
          seen.add(task.id)
          const payload = typeof task.payload === 'string' ? JSON.parse(task.payload) : task.payload

          if (saveDir) {
            // Auto-accept
            const savePath = path.join(saveDir, payload.file_name)
            this.receive({ task, savePath }).catch(err => {
              this.emit('error', err)
            })
          } else {
            // Emit event for manual handling
            this.emit('file', {
              task,
              fileName: payload.file_name,
              fileSize: payload.file_size,
              fileHash: payload.file_hash,
              senderId: payload.sender_id,
              accept: async (savePath) => this.receive({ task, savePath }),
              reject: async (reason) => {
                this._ats(`reject ${task.id} --reason "${reason || 'Declined by receiver'}"`)
              }
            })
          }
        }
      } catch (err) {
        this.emit('error', err)
      }

      await new Promise(r => setTimeout(r, 2000))
    }
  }

  /**
   * Stop watching for file offers.
   */
  stopWatching () {
    this._watching = false
  }

  /**
   * Get transfer status.
   */
  getTransfer (transferId) {
    return this.transfers.get(transferId) || null
  }

  /**
   * Get all transfers.
   */
  getAllTransfers () {
    return Object.fromEntries(this.transfers)
  }

  /**
   * Shut down the node.
   */
  async destroy () {
    this.stopWatching()
    this.removeAllListeners()
  }
}

/**
 * Create a new P2PNode.
 */
async function createNode (opts) {
  return new P2PNode(opts)
}

module.exports = { P2PNode, createNode }
