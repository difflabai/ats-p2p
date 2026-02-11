'use strict'

const path = require('path')
const fs = require('fs')
const { createNode } = require('./node')

function usage () {
  console.log(`
ats-p2p — P2P file sharing for AI agents

Usage:
  ats-p2p send <file> --to <agent-id> [--channel <ch>] [--ats-url <url>] [--actor-id <id>]
  ats-p2p listen [--channel <ch>] [--save-dir <dir>] [--ats-url <url>] [--actor-id <id>]
  ats-p2p status <transfer-id> [--ats-url <url>] [--actor-id <id>]
  ats-p2p --help

Commands:
  send      Send a file to another agent
  listen    Watch for incoming file offers
  status    Check transfer status (from local cache)

Options:
  --to          Recipient agent ID
  --channel     ATS channel (default: p2p-transfer)
  --save-dir    Directory to save incoming files (auto-accepts all)
  --ats-url     ATS server URL (default: https://ats.difflab.ai)
  --actor-id    This node's actor ID (default: p2p-node)
  --max-size    Maximum file size in bytes (default: unlimited)
  --timeout     Transfer timeout in ms (default: 120000)
  --help        Show this help
`)
}

function parseArgs (argv) {
  const args = {}
  const positional = []

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg.startsWith('--')) {
      const key = arg.slice(2)
      const next = argv[i + 1]
      if (next && !next.startsWith('--')) {
        args[key] = next
        i++
      } else {
        args[key] = true
      }
    } else {
      positional.push(arg)
    }
  }

  return { args, positional }
}

async function main (argv) {
  const { args, positional } = parseArgs(argv.slice(2))
  const command = positional[0]

  if (args.help || !command) {
    usage()
    process.exit(0)
  }

  const nodeOpts = {
    atsUrl: args['ats-url'],
    actorId: args['actor-id'] || process.env.ATS_ACTOR_ID || 'p2p-node',
    maxFileSize: args['max-size'] ? parseInt(args['max-size']) : 0,
    timeout: args.timeout ? parseInt(args.timeout) : 120000
  }

  const node = await createNode(nodeOpts)

  if (command === 'send') {
    const filePath = positional[1]
    const recipientId = args.to
    const channel = args.channel || 'p2p-transfer'

    if (!filePath || !recipientId) {
      console.error('Error: send requires <file> and --to <agent-id>')
      process.exit(1)
    }

    const absPath = path.resolve(filePath)
    if (!fs.existsSync(absPath)) {
      console.error(`Error: File not found: ${absPath}`)
      process.exit(1)
    }

    console.log(`Sending ${path.basename(absPath)} to ${recipientId} on channel ${channel}...`)

    node.on('offer_sent', ({ taskId }) => console.log(`  File offer created (task #${taskId}), waiting for acceptance...`))
    node.on('transfer_start', () => console.log('  Transfer started (Hyperswarm)...'))
    node.on('transfer_complete', () => console.log('  Transfer complete!'))
    node.on('transfer_failed', ({ reason }) => console.error(`  Transfer failed: ${reason}`))

    try {
      const result = await node.send({
        filePath: absPath,
        recipientId,
        channel,
        onProgress: ({ bytesSent, totalBytes }) => {
          const pct = Math.round((bytesSent / totalBytes) * 100)
          process.stdout.write(`\r  Progress: ${pct}% (${bytesSent}/${totalBytes} bytes)`)
        }
      })

      if (result.success) {
        console.log(`\nDone. Transfer ID: ${result.transferId}`)
      } else {
        console.error(`\nFailed: ${result.reason || 'Unknown error'}`)
        process.exit(1)
      }
    } catch (err) {
      console.error(`\nError: ${err.message}`)
      process.exit(1)
    }

    await node.destroy()
  } else if (command === 'listen') {
    const channel = args.channel || 'p2p-transfer'
    const saveDir = args['save-dir']

    if (saveDir) {
      fs.mkdirSync(saveDir, { recursive: true })
    }

    console.log(`Listening for file offers on channel "${channel}"${saveDir ? ` (auto-saving to ${saveDir})` : ''}...`)
    console.log('Press Ctrl+C to stop.\n')

    node.on('file', async ({ fileName, fileSize, senderId, accept, reject }) => {
      console.log(`Incoming file: ${fileName} (${fileSize} bytes) from ${senderId}`)
      // In non-auto mode, accept everything to the default save dir
      const dir = saveDir || './incoming'
      fs.mkdirSync(dir, { recursive: true })
      const savePath = path.join(dir, fileName)
      console.log(`  Accepting -> ${savePath}`)
      const result = await accept(savePath)
      if (result.success) {
        console.log(`  Received and verified: ${result.filePath}`)
      } else {
        console.log(`  Failed: ${result.reason}`)
      }
    })

    node.on('transfer_start', ({ taskId, senderId }) => {
      console.log(`  Transfer started (task #${taskId}) from ${senderId}`)
    })

    node.on('transfer_complete', ({ filePath }) => {
      console.log(`  Transfer complete: ${filePath}`)
    })

    node.on('error', err => console.error(`Error: ${err.message}`))

    node.watch({ channel, saveDir })

    process.on('SIGINT', async () => {
      console.log('\nShutting down...')
      await node.destroy()
      process.exit(0)
    })
  } else if (command === 'status') {
    const transferId = positional[1]
    if (!transferId) {
      console.error('Error: status requires <transfer-id>')
      process.exit(1)
    }
    const transfer = node.getTransfer(transferId)
    if (transfer) {
      console.log(JSON.stringify(transfer, null, 2))
    } else {
      console.log(`No local transfer found with ID: ${transferId}`)
      console.log('Note: Transfer status is only available within the same process session.')
    }
    await node.destroy()
  } else {
    console.error(`Unknown command: ${command}`)
    usage()
    process.exit(1)
  }
}

module.exports = { main }
