# ats-p2p

P2P file sharing for AI agents. ATS handles discovery and signaling. Hyperswarm handles encrypted byte transfer with NAT traversal. No file data touches the task system.

## Architecture

```
Agent A                    ATS                     Agent B
  |                         |                         |
  |-- file_offer task ----->|                         |
  |                         |---- notify offer ------>|
  |                         |<--- claim (accept) -----|
  |<-- task in_progress ----|                         |
  |                         |                         |
  |========= Hyperswarm (Noise encrypted P2P) =======|
  |                         |                         |
  |                         |<--- complete + hash ----|
```

- **ATS** = signaling only (task metadata: filename, size, SHA-256 hash)
- **Hyperswarm** = encrypted P2P data transfer (Noise protocol, NAT traversal)
- **SHA-256** verification on every transfer
- Peer allowlists for access control

## Install

```bash
npm install ats-p2p
# or globally for the CLI
npm install -g ats-p2p
```

Requires [ats-cli](https://github.com/difflabai/ats-cli) for ATS operations:

```bash
npm install -g @difflabai/ats-cli
```

## API

```js
const { createNode } = require('ats-p2p')

// Create a node
const node = await createNode({
  atsUrl: 'https://ats.difflab.ai',  // optional, default
  actorId: 'my-agent',
  allowedPeers: ['other-agent'],       // optional peer allowlist
  maxFileSize: 100 * 1024 * 1024,      // optional, 100MB limit
  timeout: 120000                       // optional, 2 min default
})

// --- Sender ---
const result = await node.send({
  filePath: '/path/to/file.tar.gz',
  recipientId: 'other-agent',
  channel: 'p2p-transfer',             // optional, default
  onProgress: ({ bytesSent, totalBytes }) => {
    console.log(`${Math.round(bytesSent/totalBytes*100)}%`)
  }
})
// result: { transferId, taskId, success: true }

// --- Receiver (event-driven) ---
node.watch({ channel: 'p2p-transfer' })

node.on('file', async ({ fileName, fileSize, senderId, accept, reject }) => {
  console.log(`Incoming: ${fileName} from ${senderId}`)
  const result = await accept('/path/to/save/file.tar.gz')
  // result: { success: true, filePath, hash }
})

// --- Receiver (auto-accept to directory) ---
node.watch({ channel: 'p2p-transfer', saveDir: './incoming' })

// --- Cleanup ---
await node.destroy()
```

## Events

| Event | Payload |
|-------|---------|
| `offer_sent` | `{ transferId, taskId, fileName, recipientId }` |
| `transfer_start` | `{ transferId, taskId }` |
| `transfer_complete` | `{ transferId, taskId, filePath? }` |
| `transfer_failed` | `{ transferId, taskId, reason }` |
| `file` | `{ task, fileName, fileSize, fileHash, senderId, accept, reject }` |
| `error` | `Error` |

## CLI

```bash
# Send a file
ats-p2p send ./data.csv --to agent-b --channel p2p-transfer

# Listen for incoming files (auto-accept)
ats-p2p listen --channel p2p-transfer --save-dir ./incoming

# Listen with custom settings
ats-p2p listen --channel p2p-transfer --actor-id my-agent --ats-url https://ats.difflab.ai

# Check transfer status
ats-p2p status <transfer-id>
```

### CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--to` | Recipient agent ID | required for send |
| `--channel` | ATS channel | `p2p-transfer` |
| `--save-dir` | Auto-save directory | `./incoming` |
| `--ats-url` | ATS server URL | `https://ats.difflab.ai` |
| `--actor-id` | This node's actor ID | `p2p-node` |
| `--max-size` | Max file size (bytes) | unlimited |
| `--timeout` | Transfer timeout (ms) | `120000` |

## ATS Task Flow

1. Sender creates `file_offer` task with metadata (filename, size, SHA-256 hash, sender_id, transfer_id)
2. Receiver sees the task, claims it (signaling acceptance)
3. Both derive a Hyperswarm topic from the transfer ID
4. Bytes transfer P2P over Noise-encrypted Hyperswarm connection
5. Receiver verifies SHA-256 hash
6. Receiver completes the ATS task with confirmation metadata

## Security

- **Noise protocol encryption** on all Hyperswarm connections (built-in)
- **Peer allowlists** — restrict connections to known agent IDs
- **SHA-256 verification** on every transfer
- **Configurable max file size** to prevent abuse
- **Full audit trail** — every transfer logged as ATS task with metadata
- **No file data in ATS** — only metadata (name, size, hash) goes through the task system

## Transfer Protocol

The wire protocol over Hyperswarm uses length-prefixed messages:

```
[1 byte type][4 bytes length (BE)][payload]
```

Message types:
- `0x01 META` — File metadata JSON `{ name, size, hash }`
- `0x02 DATA` — File chunk (64KB)
- `0x03 DONE` — Transfer complete signal
- `0x04 ACK` — Receiver verified hash, all good
- `0x05 NACK` — Receiver rejects (hash mismatch, too large, etc.)
- `0x06 ERROR` — Error message

## Test

```bash
# Run the E2E test (local transfer between two Hyperswarm peers)
npm test
```

The test creates a temporary file, transfers it between two local Hyperswarm peers, and verifies the SHA-256 hash matches.

## Dependencies

- [hyperswarm](https://github.com/holepunchto/hyperswarm) — P2P networking with NAT traversal and Noise encryption
- [b4a](https://github.com/holepunchto/b4a) — Buffer utilities
- [@difflabai/ats-cli](https://github.com/difflabai/ats-cli) — ATS task operations (peer dependency, used via child_process)

## License

MIT
