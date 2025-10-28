# Simple LSM DB - LSM Tree Key-Value Database

**A learning project** to understand LSM tree internals and client-server architecture. Not intended for production use.

## Architecture Overview

**Client-Server Model**: Separate server process manages the LSM tree database, clients connect via TCP.

### Server Components

- **Memtable**: In-memory sorted structure (BTreeMap) for recent writes
- **Write-Ahead Log (WAL)**: Append-only log for crash recovery
- **SSTables**: Immutable sorted files on disk
- **Background thread**: Handles memtable flushing and compaction
- **Manifest**: Tracks which SSTables exist and their metadata
- **TCP listener**: Accepts client connections and processes requests

### Client Components

- Simple client library that connects to server
- Sends commands (GET, PUT, DELETE) over TCP
- Completely unaware of LSM internals or background operations

## Project Structure (Cargo Workspace)

```
olive_db/
├── Cargo.toml                    # Workspace definition
├── protocol/
│   ├── Cargo.toml
│   └── src/
│       └── lib.rs                # Shared Command/Response types
├── server/
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs               # Server binary entry point
│       ├── db.rs                 # Core DB struct with LSM implementation
│       ├── memtable.rs           # In-memory sorted table
│       ├── wal.rs                # Write-ahead log
│       ├── sstable.rs            # SSTable format (write/read)
│       ├── compaction.rs         # Background compaction logic
│       ├── manifest.rs           # Tracks SSTable metadata
│       └── handler.rs            # TCP connection handler
└── client/
    ├── Cargo.toml
    └── src/
        ├── lib.rs                # Client library (for embedding in other Rust apps)
        └── bin/
            └── olive-cli.rs      # CLI client binary
```

**Three separate crates**:

- `protocol`: Shared types used by both server and client
- `server`: Database server binary
- `client`: Client library + CLI tool

## Protocol Design

Simple text-based protocol over TCP (easy to debug):

**Commands** (client → server):

- `GET <key>\n`
- `PUT <key> <value_len>\n<value_bytes>`
- `DELETE <key>\n`

**Responses** (server → client):

- `OK <value_len>\n<value_bytes>` (for GET)
- `OK\n` (for PUT/DELETE)
- `NOTFOUND\n` (for GET when key doesn't exist)
- `ERROR <message>\n`

## Public Client API

```rust
pub struct Client { /* connection details hidden */ }

impl Client {
    pub fn connect(addr: &str) -> Result<Self, Error>
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error>
}
```

## Server Architecture

The server runs:

1. **Main thread**: TCP listener accepting connections
2. **Worker threads**: Handle individual client connections (one thread per connection for simplicity)
3. **Background thread**: Memtable flushing and compaction

All threads share access to the `DB` struct via `Arc<Mutex<DB>>`.

## Background Operations (Hidden from Clients)

Background thread monitors and performs:

1. Memtable size checks
2. Flush to SSTable when threshold reached
3. Periodic compaction of SSTables

Clients just send commands and get responses - they never see or wait for background operations.

## Data Flow

### Write Path (`PUT`)

1. Client sends PUT command
2. Server appends to WAL
3. Server inserts into memtable
4. Server responds OK
5. Background thread flushes if needed (asynchronously)

### Read Path (`GET`)

1. Client sends GET command
2. Server checks memtable
3. If not found, searches SSTables (newest to oldest)
4. Server responds with value or NOTFOUND

### Delete Path (`DELETE`)

Writes a tombstone marker (special value indicating deletion)

## Dependencies

- `serde`/`serde_json` for serialization (manifest)
- Standard library for TCP (`std::net`)
- No async runtime needed (synchronous model)

## Implementation Order

1. Define protocol types and serialization
2. Implement core LSM components (memtable, WAL, SSTable)
3. Build basic DB operations (no networking yet)
4. Add background thread for flushing
5. Implement manifest tracking
6. Add TCP server and connection handling
7. Build client library
8. Add compaction logic

## Implementation Checklist

- [ ] Set up Cargo workspace with three crates
- [ ] Define protocol types (Command, Response) and parsing/serialization
- [ ] Implement memtable module with BTreeMap-based sorted storage
- [ ] Implement write-ahead log for crash recovery
- [ ] Implement SSTable format and writing logic
- [ ] Implement SSTable reading and key lookup
- [ ] Build core DB struct with get/put/delete operations (no networking)
- [ ] Add background thread for memtable flushing with channel communication
- [ ] Implement manifest to track active SSTables
- [ ] Implement TCP server with connection handler
- [ ] Build client library for connecting and sending commands
- [ ] Add basic compaction logic to merge SSTables
- [ ] Create CLI client binary

## Running the Database

```bash
# Start the server
cargo run --bin server

# In another terminal, use the CLI client
cargo run --bin olive-cli -- put mykey myvalue
cargo run --bin olive-cli -- get mykey
cargo run --bin olive-cli -- delete mykey
```

## Learning Resources

Key concepts to understand while implementing:

- **LSM Trees**: Log-Structured Merge Trees optimize write performance
- **Write Amplification**: Data is written multiple times during compaction
- **Leveled Compaction**: Strategy for organizing SSTables into levels
- **Bloom Filters**: (future enhancement) Quickly check if key might exist in SSTable
- **Tombstones**: Deletion markers that get compacted away later
