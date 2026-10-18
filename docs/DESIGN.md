# finenet — General-Purpose Network Transport Layer

A standalone, reusable C++ library for structured message passing over UDP
(with TCP fallback) or in-process shared memory. Designed for applications
that need multiple logical channels with independent reliability, ordering,
priority, and flow control — such as multiplayer games, distributed
simulations, or real-time collaborative tools.

finenet is **content-agnostic**. It provides the transport machinery;
applications define their own message types, channel configurations, and
routing policies. A single-player game uses the exact same `Connection` API
as a networked multiplayer game — the transport is simply short-circuited
in-process.

---

## 1. Design Principles

1. **Application-defined schema.** The library has no built-in message types.
   Applications register their own type IDs and assign them to channels.
2. **Builder/reader serialization.** Type-safe, chainable message construction
   with a mirrored reader for decoding. No code generation or IDL required.
3. **Channels as policy.** Each logical channel carries independent settings
   for reliability, ordering, priority, compression, and fragmentation.
   Applications create as many or as few channels as they need.
4. **Priority scheduling.** A muxer packs outbound datagrams by priority,
   ensuring latency-sensitive messages are never blocked behind bulk transfers.
5. **UDP-first, TCP-fallback.** UDP transport with selective reliability
   avoids head-of-line blocking. TCP is available for environments that
   block UDP.
6. **Local transport for in-process communication.** When client and server
   run in the same process (single-player, or the host player in a
   listen-server), messages are passed via concurrent queues — no sockets,
   no serialization overhead. The application uses the identical API
   regardless of transport.
7. **Transparent compression.** Per-message or per-batch, with large payloads
   offloaded to a thread pool so the send path never stalls.
8. **Automatic fragmentation.** Messages exceeding the MTU are split and
   reassembled transparently.
9. **Minimal dependencies.** Platform sockets + zstd. No game engine, no
   framework, no allocator opinions.

---

## 2. Architecture Overview

```
Application                finenet                        Transport Backend
─────────                  ───────                        ─────────────────

                     ┌─────────────────┐
  send(ch, msg) ───►│  Muxer          │     ┌──────────────────────────────┐
                     │  ├ Compress     │────►│  UdpTransport  ──► network  │
                     │  ├ Fragment     │     │  TcpTransport  ──► network  │
                     │  └ Pack into    │     │  LocalTransport ─► queue    │
                     │    datagrams    │     └──────────────────────────────┘
                     │    by priority  │
                     └─────────────────┘

                     ┌─────────────────┐     ┌──────────────────────────────┐
  onMessage() ◄─────│  Demuxer        │◄────│  UdpTransport  ◄── network  │
                     │  ├ Reassemble   │     │  TcpTransport  ◄── network  │
                     │  ├ Decompress   │     │  LocalTransport ◄─ queue    │
                     │  └ Route to     │     └──────────────────────────────┘
                     │    channel      │
                     └─────────────────┘
```

### Component Summary

| Component | Responsibility |
|-----------|----------------|
| **MessageBuilder** | Construct messages field-by-field (builder pattern) |
| **MessageReader** | Decode messages field-by-field (mirrors builder) |
| **Message** | Immutable built product: type ID + binary payload |
| **Channel** | Logical stream with reliability/ordering/priority policy |
| **Muxer** | Collects outbound messages, schedules by priority, packs datagrams |
| **Demuxer** | Unpacks inbound datagrams, reassembles fragments, routes to channels |
| **AckTracker** | Selective acknowledgment and retransmission for reliable channels |
| **CongestionController** | Adaptive send-rate control based on RTT and loss |
| **OrderedReceiver** | Per-channel reorder buffer for ordered delivery |
| **FragmentAssembler** | Reassembles multi-fragment messages |
| **CompressionPool** | Sync/async zstd compression and decompression |
| **Connection** | Session lifecycle: handshake, keepalive, disconnect, stats |
| **ConnectionListener** | Server-side: accept incoming connections |
| **LocalTransport** | In-process message passing via MPSC queues |
| **Queue\<T\>** | Thread-safe FIFO with alarm and WakeSignal support |
| **KeyedQueue\<K,D\>** | Deduplicating queue with key-based merge |
| **WakeSignal** | Multi-queue blocking coordination primitive |

---

## 3. Message Types — Application-Defined

finenet does not define any message types. Applications register their own.

### 3.1 Type ID Encoding

Type IDs are variable-length encoded on the wire using prefix coding, so
frequently-used types consume fewer bytes:

```
Prefix byte    Wire bytes    ID range         Typical use
──────────     ──────────    ────────         ───────────
0x00–0x7F      1 byte        0 – 127          High-frequency messages
0x80–0xBF      2 bytes       128 – 16,511     Medium-frequency messages
0xC0–0xDF      3 bytes       16,512 – 2M+     Rare / setup messages
0xE0–0xFF      Reserved      —                Future extension
```

The application assigns numeric IDs to its message types. Lower IDs are
cheaper on the wire, so assign the smallest IDs to the most frequent messages.

### 3.2 Type Registration

```cpp
// Application code — no finenet header knows about these types.
enum class MyMessageType : uint32_t {
    Heartbeat  = 0x01,   // 1-byte on wire
    Position   = 0x02,
    ChatMsg    = 0x80'01, // 2-byte on wire
    AssetData  = 0xC0'0001, // 3-byte on wire
};

// At connection setup, tell finenet which channel each type belongs to:
finenet::ChannelRouter router;
router.assign(0x01, channelId_control);
router.assign(0x02, channelId_entities);
router.assign(0x8001, channelId_chat);
router.assign(0xC00001, channelId_assets);
```

The router is used by the demuxer to deliver inbound messages to the correct
channel's handler. On the send side, the application explicitly names the
channel when calling `connection.send(channel, msg)`.

### 3.3 Unregistered Types

If the demuxer receives a message with an unregistered type ID, it invokes a
configurable fallback handler (default: log a warning and drop the message).
This supports forward compatibility when one peer is newer than the other.

---

## 4. Message Serialization

### 4.1 MessageBuilder

Chainable, append-only. Writes fields sequentially into a byte buffer.
`build()` finalizes and returns an immutable `Message`.

```cpp
namespace finenet {

class MessageBuilder {
public:
    explicit MessageBuilder(uint32_t typeId);

    // ── Primitives (network byte order) ──
    MessageBuilder& writeU8(uint8_t v);
    MessageBuilder& writeU16(uint16_t v);
    MessageBuilder& writeU32(uint32_t v);
    MessageBuilder& writeU64(uint64_t v);
    MessageBuilder& writeI8(int8_t v);
    MessageBuilder& writeI16(int16_t v);
    MessageBuilder& writeI32(int32_t v);
    MessageBuilder& writeI64(int64_t v);
    MessageBuilder& writeF32(float v);
    MessageBuilder& writeF64(double v);
    MessageBuilder& writeBool(bool v);

    // ── Length-prefixed compound fields ──
    MessageBuilder& writeString(std::string_view s);
    MessageBuilder& writeBytes(std::span<const uint8_t> data);

    // ── Variable-length integer (LEB128) ──
    MessageBuilder& writeVarInt(uint64_t v);
    MessageBuilder& writeVarSint(int64_t v);    // ZigZag + LEB128

    // ── Structured helpers ──
    MessageBuilder& writeArrayHeader(uint32_t count);
    MessageBuilder& writeMapHeader(uint32_t count);

    // ── Embedded opaque blob (e.g. CBOR, protobuf, app-specific) ──
    // Identical to writeBytes() on the wire (length-prefixed). Exists as
    // a semantic alias: "this field is an opaque sub-format, not raw data."
    MessageBuilder& writeBlob(std::span<const uint8_t> data);

    // ── Finalize ──
    Message build();

    // ── Query ──
    size_t currentSize() const;
};

}  // namespace finenet
```

**Not included in the core library** but available as optional extension
headers (see Section 15):
- `writeVec3`, `writeQuantizedAngle`, `writeBlockPos`, etc. — these are
  domain-specific conveniences that applications can add via extension traits
  or free functions.

### 4.2 MessageReader

Mirrors the builder. Reads fields in the same order they were written.
Advances an internal cursor. Becomes invalid (returns errors) on any read
failure, and all subsequent reads also fail.

```cpp
class MessageReader {
public:
    explicit MessageReader(const Message& msg);
    explicit MessageReader(std::span<const uint8_t> data);

    // ── Type ──
    uint32_t typeId() const;

    // ── Primitives ──
    uint8_t  readU8();
    uint16_t readU16();
    uint32_t readU32();
    uint64_t readU64();
    int8_t   readI8();
    int16_t  readI16();
    int32_t  readI32();
    int64_t  readI64();
    float    readF32();
    double   readF64();
    bool     readBool();

    // ── Compound ──
    std::string_view readString();              // Zero-copy into buffer
    std::span<const uint8_t> readBytes();       // Zero-copy into buffer

    // ── Variable-length ──
    uint64_t readVarInt();
    int64_t  readVarSint();

    // ── Structured ──
    uint32_t readArrayHeader();
    uint32_t readMapHeader();

    // ── Blob ──
    std::span<const uint8_t> readBlob();

    // ── Cursor ──
    size_t remaining() const;
    bool   hasMore() const;
    void   skip(size_t bytes);

    // ── Error state ──
    bool valid() const;                // false after any failed read
    std::string_view error() const;    // human-readable error description
};
```

### 4.3 Message — The Built Product

`Message` is a pure application-level object: a type ID and a payload.
It carries **no transport metadata**. The builder doesn't know which channel
or priority the message will be sent on — that's specified at `send()` time.

```cpp
class Message {
public:
    uint32_t typeId() const;
    std::span<const uint8_t> payload() const;   // Encoded fields
    size_t payloadSize() const;                 // Payload bytes only

private:
    friend class MessageBuilder;
    // ...
};
```

Transport metadata (channel, priority, sequence number, flags) lives on
`MessageFrame`, which is an internal type used by the muxer/demuxer. The
application never constructs or inspects `MessageFrame` directly — it only
appears in the wire pipeline and in advanced diagnostics.

```cpp
// Internal to finenet — not part of the application-facing API.
struct MessageFrame {
    Message   message;
    ChannelId channel;
    Priority  priority;
    uint16_t  seqNum;          // Per-channel sequence number
    uint8_t   flags;           // Reliable, Ordered, Fragment, Compressed, etc.
};
```

This separation keeps `Message` lightweight and reusable. The same `Message`
object can be sent on different channels with different priorities — it's
just data. The transport layer wraps it in a `MessageFrame` when it enters
the wire pipeline.

### 4.4 Byte Order

All multi-byte primitives are written in **little-endian** byte order on the
wire. Little-endian is chosen because it is the native order on x86, x86-64,
ARM (in its default LE mode), and Apple Silicon — the vast majority of modern
client hardware. This avoids byte-swap overhead on the fast path. Big-endian
platforms (rare in practice) pay the swap cost.

### 4.5 String and Bytes Encoding

Length-prefixed with a `VarInt` length prefix:
```
[VarInt: length] [length bytes of data]
```
This allows zero-copy reads: `readString()` and `readBytes()` return views
directly into the message buffer without allocating.

### 4.6 Array and Map Headers

`writeArrayHeader(n)` / `writeMapHeader(n)` write a VarInt count. The caller
then writes `n` elements (or `n` key-value pairs for maps) using the normal
write methods. The reader calls `readArrayHeader()` / `readMapHeader()` to
get the count, then reads that many elements.

This is structurally similar to CBOR/MessagePack array/map headers and allows
variable-length collections without requiring the builder to know the total
size upfront.

### 4.7 No Type Tags in the Payload

**Design decision:** finenet payloads carry **no per-field type information**.
Each `writeU32()` writes exactly 4 bytes — there is no tag byte identifying
it as "a uint32". The producer and consumer agree on field layout via the
message type ID: if you receive message type 0x02, you know the fields are
`[F32, F32, F32]` because that's what your application defined.

This is a deliberate speed-and-size optimization. Type tags would add 1+ byte
per field for information the receiver already knows. For high-frequency
messages (e.g., position updates sent 20× per second), every byte matters.

**Consequence:** if the sender and receiver disagree on the schema for a
message type, the receiver silently reads garbage. There is no runtime type
checking. The **debug mode** (Section 7.3) can optionally force local
connections through serialization to catch such mismatches during development.

### 4.8 Embedded Sub-Formats (CBOR, Protobuf, etc.)

finenet does not parse or understand any embedded sub-format. If an
application serializes part of its data as CBOR (e.g., a key-value
`DataContainer`), protobuf, or any other format, that data is opaque bytes
from finenet's perspective.

Use `writeBytes()` / `readBytes()` to embed such data as a length-prefixed
blob. The `writeBlob()` / `readBlob()` aliases exist as a semantic hint
("this field is an opaque sub-format") but are wire-identical to
`writeBytes()` / `readBytes()`.

```cpp
// Example: embedding a CBOR-serialized DataContainer in a finenet message.
auto cborBytes = myDataContainer.toCBOR();  // Application's own serialization
auto msg = MessageBuilder(MSG_BLOCK_UPDATE)
    .writeI32(x).writeI32(y).writeI32(z)
    .writeBlob(cborBytes)   // finenet just sees: [VarInt length][bytes]
    .build();
```

---

## 5. Channels

A channel is a logical stream multiplexed over a single connection. Each
channel has independent policies for reliability, ordering, priority,
compression, and fragmentation.

### 5.1 Channel Configuration

```cpp
using ChannelId = uint8_t;   // 0–255 channels per connection

struct ChannelConfig {
    ChannelId   id;
    std::string name;                     // For debugging / logging

    // ── Delivery guarantees ──
    Reliability reliability;
    bool        ordered;                  // Preserve send order within channel

    // ── Priority ──
    Priority    defaultPriority;
    bool        allowPriorityOverride;    // Per-message priority override

    // ── Compression ──
    CompressionPolicy compression;
    uint32_t    compressionThreshold;     // Bytes; compress if larger

    // ── Fragmentation ──
    bool        allowFragmentation;       // Messages may exceed MTU
    uint32_t    maxMessageSize;           // Hard limit (0 = unlimited)

    // ── Flow control ──
    uint32_t    maxUnackedBytes;          // Backpressure: pause sending
    uint32_t    maxQueuedMessages;        // For unreliable: drop oldest
};
```

### 5.2 Reliability Modes

```cpp
enum class Reliability : uint8_t {
    Unreliable,       // Fire-and-forget. No ack, no retransmit.
    Reliable,         // Retransmit until acked. No ordering guarantee.
    ReliableOrdered,  // Retransmit + deliver in send order.
};
```

| Mode | Acked? | Retransmitted? | Ordered? | Use case |
|------|--------|----------------|----------|----------|
| Unreliable | No | No | No | Ephemeral data (VoIP, effects) |
| Reliable | Yes | Yes | No | Important but order-insensitive (asset chunks) |
| ReliableOrdered | Yes | Yes | Yes | State-changing commands (actions, RPCs) |

### 5.3 Priority Levels

```cpp
enum class Priority : uint8_t {
    Critical = 0,   // Never delayed. Reserved bandwidth fraction.
    High     = 1,   // Minimal delay.
    Normal   = 2,   // Standard scheduling.
    Low      = 3,   // Background. Yields to higher priorities.
    Bulk     = 4,   // Fills remaining bandwidth only.
};
```

### 5.4 Compression Policy

```cpp
enum class CompressionPolicy : uint8_t {
    Never,           // Skip compression (already compressed, or too small)
    Always,          // Always compress
    SizeThreshold,   // Compress only if payload > compressionThreshold
};
```

### 5.5 Application-Defined Channel Sets

The library does not predefine any channels. The application provides its
channel configs at connection setup:

```cpp
finenet::ConnectionConfig config;
config.channels = {
    { .id = 0, .name = "control",
      .reliability = Reliability::ReliableOrdered,
      .defaultPriority = Priority::Critical,
      .compression = CompressionPolicy::Never },

    { .id = 1, .name = "realtime",
      .reliability = Reliability::Unreliable,
      .defaultPriority = Priority::High,
      .compression = CompressionPolicy::Never,
      .maxQueuedMessages = 128 },

    { .id = 2, .name = "bulk",
      .reliability = Reliability::Reliable,
      .defaultPriority = Priority::Bulk,
      .compression = CompressionPolicy::Always,
      .allowFragmentation = true },
};
```

---

## 6. Wire Format

### 6.1 UDP Datagram Structure

Each UDP datagram has a fixed header followed by one or more packed messages:

```
┌─────────────────────────────────────────────────────────┐
│                   Datagram Header (20 bytes)             │
├─────────────────────────────────────────────────────────┤
│ Connection ID        4 bytes   Session identifier        │
│ Datagram Sequence    4 bytes   Monotonic per-connection   │
│ Ack Bitfield         4 bytes   Selective ack (32 recent)  │
│ Last Ack Sequence    4 bytes   Highest received seq       │
│ Timestamp            4 bytes   Sender clock (ms), for RTT │
├─────────────────────────────────────────────────────────┤
│                   Message Frames                         │
├─────────────────────────────────────────────────────────┤
│ Frame 1: [Chan(1) | Flags(1) | Seq(2) | Len(2) | Data] │
│ Frame 2: [Chan(1) | Flags(1) | Seq(2) | Len(2) | Data] │
│ ...                                                      │
└─────────────────────────────────────────────────────────┘
```

**Datagram header fields:**

| Field | Size | Description |
|-------|------|-------------|
| `connectionId` | 4 B | Identifies the session. Prevents misrouted stale packets. |
| `datagramSeq` | 4 B | Monotonic sequence for this connection's datagrams. |
| `ackBitfield` | 4 B | Bit N = received `lastAckSeq - N - 1`. Acks up to 32 recent datagrams. |
| `lastAckSeq` | 4 B | Highest datagram sequence number received from peer. |
| `timestamp` | 4 B | Sender's monotonic clock in milliseconds (see §6.5). Used for RTT estimation. |

**Message frame header:**

| Field | Size | Description |
|-------|------|-------------|
| `channel` | 1 B | Channel ID (0–255) |
| `flags` | 1 B | Bitfield (see below) |
| `seqNum` | 2 B | Per-channel sequence number |
| `length` | 2 B | Payload length in bytes |

**Flags bitfield:**

| Bit | Name | Meaning |
|-----|------|---------|
| 0 | Reliable | Sender expects an ack for this frame |
| 1 | Ordered | Frame has a meaningful sequence number for ordering |
| 2 | Fragment | Frame is part of a fragmented message |
| 3 | Compressed | Payload is zstd-compressed |
| 4 | LastFragment | Final fragment of a fragmented message |
| 5–7 | Reserved | Must be zero |

Multiple frames are packed into a single datagram up to the MTU. The default
safe UDP MTU is **1200 bytes** (accounts for IPv4/IPv6 headers, tunneling,
and common internet path MTUs). This is configurable.

### 6.2 TCP Framing (Fallback)

For environments that block UDP, TCP transport uses length-prefixed framing:

```
┌──────────────────────────────────────────┐
│ Total Length     4 bytes (big-endian)      │
│ Channel          1 byte                   │
│ Flags            1 byte                   │
│ Sequence Num     2 bytes                  │
│ Payload          (Total Length − 4) bytes  │
└──────────────────────────────────────────┘
```

TCP inherently provides reliability and ordering, so:
- The AckTracker is bypassed (no retransmission logic needed).
- `Unreliable` channels are still delivered, but with TCP's ordering
  guarantee — effectively upgraded to ReliableOrdered semantics.
- Head-of-line blocking across channels is unavoidable with a single TCP
  connection. The library can **dynamically** open secondary TCP connections
  when congestion is detected on the primary connection, to isolate
  priority tiers. This is done conservatively — too many TCP connections
  cause internet routing congestion and increase connection management
  overhead. The default is to start with one connection and add a second
  only if congestion metrics warrant it (configurable).

### 6.3 Transport Selection Strategy

```
Default: Try UDP
  ├── UDP handshake succeeds → use UDP for all traffic
  └── UDP handshake times out → fall back to TCP
        ├── Single TCP connection (default)
        └── Multiple TCP connections (optional, one per priority tier)
```

The application can also force a specific transport mode.

### 6.4 MTU Considerations

The default safe MTU of 1200 bytes is conservative but works across virtually
all internet paths. The library supports MTU discovery:

1. Start at 1200 bytes.
2. Optionally probe larger sizes using don't-fragment datagrams.
3. If a probe succeeds, increase the effective MTU (up to a configured max).
4. If loss increases at a larger MTU, fall back.

Applications can also set a fixed MTU if the network topology is known.

### 6.5 Timestamp Semantics and Rollover

The 4-byte `timestamp` field carries the sender's monotonic clock in
milliseconds, truncated to 32 bits. It is used **only** for RTT estimation
(the receiver echoes it back, and the sender computes the round-trip delta).

**Rollover:** A 32-bit millisecond counter wraps after ~49.7 days. Unlike
sequence numbers (which require modular comparison for ordering), timestamps
do not need explicit rollover handling:

- RTT is computed as `(now - echoed_timestamp) & 0xFFFFFFFF`, which
  naturally handles a single wrap using unsigned subtraction (modular
  arithmetic). As long as the RTT is less than ~24 days (half the
  range), the delta is correct.
- No connection is expected to have a 24-day RTT, so this is safe in
  practice.
- The timestamp is not used for ordering, sequencing, or any comparison
  where "before" vs. "after" matters across a wrap boundary.

If a connection is open for more than 49 days, the timestamp wraps silently
and RTT estimation continues working correctly.

---

## 7. Local Transport (In-Process)

When client and server live in the same process — single-player, or the
host player in a listen-server topology — there is no reason to serialize
messages onto a socket, compress them, or fragment them across an MTU
boundary. The local transport short-circuits the entire wire pipeline while
preserving the exact same `Connection` API.

### 7.1 Motivation

A game should not need two code paths: one for single-player and one for
multiplayer. By making local transport a first-class backend, the
application always uses `connection.send()` and `onMessage()`. The only
thing that changes is how the `Connection` was created.

### 7.2 How It Works

A local connection is a matched pair of `Connection` objects that share a
set of MPSC queues (Section 14.1) — one per channel, per direction. When one side calls `send()`, the `Message` object
is moved directly onto the shared queue. When the other side calls
`update()`, it drains the queues and invokes handlers.

```
┌──────────────────────────────────────────────────────────────┐
│                     Same Process                              │
│                                                               │
│   Client Connection          Shared Queues      Server Connection
│   ┌──────────────┐     ┌─────────────────┐     ┌──────────────┐
│   │ send(ch, msg)│────►│  C→S per-channel│────►│ update()     │
│   │              │     │  MPSC queues     │     │  → handlers  │
│   │ update()     │◄────│  S→C per-channel│◄────│ send(ch, msg)│
│   │  → handlers  │     │  MPSC queues     │     │              │
│   └──────────────┘     └─────────────────┘     └──────────────┘
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### 7.3 What Gets Bypassed (and Debug Mode)

When using local transport in **normal mode**, the following are skipped:

| Component | Bypassed? | Why |
|-----------|-----------|-----|
| Wire encode/decode | Yes | No network involved |
| Socket I/O | Yes | No network involved |
| Fragmentation/reassembly | Yes | No MTU constraint |
| Compression/decompression | Yes | No bandwidth constraint |
| AckTracker / retransmission | Yes | Delivery is guaranteed (in-memory) |
| Datagram header (connId, seq, ack) | Yes | No datagrams |
| Keepalive | Yes | Liveness is implicit (same process) |
| Handshake | Simplified | No nonce exchange needed; instant |

**Debug mode:** Local connections can be created with
`debugSerialization = true` in the config. In debug mode, the local
transport forces messages through the **full serialize → compress →
decompress → deserialize** path, even though the data never touches a
socket. This catches encoding mismatches, schema disagreements, and
compression bugs during single-player development — before the game goes
multiplayer and such bugs become hard to reproduce.

```cpp
ConnectionConfig cfg = makeChannelConfig();
cfg.debugSerialization = true;  // Force full pipeline on local transport.
auto [client, server] = Connection::createLocalPair(cfg);
// Now every send() serializes, compresses, decompresses, deserializes.
// Slower, but catches bugs that zero-copy local transport would hide.
```

Debug mode is off by default and adds overhead proportional to message
size. It is intended for development builds only.

What is **preserved** (both normal and debug mode):
- Channel routing and per-channel ordering semantics.
- Priority ordering within a single `update()` call (higher-priority
  messages are delivered to handlers first).
- The `Unreliable` + `maxQueuedMessages` drop policy (if the queue
  overflows, oldest messages are dropped, same as on the network path).
- Backpressure semantics (`ChannelFull` if the receiver isn't draining).
- All `ConnectionStats` counters (messages sent/received, per-channel).

### 7.4 Message Transfer Semantics

On network transports, `send()` eventually serializes the message to bytes.
The `Message` object on the receive side is a distinct deserialized copy.

On local transport, the `Message` object is **moved** across the queue.
This is zero-copy for the payload buffer — the `std::vector<uint8_t>` inside
the `Message` transfers ownership without copying or allocating.

The sender must not access the `Message` after calling `send()`. This is
the same contract as the network path (where the message is consumed by the
muxer), so application code that works with network transport automatically
works with local transport.

### 7.5 Creating Local Connections

```cpp
// Create a matched pair. Returns {client_side, server_side}.
auto [clientConn, serverConn] = Connection::createLocalPair(
    connectionConfig);
```

Both sides share the same `ConnectionConfig` (channel definitions). Both
immediately enter the `Connected` state — no handshake delay.

The server holds `serverConn` alongside any network connections it manages.
The client holds `clientConn`. From the application's perspective, these are
just `Connection` objects — they can be stored in the same container, passed
to the same handler registration code, etc.

### 7.6 Single-Player → Open-to-Network Transition

This is the key scenario: a game starts as single-player (local transport
only), then the player decides to open the world to the network (e.g.,
"invite friends" or "open to LAN").

```
Phase 1: Single-player
─────────────────────────────────────────────────────
  Game client ◄──local──► Game server
                           (both in same process)

Phase 2: Player opens to network
─────────────────────────────────────────────────────
  Game client ◄──local──► Game server ◄──UDP/TCP──► Remote client A
                                       ◄──UDP/TCP──► Remote client B
```

The server simply starts a `ConnectionListener`. The local connection for
the host player remains untouched — it continues using in-process queues
with zero overhead. New remote clients connect over the network and get
network-backed `Connection` objects.

The server code doesn't distinguish between local and remote connections:

```cpp
// Server holds all connections uniformly:
std::vector<std::unique_ptr<Connection>> connections_;

// The host player's local connection:
auto [clientConn, serverConn] = Connection::createLocalPair(config);
connections_.push_back(std::move(serverConn));

// Later, when opening to network:
listener_ = ConnectionListener::listen(port, listenerConfig);

// Accept loop treats all connections identically:
if (auto remoteConn = listener_->accept()) {
    connections_.push_back(std::move(remoteConn));
}

// Update loop — doesn't know or care which are local vs. remote:
for (auto& conn : connections_) {
    conn->update();
}
```

### 7.7 Latency Characteristics

Local transport delivers messages as soon as the receiving side processes
its queue. In the typical multi-threaded setup where the consumer blocks on
a `WakeSignal` or `Queue::waitForWork()`, the consumer wakes **immediately**
when a message is pushed — effective one-way latency is limited only by
thread scheduling overhead (typically microseconds). For a single-threaded
setup where client and server `update()` in the same loop iteration,
latency is effectively zero within a tick.

`ConnectionStats::rttMs` reports 0.0 for local connections.

### 7.8 Threading Considerations

Local transport uses the same `Queue<T>` (MPSC) as the rest of finenet.
Multiple threads may call `send()` on the same local connection — the
queue handles producer synchronization internally.

- **Multi-threaded** (typical): Client thread(s) produce on outbound
  queues, server thread consumes inbound queues (and vice versa).
- **Single-threaded**: Client and server update in the same loop. The
  queues work fine with a single thread (MPSC is a superset of
  single-threaded access), and messages sent in one `update()` are
  available in the next.

### 7.9 Transport Querying

Applications can query the transport type of a connection:

```cpp
enum class TransportType : uint8_t {
    Local,    // In-process, same-process peer
    Udp,      // UDP socket
    Tcp,      // TCP socket
};

// On Connection:
TransportType transportType() const;
bool isLocal() const;  // Convenience: transportType() == TransportType::Local
```

This is intentionally available for cases where the application wants to
know (e.g., displaying "(Local)" vs. ping in a player list), but
application logic should never branch on it for correctness — the API
contract is identical across transports.

---

## 8. Fragmentation and Reassembly (Network Transports Only)

Messages larger than the effective MTU payload size (MTU minus headers,
typically ~1100 bytes) are automatically fragmented by the muxer and
reassembled by the demuxer.

### 8.1 Fragment Header

When the `Fragment` flag is set in a message frame, the payload is prefixed
with a fragment header:

```
┌──────────────────────────────────────────────┐
│ Fragment Group ID    2 bytes                  │
│ Fragment Index       2 bytes  (0-based)       │
│ Fragment Count       2 bytes  (total pieces)  │
│ Total Size           4 bytes  (first frag only)│
│ Fragment Payload     ...                       │
└──────────────────────────────────────────────┘
```

- `Fragment Group ID` — unique within the channel, wrapping 16-bit counter.
- `Fragment Index` / `Fragment Count` — 16-bit each. At ~1100 bytes per
  fragment, this allows messages up to ~72 MB (65535 × 1100). With `uint8_t`
  counts, the limit would be ~280 KB — too tight for bulk asset transfer.
  The extra 2 bytes of header overhead per fragment is negligible.
- `Fragment Count` — present in the first fragment; receivers learn the total
  expected pieces.
- `Total Size` — uncompressed message size, present in the first fragment
  only. Used to pre-allocate the reassembly buffer.

### 8.2 Ordering of Operations

Compression and fragmentation are ordered carefully:

**Send side:** Message → Compress → Fragment → Frame → Wire
**Receive side:** Wire → Deframe → Reassemble → Decompress → Message

Compressing the full message before fragmenting produces better compression
ratios than compressing individual fragments. Reassembly must complete before
decompression can begin.

### 8.3 Fragment Reliability

Each fragment is individually tracked for reliability. If any fragment of a
reliable message is lost, only that fragment is retransmitted — not the
entire message.

### 8.4 Reassembly Timeout

Incomplete fragment groups are garbage-collected after a configurable timeout
(default: 10 seconds). This prevents memory leaks from permanently lost
fragments on unreliable channels.

```cpp
class FragmentAssembler {
public:
    // Returns completed message when all fragments arrive, nullopt otherwise.
    std::optional<Message> addFragment(
        ChannelId channel, uint16_t groupId,
        uint16_t index, uint16_t count,
        std::span<const uint8_t> data);

    // Expire incomplete groups older than maxAge.
    void expireStale(std::chrono::milliseconds maxAge);
};
```

---

## 9. Compression

### 9.1 Algorithm

**zstd** is the compression algorithm. It offers an excellent
speed-to-ratio tradeoff across a wide range of compression levels:
- Level 1–3: real-time compression (hundreds of MB/s)
- Level 6–9: higher ratio for bulk data

The default compression level is configurable per channel.

### 9.2 Compression Decision Flow

```
Message arrives at muxer for channel C
  │
  ├── C.compression == Never           → send uncompressed
  ├── C.compression == Always          → compress
  └── C.compression == SizeThreshold
        ├── payload.size() < C.compressionThreshold → send uncompressed
        └── payload.size() >= C.compressionThreshold → compress
```

### 9.3 Async Compression Pipeline

**Design principle:** The network I/O thread should never block on
compression. Synchronous compression on the send thread means the wire sits
idle while the CPU crunches — wasting bandwidth headroom. Instead,
compression is implemented as **queue hops**: the network thread hands off
work to compression workers and continues sending other traffic.

```
Outbound path:
  Application ──send()──► Outbound Queue
                               │
                               ▼
  Network Thread: dequeue, check compression policy
       │                                    │
       ├── No compression needed ──────────►├── Frame + send on wire
       │                                    │
       └── Needs compression ──► Compress   │
            Job Queue ──► Worker Thread(s)  │
                               │            │
                               ▼            │
                          Compressed        │
                          Result Queue ────►┘
```

The network thread continuously sends whatever is ready — uncompressed
messages that don't need compression, plus compressed results as they arrive
from the worker pool. This keeps the wire busy even while large payloads are
being compressed.

**Decompression** on the receive side follows the same async pattern for
large payloads. Small payloads (below a configurable threshold, default
4 KB) are decompressed synchronously to avoid the queue-hop overhead.

```cpp
class CompressionPool {
public:
    explicit CompressionPool(uint32_t threadCount = 2);

    // Async — result pushed to the provided result queue.
    void compressAsync(CompressJob job);
    void decompressAsync(DecompressJob job);

    // Sync — for small payloads where queue-hop overhead exceeds benefit.
    std::vector<uint8_t> compress(std::span<const uint8_t> data);
    std::vector<uint8_t> decompress(std::span<const uint8_t> data,
                                    uint32_t originalSize);
};

struct CompressJob {
    std::vector<uint8_t> data;
    ChannelId channel;
    uint16_t  seqNum;
    // ... enough metadata to frame the result on the wire after compression.
};
```

### 9.4 Dual Local + Network Delivery

When a host player has both a local connection and network connections
(Section 7.6), a single `send()` on the server may need to deliver the
message to both:

- **Local connection:** The uncompressed `Message` is moved directly onto
  the local queue — no compression, no serialization.
- **Network connections:** The message is compressed (if applicable) and
  framed for the wire.

The muxer handles this by routing the original `Message` to local
recipients first (zero-copy move), then submitting the payload to the
compression pipeline for network recipients. The message data is only
copied if it must go to both local and network paths.

### 9.5 Dictionary Compression (Optional)

Applications can provide pre-trained zstd dictionaries for specific channels
or message types. This significantly improves compression ratios for small,
structured messages with repetitive patterns.

Dictionaries are included in the connection handshake (both peers must
agree on the same dictionary for each channel that uses one). Dictionary
data is sent during handshake rather than cached on clients, since client
software updates may change the dictionary and there's no reliable way to
guarantee cache validity.

---

## 10. Priority Scheduling (Muxer)

The muxer is the heart of the outbound pipeline. It decides what goes into
each datagram.

### 10.1 Scheduling Algorithm

```
Each send tick:
  1. Collect pending messages from all channels.
  2. Add retransmission candidates from AckTracker.
  3. Sort by: priority (ascending), then age (oldest first within same priority).
  4. Pack messages into datagrams up to MTU:
     a. Every datagram includes the ack header (piggyback acks).
     b. Fill with highest-priority messages first.
     c. If space remains, pack lower-priority messages.
  5. Respect bandwidth budget:
     - Critical priority has a reserved fraction (always available).
     - Bulk priority only uses leftover bandwidth.
  6. Send datagrams.
```

### 10.2 Bandwidth Budget

```cpp
struct BandwidthConfig {
    uint32_t maxBytesPerSecond;      // Total cap (0 = unlimited)
    float    criticalReserve;        // Fraction reserved for Critical (e.g. 0.2)
    float    bulkMaxFraction;        // Max fraction for Bulk (e.g. 0.5)
};
```

Under congestion, the muxer sheds load from bottom up:
1. Drop `Unreliable` messages from `Bulk` priority first.
2. Reduce `Low` throughput.
3. `Critical` and `High` are the last to be throttled.

Reliable messages are never dropped — they queue and backpressure the
sender.

### 10.3 Send Rate

The muxer operates on the network I/O thread (see Section 16.2) and
flushes outbound datagrams whenever messages are available in the outbound
queues. The network thread wakes on queue push, so messages are sent as
soon as they arrive — no fixed polling rate.

For applications that prefer `connection.update()`-driven sending (e.g.,
single-threaded mode), the muxer flushes on each `update()` call instead.

### 10.4 Packet Coalescing and Socket Tuning

The muxer defaults to **zero coalescing delay** — messages are sent
immediately. The conditions where coalescing helps (network backed up) are
the same conditions where the network is already congested, so coalescing
buys little and costs latency. Optimize for immediacy.

A configurable coalescing window (e.g., 0–2 ms) is available for
applications that prefer to batch, but the default is 0.

**Socket tuning:** On TCP connections, the library disables **Nagle's
Algorithm** (`TCP_NODELAY`) and, where the platform supports it, disables
**delayed ACK** (`TCP_QUICKACK` on Linux). These two algorithms interact
badly for real-time traffic: Nagle holds small writes waiting for an ACK,
while delayed ACK holds the ACK waiting for data to piggyback on — creating
up to 200 ms of artificial latency. Disabling both ensures packets are sent
and acknowledged as quickly as the OS can manage.

---

## 11. Reliability and Acknowledgment (Network Transports Only)

### 11.1 Selective ACK (SACK)

Every outbound datagram piggybacks acknowledgments in its header:
- `lastAckSeq` — highest datagram sequence received from the peer.
- `ackBitfield` — 32 bits, where bit N indicates receipt of datagram
  `lastAckSeq - N - 1`.

This acks up to 33 datagrams per outbound datagram with zero overhead
(no dedicated ack packets needed).

### 11.2 Retransmission

```cpp
class AckTracker {
public:
    // Track a reliable message that was just sent.
    void trackSent(uint16_t channelSeq, ChannelId channel,
                   std::span<const uint8_t> payload, Priority priority);

    // Process ack information from a received datagram.
    void processAck(uint32_t datagramSeq, uint32_t lastAckSeq,
                    uint32_t ackBitfield);

    // Get messages needing retransmission.
    std::vector<PendingPacket> getRetransmissions();

    // RTT statistics.
    float smoothedRttMs() const;   // Exponentially weighted moving average
    float rttVarianceMs() const;
};
```

**Retransmission policy:**
- A reliable message is considered lost if unacked after `2 × smoothedRTT`.
- Retransmissions use exponential backoff (2×, 4×, 8× RTT, capped).
- After a configurable maximum number of retransmissions (default: 15),
  the connection is considered dead and disconnected.

### 11.3 Ordered Delivery

For `ReliableOrdered` channels, the receiver buffers out-of-order messages
and delivers them in sequence:

```cpp
class OrderedReceiver {
public:
    // Accept a message with its channel sequence number.
    // Returns all messages that are now deliverable (may be multiple
    // if this message filled a gap).
    std::vector<Message> receive(uint16_t seq, Message msg);

    // Number of buffered messages waiting for gap-fill.
    size_t bufferedCount() const;
};
```

### 11.4 Sequence Number Wrapping

Per-channel sequence numbers are 16-bit (`uint16_t`) and wrap at 65536.
At 60 messages/sec on a single channel, this wraps in ~18 minutes. This
is by design — the sequence number only needs to cover the retransmission
and reorder window (typically a few hundred messages at most), not be
globally unique.

**All sequence comparisons must use modular arithmetic** (RFC 1982 serial
number arithmetic). A sequence `a` is "before" `b` if
`(int16_t)(a - b) < 0`. This correctly handles wrapping as long as the
compared sequences are within 32768 of each other — far larger than any
reasonable retransmission window.

Implementers: do not use `a < b` for sequence comparison. Use the modular
comparison helper consistently throughout the AckTracker, OrderedReceiver,
and FragmentAssembler.

### 11.5 Congestion Control

Congestion control is an adaptive layer between the muxer's "what to send"
decision and the actual socket write. It determines **how much** data can
be sent at any given moment, complementing the muxer's decision of **what**
to send.

```
Muxer (priority scheduling)
  │ "here are the messages to send, in priority order"
  ▼
Congestion Controller
  │ "you may send up to N bytes this interval"
  │ adjusts N based on: RTT samples, packet loss, ack rate
  ▼
Socket Write
```

**Interactions with other components:**

| Component | Interaction |
|-----------|-------------|
| **AckTracker** | Feeds RTT samples and loss information to the congestion controller. An ack means bytes were delivered; a timeout means congestion. |
| **Bandwidth budget** | The application's `maxBytesPerSecond` is a hard ceiling. The congestion controller operates *below* this ceiling, further reducing the send rate when the network is congested. |
| **Priority scheduler** | When the congestion window shrinks, the muxer sheds load from bottom up (Bulk first, then Low, etc.). Critical priority always gets its reserved fraction of the current window. |
| **Retransmission timer** | Congestion events (loss, timeout) trigger both retransmission and a reduction of the congestion window. These must be coordinated to avoid retransmission storms. |

**Algorithm selection** is deferred to implementation (see Section 21 open
questions). Candidates include BBR-style (model-based, measures bottleneck
bandwidth and RTT) and AIMD (loss-based, classic approach). The interface
between the muxer and the congestion controller will be abstract enough to
swap algorithms.

```cpp
class CongestionController {
public:
    // How many bytes may be sent right now?
    uint32_t availableWindow() const;

    // Notify that bytes were sent (reduces available window).
    void onSend(uint32_t bytes);

    // Notify that bytes were acknowledged (opens window).
    void onAck(uint32_t bytes, float rttMs);

    // Notify that a packet was lost (shrinks window).
    void onLoss();

    // Current estimated bandwidth (bytes/sec).
    uint32_t estimatedBandwidth() const;
};
```

---

## 12. Connection Lifecycle

### 12.1 States

```
Disconnected ──► Connecting ──► Connected ──► Disconnecting ──► Disconnected
                     │                              │
                     └───── Timeout ────────────────┘
```

### 12.2 Handshake

The handshake runs over channel 0 (which the library implicitly creates as a
reliable-ordered control channel).

```
Initiator (Client)                   Responder (Server)
       │                                    │
       │─── Hello ─────────────────────►   │
       │    protocol version                │
       │    supported features bitmask      │
       │    client nonce                    │
       │                                    │
       │◄────────────────── HelloAck ────  │
       │    connection ID                   │
       │    accepted features               │
       │    server nonce                    │
       │    channel config agreement        │
       │                                    │
       │─── Ready ─────────────────────►   │
       │    (confirms receipt)              │
       │                                    │
       │         ═══ Connected ═══         │
```

The handshake messages are library-internal (not application message types).
The library reserves type IDs 0xFFFF'FF00–0xFFFF'FFFF for internal control
messages:

| Internal Type | ID | Purpose |
|---------------|----|---------|
| Hello | 0xFFFFFF00 | Initiate connection |
| HelloAck | 0xFFFFFF01 | Accept connection |
| Ready | 0xFFFFFF02 | Confirm handshake complete |
| KeepAlive | 0xFFFFFF03 | Liveness probe |
| Disconnect | 0xFFFFFF04 | Graceful disconnect |

Applications never see or handle these types.

**Connection ID generation:** The 4-byte `connectionId` in the datagram
header is a **cryptographically random** 32-bit value generated by the
server during handshake and sent to the client in the `HelloAck`. Using a
random value (rather than a counter or hash) provides:
- **Spoofing resistance:** An attacker cannot guess valid connection IDs
  to inject forged packets. With a 32-bit random space and ~100 active
  connections, the probability of guessing a valid ID is ~1 in 43 million
  per attempt.
- **No collision coordination:** The server doesn't need to track
  previously used IDs or coordinate across restarts.
- **Privacy:** Connection IDs don't leak information about connection
  order or count.

The server rejects any datagram whose `connectionId` doesn't match a known
active connection. Collision probability (~100 connections / 2^32) is
negligible. If a collision occurs, the server detects it during handshake
(the nonce exchange will fail) and assigns a new ID.

### 12.3 Keepalive

If no datagram has been sent for `keepAliveIntervalMs` (default: 5000 ms),
a keepalive datagram is sent. If no datagram has been received for
`connectionTimeoutMs` (default: 15000 ms), the connection is considered
dead.

### 12.4 Graceful Disconnect

Either peer can send a `Disconnect` message with an optional reason string.
The sender continues processing inbound traffic for a short grace period
(default: 1 second) to allow in-flight reliable messages to be acked.

### 12.5 Connection API

```cpp
class Connection {
public:
    // ── Client-side: connect to a server over the network ──
    static std::unique_ptr<Connection> connect(
        const std::string& host, uint16_t port,
        const ConnectionConfig& config = {});

    // ── Local: create an in-process connection pair (see Section 7) ──
    struct LocalPair {
        std::unique_ptr<Connection> client;
        std::unique_ptr<Connection> server;
    };
    static LocalPair createLocalPair(const ConnectionConfig& config);

    ~Connection();

    // ── State ──
    ConnectionState state() const;
    bool isConnected() const;

    // ── Transport info ──
    TransportType transportType() const;
    bool isLocal() const;   // Convenience: transportType() == TransportType::Local

    // ── Sending ──
    SendResult send(ChannelId channel, Message msg);
    SendResult send(ChannelId channel, Message msg, Priority overridePriority);

    // ── Receiving (multi-threaded mode) ──
    // Direct queue access for game logic thread to drain.
    Queue<Message>& inboundQueue();

    // ── Receiving (single-threaded mode / update()-driven) ──
    // Register handler for a specific message type (highest priority).
    using MessageHandler = std::function<void(const Message&)>;
    void onMessage(uint32_t typeId, MessageHandler handler);

    // Register handler for all messages on a channel (catch-all).
    void onChannel(ChannelId channel, MessageHandler handler);

    // Dispatch priority (see §12.8):
    //   1. onMessage(typeId) — if registered, ONLY this handler fires.
    //   2. onChannel(channelId) — fires only if no type-specific handler matched.
    //   3. Fallback handler (ChannelRouter) — if neither matched.
    // Note: these callbacks are only invoked by update() in single-threaded
    // mode. In multi-threaded mode, drain inboundQueue() instead.

    // ── Connection events ──
    using StateHandler = std::function<void(ConnectionState)>;
    void onStateChange(StateHandler handler);

    // ── Lifecycle ──
    void disconnect(std::string_view reason = "");
    void update();   // Call each tick: process inbound, flush outbound.

    // ── Stats ──
    ConnectionStats stats() const;
};

enum class TransportType : uint8_t {
    Local,    // In-process, same-process peer
    Udp,      // UDP socket
    Tcp,      // TCP socket
};
```

### 12.6 Connection Configuration

```cpp
struct ConnectionConfig {
    TransportMode transport = TransportMode::PreferUdp;
    uint32_t maxBandwidthBps   = 0;         // 0 = unlimited
    uint32_t connectionTimeoutMs = 15000;
    uint32_t keepAliveIntervalMs = 5000;
    uint32_t mtu               = 1200;
    uint32_t compressionThreads = 2;
    uint32_t maxRetransmissions = 15;

    float coalescingWindowMs = 0.0f;        // 0 = send immediately

    // Debug: force local transport through full serialize/compress pipeline.
    bool debugSerialization = false;

    BandwidthConfig bandwidth = {
        .maxBytesPerSecond = 0,
        .criticalReserve   = 0.2f,
        .bulkMaxFraction   = 0.5f,
    };

    std::vector<ChannelConfig> channels;    // Application-defined
};

enum class TransportMode : uint8_t {
    PreferUdp,     // Try UDP, fall back to TCP
    UdpOnly,       // UDP or fail
    TcpOnly,       // TCP only
    // Local transport is not selected via TransportMode — it is created
    // via Connection::createLocalPair() (see Section 7).
};
```

### 12.7 Connection Statistics

```cpp
struct ConnectionStats {
    float    rttMs;
    float    packetLossPercent;
    uint64_t bytesSent;
    uint64_t bytesReceived;
    uint64_t packetsSent;
    uint64_t packetsReceived;
    uint32_t pendingReliableMessages;

    struct ChannelStats {
        uint64_t messagesSent;
        uint64_t messagesReceived;
        uint64_t bytesSent;
        uint64_t bytesReceived;
        uint32_t retransmissions;
        uint32_t droppedMessages;     // Unreliable channels only
    };
    std::unordered_map<ChannelId, ChannelStats> perChannel;
};
```

### 12.8 Message Dispatch Rules

When a message arrives, the connection dispatches it using a priority chain:

```
Inbound message (typeId=T, arrived on channel=C)
  │
  ├─ onMessage(T) registered?
  │    YES → call that handler. DONE. (Channel handler is NOT called.)
  │    NO  ↓
  ├─ onChannel(C) registered?
  │    YES → call that handler. DONE.
  │    NO  ↓
  └─ ChannelRouter fallback handler registered?
       YES → call fallback. DONE.
       NO  → drop message (log warning in debug builds).
```

**Key rule:** `onMessage` and `onChannel` are mutually exclusive per
message delivery. If a type-specific handler is registered and matches,
the channel handler is **not** also called. This prevents double-processing.

**Rationale:** `onMessage(typeId)` is the primary registration mechanism —
the application knows which types it expects and registers handlers for
each. `onChannel(channelId)` is a catch-all for cases where the application
wants to handle all messages on a channel uniformly (e.g., forwarding,
logging, or protocol-agnostic routing). The fallback handler on
`ChannelRouter` handles truly unknown message types (forward compatibility).

---

## 13. Server-Side Listener

```cpp
class ConnectionListener {
public:
    static std::unique_ptr<ConnectionListener> listen(
        uint16_t port, const ListenerConfig& config = {});

    // Poll for new connections (non-blocking).
    std::unique_ptr<Connection> accept();

    // Callback-based alternative.
    using AcceptHandler = std::function<void(std::unique_ptr<Connection>)>;
    void onAccept(AcceptHandler handler);

    void stop();
};

struct ListenerConfig {
    uint32_t maxConnections = 256;
    ConnectionConfig defaultConnectionConfig;
    // Optional: per-connection config override callback.
    std::function<ConnectionConfig(const std::string& peerAddress)> configOverride;
};
```

---

## 14. Concurrency Primitives

finenet includes a set of general-purpose, thread-safe concurrent data
structures. These are used internally by the transport layer (e.g., for
per-channel message queues, the local transport's SPSC paths, and
connection accept queues) and are also part of the public API for
applications to use directly.

By shipping these as part of finenet, applications that depend on finenet
get a production-quality concurrent queue library for free — and the
QueueBridge adapter (Section 15.2) has concrete queue types to bind to
without depending on an external concurrency library.

**Provenance:** These primitives were lifted from finevox, where they
replaced an earlier set of specialized queues (SimpleQueue, CoalescingQueue,
BlockingQueue, AlarmQueue, AlarmQueueWithData). The three surviving types
— `Queue<T>`, `KeyedQueue<K,D>`, and `WakeSignal` — cover all the same
use cases with a smaller, more composable API. The deprecated types have
been removed from finevox; both projects now share this common set.

All three primitives are header-only.

### 14.1 Queue\<T\> — Thread-Safe MPSC FIFO Queue

A general-purpose, mutex-protected FIFO queue with integrated support for
timed alarms, multi-queue coordination via `WakeSignal`, and optional
capacity limits for backpressure.

**Thread safety model:** Multi-Producer, Single-Consumer (MPSC). Multiple
threads may call `push()` concurrently. Only one thread should call
`tryPop()` / `drainAll()` / `waitForWork()`.

```cpp
namespace finenet {

template<typename T>
class Queue {
public:
    using Clock     = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    Queue() = default;
    explicit Queue(size_t capacity);    // 0 = unlimited (default).
    // Non-copyable, non-movable.

    // ── Capacity (backpressure) ──
    void   setCapacity(size_t maxItems);  // 0 = unlimited.
    size_t capacity() const;              // 0 means unlimited.
    bool   isFull() const;               // size() >= capacity (if limited).

    // ── WakeSignal attachment (multi-queue consumers) ──
    void attach(WakeSignal* signal);
    void detach();
    bool isAttached() const;

    // ── Push (producer — thread-safe, multiple producers OK) ──
    // push() is NON-BLOCKING. If the queue has a capacity limit and is
    // full, push() returns false (item not enqueued). If the queue has
    // no capacity limit, push() always succeeds and returns true.
    // This avoids deadlocks in single-threaded mode or when producers
    // hold locks.
    bool push(const T& item);
    bool push(T&& item);

    // pushBlocking() BLOCKS until space is available. Only use this
    // when you are certain the consumer is on a different thread and
    // will drain the queue. Deadlocks if the consumer is on the same
    // thread or stopped.
    void pushBlocking(const T& item);
    void pushBlocking(T&& item);

    void pushBatch(std::vector<T> items);       // Non-blocking; drops if full.
    template<typename Iterator>
    void pushBatch(Iterator begin, Iterator end);
    // On push: notifies BOTH the internal condition variable AND any
    // attached WakeSignal, so both single-queue and multi-queue
    // consumers are woken.

    // ── Pop (consumer — single consumer only) ──
    std::optional<T> tryPop();              // Non-blocking.
    std::vector<T>   drainAll();            // Remove and return everything.
    std::vector<T>   drainUpTo(size_t n);   // Remove up to n items.

    // ── Alarm (timed wakeup) ──
    void setAlarm(TimePoint wakeTime);   // If alarm exists, keeps later time.
    void clearAlarm();
    bool hasAlarm() const;

    // ── Blocking wait (single-queue consumers) ──
    bool waitForWork();                                 // Until push, alarm, or shutdown.
    bool waitForWork(std::chrono::milliseconds maxWait);// With timeout.
    // Returns false if shutdown was requested.

    // ── Shutdown ──
    void shutdown();        // Wake all waiters; subsequent pushes are silent drops.
    bool isShutdown() const;
    void resetShutdown();

    // ── Query ──
    bool   empty() const;
    size_t size() const;
    void   clear();         // Clears items and cancels alarm.
};

}  // namespace finenet
```

**Internal storage:** `std::deque<T>` protected by a `std::mutex`, with a
`std::condition_variable` for blocking waits. The deque provides amortized
O(1) push/pop at both ends and doesn't invalidate iterators on push
(important for `drainAll()`).

**Dual notification:** On every `push()`, the queue notifies its internal
condition variable (for `waitForWork()` callers) and signals any attached
`WakeSignal` (for multi-queue consumers). This means a single `Queue` can
be consumed either by a dedicated thread that calls `waitForWork()` or by
a multi-queue event loop that blocks on a shared `WakeSignal` — without
changing the queue's configuration.

**Capacity limits and backpressure:** When a capacity is set, `push()`
returns `false` if the queue is full, allowing the producer to decide what
to do (drop, retry, apply backpressure upstream). `pushBlocking()` blocks
until space is available — useful when the producer has nothing else to do
and the consumer is guaranteed to be on a separate thread.

The non-blocking `push()` is the default because a blocking push is a
deadlock footgun: if the consumer is on the same thread (single-threaded
mode), or if the producer holds a lock the consumer needs, the program
hangs. By making the safe path the easy path, accidental deadlocks are
avoided.

When no capacity is set (the default), `push()` always succeeds and returns
`true`. Capacity limits prevent runaway producers from causing
out-of-memory conditions — even if the consumer is broken or slow, the
process won't blow up.

Capacity can also be set on `KeyedQueue` (Section 14.2) using the same
`setCapacity()` / `push()` / `pushBlocking()` pattern.

### 14.2 KeyedQueue\<Key, Data\> — Deduplicating Queue with Merge

A thread-safe queue where each item is identified by a key. If a key is
pushed that already exists in the queue, the new data is **merged** with
the existing data rather than creating a duplicate entry. This is useful
for coalescing updates: e.g., if a chunk needs re-meshing twice before
the consumer drains it, only one entry remains in the queue with the
merged data.

```cpp
template<typename Key, typename Data,
         typename Hash = std::hash<Key>,
         typename Equal = std::equal_to<Key>>
class KeyedQueue {
public:
    using Clock     = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using MergeFn   = std::function<Data(const Data& existing, const Data& incoming)>;

    KeyedQueue();                      // Default merge: replace with incoming.
    explicit KeyedQueue(MergeFn merge);// Custom merge.

    // ── WakeSignal attachment ──
    void attach(WakeSignal* signal);
    void detach();
    bool isAttached() const;

    // ── Push (producer) ──
    bool   push(const Key& key, const Data& data);   // true = new key, false = merged.
    bool   push(Key&& key, Data&& data);
    size_t pushBatch(std::vector<std::pair<Key, Data>> items);  // Returns count of new keys.

    // ── Pop (consumer) ──
    std::optional<std::pair<Key, Data>> tryPop();      // Non-blocking.
    std::optional<std::pair<Key, Data>> popWait();     // Blocking.
    std::vector<std::pair<Key, Data>>   drainAll();
    std::vector<std::pair<Key, Data>>   drainUpTo(size_t n);

    // ── Alarm ──
    void setAlarm(TimePoint wakeTime);
    void clearAlarm();
    bool hasAlarm() const;

    // ── Blocking wait ──
    bool waitForWork();
    bool waitForWork(std::chrono::milliseconds maxWait);

    // ── Query ──
    bool   empty() const;
    size_t size() const;
    bool   contains(const Key& key) const;
    std::optional<Data> getData(const Key& key) const;  // Peek without removing.

    // ── Shutdown ──
    void shutdown();
    bool isShutdown() const;
    void resetShutdown();
    void clear();
};
```

**Internal storage:** Three structures provide O(1) operations:
- `std::deque<Key>` — maintains FIFO insertion order.
- `std::unordered_set<Key>` — O(1) presence check for deduplication.
- `std::unordered_map<Key, Data>` — stores the current (possibly merged) data.

On `tryPop()`, the front key is dequeued, looked up in the map, and the
data is moved out. On `push()` with an existing key, the merge function
is called and the key retains its original position in the FIFO order
(no re-insertion — the consumer sees the merged result when the key's
turn comes up).

### 14.3 WakeSignal — Multi-Queue Coordination

A synchronization primitive that allows a single consumer thread to block
until **any** of multiple queues has work available — or a deadline is
reached, or shutdown is requested.

```cpp
class WakeSignal {
public:
    using Clock     = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    WakeSignal() = default;
    // Non-copyable, non-movable.

    // ── Producer API (called by queues on push) ──
    void signal();

    // ── Consumer API ──
    bool wait();                                        // Block until signaled, deadline, or shutdown.
    bool waitFor(std::chrono::milliseconds timeout);    // Block with timeout.
    // Returns false if shutdown was requested.

    // ── Deadline (frame-synchronized wakeup) ──
    void      setDeadline(TimePoint when);
    void      clearDeadline();
    bool      hasDeadline() const;
    TimePoint deadline() const;

    // ── Shutdown ──
    void requestShutdown();
    bool isShutdown() const;
    void resetShutdown();
    void reset();               // Clear signaled, deadline, and shutdown state.
};
```

**How it fits together:** Multiple `Queue<T>` and/or `KeyedQueue<K,D>`
instances attach to the same `WakeSignal`. When any queue pushes an item,
it calls `signal()`. The consumer thread blocks on `WakeSignal::wait()`,
wakes up, then drains whichever queues have items. This replaces the
pattern of one-thread-per-queue or busy polling.

```
                     ┌──────────────┐
  Queue<A> ─attach──►│              │
  Queue<B> ─attach──►│  WakeSignal  │◄── consumer blocks on wait()
  KeyedQueue<K,D> ──►│              │    wakes when any queue pushes,
                     └──────────────┘    or deadline fires, or shutdown
```

**Deadline support:** `setDeadline()` schedules an automatic wakeup at a
specific time point. This is useful for frame-synchronized loops: the
consumer processes all available work, then sets a deadline for the next
frame boundary. If no work arrives before the deadline, the consumer
wakes up anyway to advance the frame.

**Extensibility beyond queues:** The `attach(WakeSignal*)`/`signal()`
pattern is not limited to queues. Any async event source can participate
in the same wake-on-event loop by holding a `WakeSignal*` and calling
`signal()` when its condition is met. For example, finevox uses a
`FrameFenceWaiter` adapter — a background thread that blocks on a GPU
fence and signals the attached `WakeSignal` when the fence completes.
The consumer thread sleeps on the same `WakeSignal` as its queues, so it
wakes for either a new queue item **or** a completed GPU fence, whichever
comes first. This pattern generalizes to socket readiness, timer events,
OS signals, or any asynchronous completion.

```
                     ┌──────────────┐
  Queue<A> ─attach──►│              │
  Queue<B> ─attach──►│  WakeSignal  │◄── consumer blocks on wait()
  FenceWaiter ──────►│              │    wakes on queue push, fence
  SocketReady ──────►│              │    completion, deadline, etc.
                     └──────────────┘
```

### 14.4 Usage Patterns

**Single-queue consumer** (dedicated thread per queue):
```cpp
finenet::Queue<WorkItem> workQueue;

// Consumer thread:
while (!workQueue.isShutdown()) {
    workQueue.waitForWork();
    for (auto& item : workQueue.drainAll()) {
        process(item);
    }
}
```

**Multi-queue consumer** (one thread, many queues):
```cpp
finenet::Queue<InputEvent>  inputQ;
finenet::Queue<NetworkMsg>  netQ;
finenet::KeyedQueue<ChunkPos, ChunkData> chunkQ;
finenet::WakeSignal wake;

inputQ.attach(&wake);
netQ.attach(&wake);
chunkQ.attach(&wake);

// Consumer thread:
while (wake.wait()) {
    for (auto& ev : inputQ.drainAll())  handleInput(ev);
    for (auto& msg : netQ.drainAll())   handleNetwork(msg);
    for (auto& [pos, data] : chunkQ.drainAll()) rebuildChunk(pos, data);
}
```

**Frame-paced consumer** (game server tick loop):
```cpp
finenet::WakeSignal wake;
inputQ.attach(&wake);
netQ.attach(&wake);

auto nextTick = Clock::now();
while (wake.wait()) {
    // Process all available work
    for (auto& ev : inputQ.drainAll())  handleInput(ev);
    for (auto& msg : netQ.drainAll())   handleNetwork(msg);

    if (Clock::now() >= nextTick) {
        gameServer.tick(dt);
        nextTick += tickInterval;
    }
    wake.setDeadline(nextTick);
}
```

### 14.5 Role in finenet Internals

The queue primitives are used throughout finenet itself:

| Internal use | Queue type | Purpose |
|-------------|-----------|---------|
| Per-channel outbound queue | `Queue<Message>` | Buffered messages waiting to be muxed |
| Per-channel inbound delivery | `Queue<Message>` | Received messages waiting for handler dispatch |
| Local transport paths | `Queue<Message>` | Per-channel queues between local connection pairs |
| Connection accept queue | `Queue<Connection>` | Listener pushes accepted connections |
| Compression pool work queue | `Queue<CompressJob>` | Jobs dispatched to compression threads |

Applications can also use these queues directly for their own subsystem
communication — they're not limited to finenet's internal use.

---

## 15. Extension Points

finenet is deliberately minimal in its core. Applications extend it through
these mechanisms:

### 15.1 Custom Serialization Helpers

Applications can write free functions or wrapper classes that extend
`MessageBuilder` / `MessageReader` for domain-specific types:

```cpp
// In application code, not in finenet:
inline finenet::MessageBuilder& writeVec3(
    finenet::MessageBuilder& b, float x, float y, float z) {
    return b.writeF32(x).writeF32(y).writeF32(z);
}

inline void readVec3(finenet::MessageReader& r, float& x, float& y, float& z) {
    x = r.readF32(); y = r.readF32(); z = r.readF32();
}
```

### 15.2 Queue Bridge

A template adapter that connects a concurrent queue to a network channel.
The application provides encode/decode functions. Defaults to
`finenet::Queue<T>` (Section 14.1) but can be parameterized with any
queue type that provides `push()`, `tryPop()`, and `drainAll()`.

```cpp
template<typename T, typename QueueType = Queue<T>>
class QueueBridge {
public:
    QueueBridge(QueueType& localQueue, Connection& connection,
                ChannelId channel, uint32_t messageTypeId);

    using Encoder = std::function<Message(const T& item)>;
    using Decoder = std::function<T(MessageReader& reader)>;

    void setEncoder(Encoder enc);
    void setDecoder(Decoder dec);

    // Drain local queue → encode → send. Called from send thread.
    size_t flushToNetwork();

    // Decode incoming → push to local queue. Called from receive thread.
    void onReceive(const Message& msg);

    // Batch multiple items into one message.
    void setBatchMode(bool enabled, uint32_t maxBatchSize = 32);
};
```

### 15.3 Message Type Router

Applications register which channel handles each message type. The demuxer
uses this to route inbound messages. For outbound messages, the application
explicitly specifies the channel in `connection.send()`.

```cpp
class ChannelRouter {
public:
    void assign(uint32_t typeId, ChannelId channel);
    void assignRange(uint32_t typeIdStart, uint32_t typeIdEnd, ChannelId channel);
    ChannelId resolve(uint32_t typeId) const;

    // Fallback for unregistered types.
    using FallbackHandler = std::function<void(const Message&)>;
    void setFallback(FallbackHandler handler);
};
```

---

## 16. Threading Model

### 16.1 Overview — Wake-on-Message, Not Polling

finenet's primary threading model is **wake-on-message**: consumer threads
block on a `Queue` or `WakeSignal` and wake instantly when work arrives.
Fixed-rate polling is avoided — threads that need periodic ticks (e.g., a
game server tick loop) superimpose timing via `WakeSignal::setDeadline()` or
`Queue::setAlarm()` and wake for **either** incoming work or the next
scheduled tick, whichever comes first.

This design means:

- **Most threads sleep until needed.** A network I/O thread, a game logic
  thread, and a compression worker thread all wake only when there is
  work to process. CPU usage is near-zero when idle.
- **No fixed "60 Hz" update rate for non-graphics threads.** The 60 Hz or
  framerate-driven tick only applies to the graphics/rendering thread
  (which is outside finenet's scope). Game logic threads use alarm-based
  timing to run their tick at a fixed interval (e.g., 20 ticks/sec) while
  still waking instantly for urgent messages between ticks.
- **Single-threaded mode is supported** but multi-threaded is the primary
  design target. finenet assumes the application has dedicated threads for
  network I/O, game logic, and (optionally) compression.

### 16.2 Multi-Threaded Architecture (Primary)

In the primary multi-threaded design, finenet's internal network thread owns
socket I/O and the wire pipeline. The application's game logic thread
consumes decoded messages from queues. **`connection.update()` is not called
in multi-threaded mode** — the network thread drives the pipeline
internally, and the game logic thread drains inbound queues directly.

```
┌─────────────────────────────────────────────────────────────────┐
│ Network I/O Thread (internal to finenet)                         │
│   Owns: UDP/TCP sockets, muxer, demuxer, ack tracker,           │
│          congestion controller, keepalive timer                   │
│   Wakes: on socket data (epoll/kqueue), on outbound queue push,  │
│          or on keepalive/retransmission timer                     │
│                                                                   │
│   Outbound: pulls from per-channel outbound Queues →             │
│             compress → fragment → frame → socket write            │
│                                                                   │
│   Inbound:  socket read → deframe → reassemble → decompress →   │
│             push decoded Message to per-connection inbound Queue  │
├─────────────────────────────────────────────────────────────────┤
│ Game Logic Thread (application-owned)                            │
│   Consumes: per-connection inbound Queues (via WakeSignal)       │
│   Produces: outbound messages via connection.send() → outbound Q │
│                                                                   │
│   Pattern:                                                        │
│     attach all inbound queues to a WakeSignal                     │
│     loop:                                                         │
│       wake.wait()  // wakes on message arrival OR tick deadline   │
│       for each connection: drain inbound queue → dispatch handlers│
│       if tick due: gameServer.tick(dt)                             │
│       wake.setDeadline(nextTick)                                  │
├─────────────────────────────────────────────────────────────────┤
│ Compression Worker(s) (1–N threads, configurable)                │
│   Wakes: on compression job queue push                           │
│   Pushes: compressed results → result queue → network thread     │
├─────────────────────────────────────────────────────────────────┤
│ Graphics Thread (application-owned, outside finenet)             │
│   Runs at display refresh rate, blocked on swapchain/vsync       │
│   Reads game state snapshot; does NOT interact with finenet      │
└─────────────────────────────────────────────────────────────────┘
```

**Data flow — who calls what:**

| Operation | Thread | Mechanism |
|-----------|--------|-----------|
| `connection.send(ch, msg)` | Any (game logic, etc.) | Pushes to outbound queue (MPSC) |
| Socket read → demux → decode | Network I/O thread | Internal; pushes to inbound queue |
| Drain inbound queue → handlers | Game logic thread | Application drains `Queue<Message>` |
| Retransmission, keepalive | Network I/O thread | Timer-driven internally |

In this model, `onMessage()` / `onChannel()` handlers registered on the
`Connection` are **not used**. Instead, the game logic thread pulls
`Message` objects from the connection's inbound queue and dispatches them
itself. This gives the game logic thread full control over when and how
messages are processed, and avoids cross-thread callback complexity.

The `Connection` exposes its inbound queue for direct access:

```cpp
// In multi-threaded mode, the game logic thread consumes this directly:
Queue<Message>& Connection::inboundQueue();

// Attach to a WakeSignal so the game thread wakes on any connection's
// inbound message:
for (auto& conn : connections_) {
    conn->inboundQueue().attach(&gameWake);
}
```

### 16.3 Single-Threaded Mode

For simpler applications or testing, finenet works entirely on a single
thread. The application calls `connection.update()` in its main loop, which
does everything in one call:

1. Reads from sockets (non-blocking poll).
2. Runs the demuxer (deframe, reassemble, decompress).
3. Dispatches decoded messages to `onMessage()` / `onChannel()` handlers.
4. Flushes outbound queues through the muxer (compress, fragment, frame).
5. Sends datagrams on sockets.
6. Handles keepalive and retransmission timers.

Compression runs synchronously (the async pool is not started).

**`onMessage()` and `onChannel()` handlers are only invoked in
single-threaded mode** (from within `update()`). In multi-threaded mode,
the application drains the inbound queue itself, so these callback
registrations are not used.

This keeps the callback contract simple: handlers always fire on the thread
that called `update()`, and that thread has exclusive access to game state.

### 16.4 Thread Safety

- `connection.send()` is thread-safe. Multiple threads may call `send()`
  on the same connection concurrently (MPSC pattern — see Section 14).
- `connection.update()` must be called from a single thread (single-threaded
  mode only).
- `connection.inboundQueue()` returns a reference to the MPSC inbound queue.
  The network thread pushes; the game logic thread pops. Only one consumer.
- `connection.stats()` is thread-safe (atomic reads).
- All other methods must be called from the thread that owns the connection.

---

## 17. Error Handling

### 17.1 Strategy

finenet uses a combination of return codes and error states rather than
exceptions:

- **MessageReader:** becomes invalid on any read failure. `valid()` returns
  false, and `error()` provides a description. All subsequent reads return
  zero/empty.
- **Connection:** state transitions to `Disconnected` on fatal errors.
  The `onStateChange` handler fires with the new state. An error reason
  is available via `connection.disconnectReason()`.
- **Send failures:** `connection.send()` returns a `SendResult` enum:
  - `Ok` — queued successfully.
  - `ChannelFull` — backpressure; the channel's queue is full.
  - `MessageTooLarge` — exceeds channel's `maxMessageSize`.
  - `NotConnected` — connection is not in the `Connected` state.

### 17.2 Backpressure

When a reliable channel's unacked byte count exceeds `maxUnackedBytes`, the
muxer stops dequeuing from that channel until acks arrive. `send()` will
return `ChannelFull` if the channel's internal queue is also full.

For unreliable channels, if `maxQueuedMessages` is set and the queue exceeds
it, the oldest messages are silently dropped.

---

## 18. Security Considerations

### 18.1 Connection ID Validation

Every datagram carries a `connectionId` that must match the expected value
for the peer. Datagrams with invalid connection IDs are silently dropped.
This prevents stale or misrouted packets from corrupting state.

### 18.2 Encryption (Future)

The initial version does **not** include encryption. The design accommodates
it as a future layer:

- **UDP:** DTLS or a custom symmetric cipher applied after framing, before
  wire send. Keys established during handshake.
- **TCP:** TLS wrapping the TCP stream.

The `Hello` / `HelloAck` handshake can be extended with a key-exchange step
(e.g., X25519 ECDH) without changing the rest of the protocol.

### 18.3 Amplification Prevention

The server does not send large responses to unverified peers. The handshake
requires the client to prove it can receive on its claimed address (by
echoing a server nonce) before the server streams any bulk data.

### 18.4 Sequence Number Validation

Received datagram sequence numbers are validated against a sliding window.
Datagrams with sequences far outside the window (replay or far-future) are
dropped.

---

## 19. Library Structure

```
finenet/
├── include/finenet/
│   ├── finenet.hpp                 // Single-include convenience header
│   ├── message.hpp                 // Message, type ID encoding
│   ├── message_builder.hpp         // MessageBuilder
│   ├── message_reader.hpp          // MessageReader
│   ├── channel.hpp                 // ChannelConfig, ChannelId
│   ├── connection.hpp              // Connection, ConnectionConfig, Stats
│   ├── listener.hpp                // ConnectionListener
│   ├── priority.hpp                // Priority, Reliability, CompressionPolicy
│   ├── compression.hpp             // CompressionPool
│   ├── router.hpp                  // ChannelRouter
│   ├── transport.hpp               // TransportType enum
│   ├── queue.hpp                   // Queue<T> (header-only)
│   ├── keyed_queue.hpp             // KeyedQueue<Key, Data> (header-only)
│   ├── wake_signal.hpp             // WakeSignal (header-only)
│   └── queue_bridge.hpp            // QueueBridge<T, Queue>
├── src/
│   ├── connection.cpp
│   ├── muxer.cpp                   // Priority scheduling, datagram packing
│   ├── demuxer.cpp                 // Datagram unpacking, routing
│   ├── ack_tracker.cpp             // Selective ACK, retransmission, RTT
│   ├── fragment.cpp                // Fragmentation + reassembly
│   ├── ordered_receiver.cpp        // Per-channel reorder buffer
│   ├── compression.cpp             // zstd wrapper + thread pool
│   ├── wire_udp.cpp                // UDP socket layer
│   ├── wire_tcp.cpp                // TCP framing layer
│   ├── wire_format.cpp             // Datagram/frame encode/decode
│   ├── local_transport.cpp         // In-process SPSC queue transport
│   └── router.cpp                  // ChannelRouter
├── tests/
│   ├── test_queue.cpp              // Queue<T> push/pop/drain/alarm/shutdown
│   ├── test_keyed_queue.cpp        // KeyedQueue dedup, merge, ordering
│   ├── test_wake_signal.cpp        // WakeSignal multi-queue coordination
│   ├── test_message_builder.cpp    // Builder/reader round-trip
│   ├── test_channels.cpp           // Reliability, ordering semantics
│   ├── test_fragmentation.cpp      // Fragment/reassemble
│   ├── test_compression.cpp        // Compress/decompress
│   ├── test_muxer.cpp              // Priority scheduling
│   ├── test_ack_tracker.cpp        // Selective ACK, retransmission
│   ├── test_connection.cpp         // End-to-end loopback (network)
│   ├── test_local_transport.cpp    // Local pair, single-player scenario
│   ├── test_mixed_transport.cpp    // Local + network coexistence
│   ├── test_wire_format.cpp        // Encode/decode round-trip
│   └── test_queue_bridge.cpp       // Queue ↔ network round-trip
└── CMakeLists.txt
```

### Dependencies

| Dependency | Purpose | Required? |
|------------|---------|-----------|
| **zstd** | Compression | Yes (can be vendored) |
| **Platform sockets** | UDP/TCP I/O | Yes (POSIX / Winsock) |
| **C++17** | string_view, optional, variant, structured bindings | Yes |
| **A test framework** | Unit tests | Dev only |

No dependency on any game engine, GUI framework, or application code.

**C++17 note:** `std::span` is C++20. Throughout this document, `std::span<T>`
appears in API signatures for clarity. The implementation will use either a
bundled lightweight `span` polyfill (a single header, ~50 lines) or
`const T* data, size_t size` pairs. This is an implementation detail — the
documented signatures show `span` for readability.

---

## 20. Usage Examples

All examples use the same shared setup:

```cpp
#include <finenet/finenet.hpp>
using namespace finenet;

// ── Application message types (not known to finenet) ──
constexpr uint32_t MSG_POSITION = 0x01;
constexpr uint32_t MSG_CHAT     = 0x80'01;

// ── Define channels ──
constexpr ChannelId CH_CONTROL  = 0;
constexpr ChannelId CH_GAME     = 1;
constexpr ChannelId CH_CHAT     = 2;

ConnectionConfig makeChannelConfig() {
    ConnectionConfig cfg;
    cfg.channels = {
        { .id = CH_CONTROL, .name = "control",
          .reliability = Reliability::ReliableOrdered,
          .defaultPriority = Priority::Critical },
        { .id = CH_GAME, .name = "game",
          .reliability = Reliability::Unreliable,
          .defaultPriority = Priority::High },
        { .id = CH_CHAT, .name = "chat",
          .reliability = Reliability::ReliableOrdered,
          .defaultPriority = Priority::Normal },
    };
    return cfg;
}

// Handler registration is the same regardless of transport:
void registerServerHandlers(Connection& conn) {
    conn.onMessage(MSG_POSITION, [](const Message& msg) {
        MessageReader r(msg);
        float x = r.readF32(), y = r.readF32(), z = r.readF32();
        // ... update game state ...
    });
    conn.onMessage(MSG_CHAT, [](const Message& msg) {
        MessageReader r(msg);
        auto text = r.readString();
        // ... broadcast to other clients ...
    });
}
```

### 20.1 Single-Player (Local Transport)

The simplest case. Client and server run in the same process. No sockets
are opened, no network configuration needed.

```cpp
void runSinglePlayer() {
    auto cfg = makeChannelConfig();

    // Create a local connection pair — instant, no handshake.
    auto [clientConn, serverConn] = Connection::createLocalPair(cfg);

    // Server side: register handlers just like it would for a remote client.
    registerServerHandlers(*serverConn);

    // Client side: register handlers just like it would for a remote server.
    clientConn->onMessage(MSG_POSITION, [](const Message& msg) {
        // ... update remote player rendering (other entities from server) ...
    });

    // Game loop — both sides update in the same loop (or on separate threads).
    while (running) {
        // Client sends
        auto msg = MessageBuilder(MSG_POSITION)
            .writeF32(player.x).writeF32(player.y).writeF32(player.z)
            .build();
        clientConn->send(CH_GAME, msg);

        // Both sides process
        clientConn->update();   // Delivers server→client messages
        serverConn->update();   // Delivers client→server messages
        gameServer.tick(dt);
    }
}
```

### 20.2 Single-Player → Open to Network

The player starts a single-player game, then later decides to invite
friends. The local connection for the host player stays untouched.

```cpp
class GameHost {
    ConnectionConfig cfg_ = makeChannelConfig();

    // All connections the server manages — local and remote, uniformly.
    std::vector<std::unique_ptr<Connection>> connections_;

    // The host player's client-side connection.
    std::unique_ptr<Connection> hostClient_;

    // Network listener — null until opened.
    std::unique_ptr<ConnectionListener> listener_;

public:
    // Called at game start.
    void startSinglePlayer() {
        auto [client, server] = Connection::createLocalPair(cfg_);
        registerServerHandlers(*server);
        connections_.push_back(std::move(server));
        hostClient_ = std::move(client);
    }

    // Called when the player clicks "Open to LAN" or "Invite Friends".
    void openToNetwork(uint16_t port) {
        ListenerConfig lcfg;
        lcfg.defaultConnectionConfig = cfg_;
        listener_ = ConnectionListener::listen(port, lcfg);
        // The local connection is unaffected. It keeps running as before.
    }

    // Called when the player clicks "Close to Network".
    void closeToNetwork() {
        // Disconnect all remote clients; keep the local one.
        std::erase_if(connections_, [](const auto& c) {
            if (!c->isLocal()) {
                c->disconnect("Server closed to network");
                return true;
            }
            return false;
        });
        listener_->stop();
        listener_.reset();
    }

    // Server update loop — runs on server thread.
    void update() {
        // Accept remote clients (if open to network).
        if (listener_) {
            while (auto remote = listener_->accept()) {
                registerServerHandlers(*remote);
                connections_.push_back(std::move(remote));
            }
        }

        // Update all connections uniformly — local and remote.
        for (auto& conn : connections_) {
            conn->update();
        }
    }
};
```

### 20.3 Dedicated Server (Network Only)

A headless server with no local player. All connections are network-backed.

```cpp
void runDedicatedServer(uint16_t port) {
    auto cfg = makeChannelConfig();

    ListenerConfig lcfg;
    lcfg.defaultConnectionConfig = cfg;
    auto listener = ConnectionListener::listen(port, lcfg);

    std::vector<std::unique_ptr<Connection>> connections;

    while (running) {
        if (auto conn = listener->accept()) {
            registerServerHandlers(*conn);
            connections.push_back(std::move(conn));
        }

        for (auto& conn : connections) {
            conn->update();
        }

        gameServer.tick(dt);
    }
}
```

### 20.4 Client Connecting to a Remote Server

```cpp
void runRemoteClient(const std::string& host, uint16_t port) {
    auto cfg = makeChannelConfig();
    auto conn = Connection::connect(host, port, cfg);

    conn->onMessage(MSG_POSITION, [](const Message& msg) {
        // ... update remote entity rendering ...
    });

    while (running) {
        conn->update();

        auto msg = MessageBuilder(MSG_POSITION)
            .writeF32(player.x).writeF32(player.y).writeF32(player.z)
            .build();
        conn->send(CH_GAME, msg);
    }
}
```

Note that the client code is identical whether it's connecting to a
dedicated server or to a listen-server hosted by another player. The
client doesn't know or care whether the server has a local connection
for its host player.

---

## 21. Design Decisions and Remaining Questions

### Resolved Decisions

These questions were raised during design and have been resolved:

| # | Question | Decision |
|---|----------|----------|
| 1 | **Encryption** | Defer to a future phase. The handshake and wire format are designed to accommodate an encryption layer (DTLS for UDP, TLS for TCP, key exchange in Hello/HelloAck) without major refactoring. No encryption code ships in v1. |
| 2 | **Congestion control** | **Yes, include it.** Important for slow and high-latency connections. Implement an adaptive congestion control algorithm (BBR-style or AIMD). The fixed bandwidth budget is a ceiling; congestion control operates below it. |
| 3 | **NAT traversal + LAN discovery** | **Yes, include both.** NAT traversal (STUN/TURN-style) is required for internet multiplayer. LAN peer discovery (multicast/broadcast announcement) is required for "open to network" scenarios where players need to find each other on the local network without a matchmaking server. |
| 4 | **Packet coalescing** | Default to **zero** (send immediately). Optimize for latency. See Section 10.4. |
| 5 | **Dictionary compression** | Include dictionaries in the handshake payload if the compression improvement is significant. Don't rely on client-side caching — send the dictionary data during handshake. See Section 9.5. |
| 6 | **Multiple TCP connections** | **Dynamic.** Start with one TCP connection. Open secondary connections only when congestion metrics warrant it. Be conservative — too many connections cause routing congestion. See Section 6.2. |
| 7 | **WebSocket / WebRTC** | Defer to a future phase. The transport abstraction is designed so that a WebSocket backend can be added later without restructuring the library. No WebSocket code ships in v1. |
| 8 | **Connection migration** | Defer to a future phase. Pin for later consideration. The connection-ID-based scheme in the datagram header provides a foundation if this is needed. |
| 9 | **Queue sizing** | Application-dependent. Derive from `ChannelConfig::maxQueuedMessages` for the default. The application can also set capacity directly via `Queue::setCapacity()`. |
| 10 | **Debug mode for local transport** | **Yes, definitely.** `debugSerialization` flag in `ConnectionConfig` forces local connections through the full serialize/compress/decompress/deserialize path. See Section 7.3. |
| 11 | **Type tags in payload** | **No.** Payloads carry no per-field type info. Producer and consumer agree on schema via message type ID. See Section 4.7. |
| 12 | **CBOR support** | **No special support.** CBOR (or any sub-format) is just bytes via `writeBlob()` / `readBlob()`. See Section 4.8. |
| 13 | **C++ standard** | **C++17.** See dependencies in Section 19. |
| 14 | **Nagle's Algorithm** | **Disabled** on all TCP connections (`TCP_NODELAY`). Also disable delayed ACK where possible. See Section 10.4. |

### Open Questions (To Be Resolved During Implementation)

1. **Congestion control algorithm choice.** BBR-style (model-based) vs.
   AIMD (loss-based) vs. hybrid. BBR is more modern but more complex.
   Decision deferred to implementation and testing.

2. **NAT traversal specifics.** Pure STUN (no relay) vs. STUN+TURN (with
   relay fallback). TURN relays add infrastructure cost but handle
   symmetric NATs that STUN alone cannot. Scope depends on deployment
   requirements.

3. **LAN discovery protocol.** Multicast vs. broadcast vs. mDNS/DNS-SD.
   Need to handle: announcement on "open to LAN", response to discovery
   queries, de-announcement on close. Must work on typical consumer
   networks without special configuration.

4. **Compression threshold tuning.** The default 4 KB threshold for async
   vs. sync compression is a guess. Needs benchmarking on representative
   workloads to find the crossover point where queue-hop overhead exceeds
   synchronous compression time.

5. **OS socket buffering.** When compression is async and the network
   thread sends uncompressed messages while waiting for compressed results,
   does the OS socket buffer keep the wire busy enough? Or do we need
   explicit pacing to avoid bursty send patterns? Needs profiling.

---

*finenet is designed to be developed and tested as a standalone library,
independent of any specific application or game engine.*
