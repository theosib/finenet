# finenet — Status

What's built, and what's wrong with it. [DESIGN.md](DESIGN.md) is the larger plan, and much of it
isn't built. The wire format in use is specified in FineStructureVoxel's
`docs/protocol/PROTOCOL.md` §2 and §9.

## Built

| Part | State |
|---|---|
| Queues, keyed queues, wake signals | Done |
| Messages, builder, reader | Done |
| Local transport | Done |
| TCP transport | Framing, priorities, big messages split so they can't block small ones, keepalive, reasons on close |
| `Listener` | Dual-stack, with a limit on handshakes in progress |
| LAN discovery | Beacon and browser |
| Endpoint parsing | Done |

## Not tested

These passed nothing because nothing has tried them.

- **Two machines.** Every run so far has been on one Mac. Discovery was heard on the real interface,
  but from itself. A second machine on a real LAN is the test that matters most: see Keystone's
  `docs/PLATFORMS.md` for the steps, and put the result here.
- **The /24 sweep.** `DiscoveryBrowser`'s `sweepSome` has never run in a test: `test_discovery.cpp`
  sets `sweep = false`, and on a working network the browser finds something before it starts.
- **`blocked()`.** macOS denying Local Network access has never happened here, so the `EHOSTUNREACH`
  path is untested. The test only checks that it stays false.
- **Linux and Windows.** Only macOS has ever built this.

## Problems

- **POSIX only.** `src/stream_transport.cpp`, `src/listener.cpp`, `src/discovery.cpp` and two test
  files won't build on Windows. It needs Winsock start-up, `closesocket`, `WSAPoll`, `ioctlsocket` in
  place of `fcntl`, something other than `pipe()` to wake the I/O thread (a loopback socket pair),
  and `GetAdaptersAddresses` in place of `getifaddrs`.
- **`SO_REUSEPORT` may differ on Linux.** Several processes share the discovery port. On Linux a
  unicast datagram to a shared port goes to just one socket, chosen by hash, which may be a browser's
  and not a beacon's. That could lose direct probes, and loopback queries with two servers on one
  machine. Group sends reach everyone. `FindsEveryServerOnOneMachine` and
  `ADirectProbeFindsAServer` are the tests to watch.
- **No UDP.** `Connection::connect` is always TCP, so packet loss on the internet stalls every
  channel for a moment. `ConnectionConfig::transport`, `preferUdp`, `mtu`, `maxRetransmissions` and
  `coalescingWindowMs` do nothing yet, and nor does a channel's `Reliability` on the wire.
- **No compression.** `ChannelConfig::compression` and `compressionThreads` are ignored. The engine
  compresses what it needs itself.
- **No encryption.** Everything's in the clear, including anything the application treats as a
  password. This has to come before any use on the internet.
- **No WebSocket.** The framing leaves room for it (PROTOCOL.md §2.3) and `StreamTransport` is the
  seam, but it opens its own socket, so it'd need splitting into framing and a byte stream first.
- **A thread per connection.** Fine for a game, not for a hub with thousands of them.
- **`peerAddress` shows mapped addresses.** An IPv4 peer on the dual-stack listener reads as
  `::ffff:127.0.0.1`.
- **`DESIGN.md` §6.2 is out of date** on TCP framing, and §13's `Listener` API isn't what was built.
