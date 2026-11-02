# finenet

A C++20 library for passing structured messages between a client and a server. It knows nothing
about what's in them: you define the message types and channels, and it delivers them in priority
order. It carries all the multiplayer in the
[FineStructureVoxel](https://github.com/theosib/FineStructureVoxel) engine, and so in
[Keystone](https://github.com/theosib/Keystone-RTS).

## The same API in one process and across the world

Its central idea is that **a game doesn't need a separate single-player path**. A `Connection` is
the same class whether the other end is on another continent or on another thread in this process:

```cpp
#include <finenet/finenet.hpp>
using namespace finenet;

auto [client, server] = Connection::createLocalPair();   // Or Connection::connect(host, port)

server->onMessage(MessageType::ChatMessage, [](const Message& message) {
    MessageReader reader(message);
    std::cout << reader.readString() << "\n";
});

client->send(Channels::Chat, MessageBuilder(MessageType::ChatMessage).writeString("Hello").build());
server->update();
```

In-process, messages go through queues, with no sockets involved. So single player is just a server
with nobody else connected, a game can open itself to the network while it's running, and the code
you test alone is the code your players use together.

## What it does

- **Channels and priorities.** Each channel has its own policy, and there are five priorities, from
  `Critical` for player input to `Bulk` for downloads. Traffic that matters is never held up behind
  traffic that doesn't.
- **Big messages can't block small ones.** Large messages are split on the wire, so a chat line can
  overtake a chunk of world data on the same connection.
- **Type-safe messages with no code generation.** A chainable `MessageBuilder` and a matching
  `MessageReader`, with no schema compiler and no generated files.
- **LAN discovery.** `DiscoveryBeacon` announces a game, and `DiscoveryBrowser` finds games with no
  address typed in, by multicast and broadcast, and with a direct sweep of the subnet where those are
  blocked. Games on one machine find each other too.
- **Backpressure you can see.** A sender can ask how much is still on its way, which is what lets a
  game send the latest state rather than a growing queue of stale state.
- **Robust connections.** Dual-stack listening, a limit on handshakes in progress, keepalive, and a
  reason given when a connection closes.
- **Concurrency parts you can use by themselves.** Wake signals, queues, keyed queues that keep one
  entry per key, and a bridge that carries a queue across a connection in batches.
- **Portable.** macOS, Linux and Windows, with Winsock's differences kept in one file.

## What it doesn't do yet

The [design](docs/DESIGN.md) goes further than the code, and [docs/STATUS.md](docs/STATUS.md) says
exactly how far. The main gaps:

- **TCP only.** The design is UDP-first, with selective reliability. That isn't built, so packet
  loss stalls a connection for a moment.
- **No encryption.** Everything's in the clear, passwords included. Use it on a LAN, or tunnel it.
- **No compression in the library.** An application can compress what it sends itself.
- **A thread per connection.** Fine for a game, not for a hub with thousands of clients.

## Building

No other projects are needed. CMake fetches googletest for the tests.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/finenet_tests
```

## Documentation

| Document | Covers |
|---|---|
| [docs/STATUS.md](docs/STATUS.md) | What's built, what's untested, and what's wrong |
| [docs/DESIGN.md](docs/DESIGN.md) | The full design, much of it still to come |
| [PROTOCOL.md](https://github.com/theosib/FineStructureVoxel/blob/main/docs/protocol/PROTOCOL.md) | The wire format, §2 and §9, in the engine's repo |

## License

Apache License 2.0: see [LICENSE](LICENSE) and [NOTICE](NOTICE).
