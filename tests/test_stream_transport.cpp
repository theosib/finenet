#include <gtest/gtest.h>
#include "finenet/connection.hpp"
#include "finenet/listener.hpp"
#include "finenet/message_builder.hpp"
#include "finenet/message_reader.hpp"
#include "finenet/wake_signal.hpp"

#include "socket_compat.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace finenet;
using namespace std::chrono_literals;

namespace {

constexpr uint32_t TYPE_A = 0x7001;
constexpr uint32_t TYPE_B = 0x7002;

template <typename Predicate>
bool waitFor(Predicate predicate, std::chrono::milliseconds limit = 5000ms) {
    auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

struct Pair {
    std::unique_ptr<Listener> listener;
    std::unique_ptr<Connection> client;
    std::unique_ptr<Connection> server;
};

Pair connectPair(ConnectionConfig config = {}) {
    Pair pair;
    ListenerConfig listenerConfig;
    listenerConfig.connection = config;
    std::string error;
    pair.listener = Listener::listen(0, listenerConfig, &error);
    EXPECT_NE(pair.listener, nullptr) << error;
    pair.client = Connection::connect("127.0.0.1", pair.listener->port(), config);
    waitFor([&] { return (pair.server = pair.listener->accept()) != nullptr; });
    EXPECT_NE(pair.server, nullptr);
    waitFor([&] { return pair.client->isConnected(); });
    EXPECT_TRUE(pair.client->isConnected());
    return pair;
}

std::vector<uint8_t> pattern(size_t size, uint32_t seed) {
    std::vector<uint8_t> bytes(size);
    std::mt19937 random(seed);
    for (auto& byte : bytes) byte = static_cast<uint8_t>(random());
    return bytes;
}

}  // namespace

TEST(StreamTransportTest, ConnectsAndCarriesMessagesBothWays) {
    Pair p = connectPair();
    EXPECT_EQ(p.client->transportType(), TransportType::Tcp);
    EXPECT_FALSE(p.client->isLocal());
    EXPECT_EQ(p.server->peerAddress().find("127.0.0.1") != std::string::npos ||
                  p.server->peerAddress().find("::ffff:127.0.0.1") != std::string::npos,
              true);

    std::string atServer, atClient;
    p.server->onMessage(TYPE_A, [&](const Message& m) { atServer = std::string(MessageReader(m).readString()); });
    p.client->onMessage(TYPE_B, [&](const Message& m) { atClient = std::string(MessageReader(m).readString()); });

    ASSERT_EQ(p.client->send(Channels::Chat, MessageBuilder(TYPE_A).writeString("hello").build()), SendResult::Ok);
    ASSERT_EQ(p.server->send(Channels::Chat, MessageBuilder(TYPE_B).writeString("world").build()), SendResult::Ok);

    EXPECT_TRUE(waitFor([&] {
        p.server->update();
        p.client->update();
        return atServer == "hello" && atClient == "world";
    }));
}

TEST(StreamTransportTest, LargeMessagesArriveIntact) {
    Pair p = connectPair();
    auto bytes = pattern(5 * 1024 * 1024, 42);

    std::vector<uint8_t> got;
    p.server->onMessage(TYPE_A, [&](const Message& m) {
        auto payload = m.payload();
        got.assign(payload.begin(), payload.end());
    });
    ASSERT_EQ(p.client->send(Channels::Assets, Message(TYPE_A, bytes)), SendResult::Ok);
    EXPECT_TRUE(waitFor([&] {
        p.server->update();
        return !got.empty();
    }, 10000ms));
    EXPECT_EQ(got, bytes);
}

TEST(StreamTransportTest, EachChannelStaysInOrderAcrossInterleaving) {
    Pair p = connectPair();

    std::vector<uint32_t> first, second;
    p.server->onChannel(Channels::World, [&](const Message& m) { first.push_back(MessageReader(m).readU32()); });
    p.server->onChannel(Channels::UI, [&](const Message& m) { second.push_back(MessageReader(m).readU32()); });

    constexpr uint32_t COUNT = 500;
    std::vector<uint8_t> filler(40000);
    for (uint32_t i = 0; i < COUNT; ++i) {
        // Past a frame's size, so these two take turns on the wire
        ASSERT_EQ(p.client->send(Channels::World, MessageBuilder(TYPE_A).writeU32(i).writeBytes(filler).build()),
                  SendResult::Ok);
        ASSERT_EQ(p.client->send(Channels::UI, MessageBuilder(TYPE_B).writeU32(i).writeBytes(filler).build()),
                  SendResult::Ok);
    }

    EXPECT_TRUE(waitFor([&] {
        p.server->update();
        return first.size() == COUNT && second.size() == COUNT;
    }, 15000ms));
    for (uint32_t i = 0; i < first.size(); ++i) EXPECT_EQ(first[i], i);
    for (uint32_t i = 0; i < second.size(); ++i) EXPECT_EQ(second[i], i);
}

TEST(StreamTransportTest, AnUrgentMessageOvertakesABacklog) {
    Pair p = connectPair();

    // Nothing reads the far end, so this stays queued
    std::vector<uint8_t> big(32 * 1024 * 1024);
    ASSERT_EQ(p.client->send(Channels::Assets, Message(TYPE_A, big)), SendResult::Ok);
    ASSERT_TRUE(waitFor([&] { return p.server->stats().bytesReceived >= 0; }));
    ASSERT_EQ(p.client->send(Channels::Input, MessageBuilder(TYPE_B).writeU32(7).build()), SendResult::Ok);

    bool urgent = false, bulk = false, urgentFirst = false;
    p.server->onMessage(TYPE_B, [&](const Message&) {
        urgent = true;
        urgentFirst = !bulk;
    });
    p.server->onMessage(TYPE_A, [&](const Message&) { bulk = true; });

    EXPECT_TRUE(waitFor([&] {
        p.server->update();
        return urgent && bulk;
    }, 15000ms));
    EXPECT_TRUE(urgentFirst);
}

TEST(StreamTransportTest, PendingBytesReportsWhatsQueued) {
    Pair p = connectPair();
    std::vector<uint8_t> big(64 * 1024 * 1024);
    ASSERT_EQ(p.client->send(Channels::Assets, Message(TYPE_A, big)), SendResult::Ok);
    EXPECT_GT(p.client->pendingBytes(Channels::Assets), 0u);
    EXPECT_EQ(p.client->pendingBytes(Channels::Chat), 0u);
}

TEST(StreamTransportTest, ConnectionRefusedEndsDisconnectedWithAReason) {
    // Bound and closed again: a port that's free, and refuses
    uint16_t port;
    {
        auto listener = Listener::listen(0);
        ASSERT_NE(listener, nullptr);
        port = listener->port();
    }
    auto client = Connection::connect("127.0.0.1", port);
    EXPECT_TRUE(waitFor([&] { return client->state() == ConnectionState::Disconnected; }));
    EXPECT_FALSE(client->disconnectReason().empty());
}

TEST(StreamTransportTest, UnknownHostEndsDisconnectedWithAReason) {
    auto client = Connection::connect("no-such-host.invalid", 7810);
    EXPECT_TRUE(waitFor([&] { return client->state() == ConnectionState::Disconnected; }, 15000ms));
    EXPECT_NE(client->disconnectReason().find("no-such-host"), std::string::npos);
}

TEST(StreamTransportTest, ClosingDeliversWhatWasSentAndTheReason) {
    Pair p = connectPair();
    std::string got;
    p.client->onMessage(TYPE_A, [&](const Message& m) { got = std::string(MessageReader(m).readString()); });

    ASSERT_EQ(p.server->send(Channels::Chat, MessageBuilder(TYPE_A).writeString("server full").build()), SendResult::Ok);
    p.server->disconnect("no room");

    EXPECT_TRUE(waitFor([&] {
        p.client->update();
        return !p.client->isConnected() && !got.empty();
    }));
    EXPECT_EQ(got, "server full");
    EXPECT_EQ(p.client->disconnectReason(), "no room");
}

TEST(StreamTransportTest, DestroyingAConnectionTellsThePeer) {
    Pair p = connectPair();
    p.client.reset();
    EXPECT_TRUE(waitFor([&] { return !p.server->isConnected(); }));
}

TEST(StreamTransportTest, SendAfterDisconnectIsRefused) {
    Pair p = connectPair();
    p.client->disconnect("done");
    EXPECT_EQ(p.client->send(Channels::Chat, Message(TYPE_A)), SendResult::NotConnected);
}

TEST(StreamTransportTest, ASilentPeerTimesOut) {
    ListenerConfig config;
    config.connection.connectionTimeoutMs = 300;
    config.connection.keepAliveIntervalMs = 50;
    auto listener = Listener::listen(0, config);
    ASSERT_NE(listener, nullptr);

    // A raw socket that does its half of the handshake, stays open, and never says another word
    int fd = sock::open(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(listener->port());
    ASSERT_EQ(sock::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    constexpr uint8_t preamble[8] = {'F', 'N', 'E', 'T', 1, 0, 0, 0};
    ASSERT_EQ(sock::send(fd, preamble, 8, 0), 8);

    std::unique_ptr<Connection> server;
    ASSERT_TRUE(waitFor([&] { return (server = listener->accept()) != nullptr; }));
    EXPECT_TRUE(waitFor([&] { return !server->isConnected(); }, 2000ms));
    EXPECT_EQ(server->disconnectReason(), "timed out");
    sock::close(fd);
}

TEST(StreamTransportTest, KeepAliveHoldsAQuietConnectionOpenAndMeasuresRtt) {
    ConnectionConfig config;
    config.connectionTimeoutMs = 400;
    config.keepAliveIntervalMs = 50;
    Pair p = connectPair(config);

    std::this_thread::sleep_for(800ms);
    EXPECT_TRUE(p.client->isConnected());
    EXPECT_TRUE(p.server->isConnected());
    EXPECT_GT(p.client->stats().rttMs, 0.0f);
}

TEST(StreamTransportTest, AStrangerSpeakingAnotherProtocolIsDropped) {
    auto listener = Listener::listen(0);
    ASSERT_NE(listener, nullptr);

    int fd = sock::open(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(listener->port());
    ASSERT_EQ(sock::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    const char* request = "GET / HTTP/1.1\r\n\r\n";
    ASSERT_GT(sock::send(fd, request, strlen(request), 0), 0);

    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(listener->accept(), nullptr);
    sock::close(fd);
}

TEST(StreamTransportTest, AFrameTooLongIsRefused) {
    auto listener = Listener::listen(0);
    ASSERT_NE(listener, nullptr);

    int fd = sock::open(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(listener->port());
    ASSERT_EQ(sock::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    constexpr uint8_t hello[] = {'F', 'N', 'E', 'T', 1, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0x7F, 1, 0};
    ASSERT_EQ(sock::send(fd, hello, sizeof(hello), 0), static_cast<ssize_t>(sizeof(hello)));

    // It arrives with the preamble, so this connection never gets as far as being accepted
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(listener->accept(), nullptr);
    sock::close(fd);
}

TEST(StreamTransportTest, AMessagePastTheLimitIsRefused) {
    ConnectionConfig config;
    config.maxMessageBytes = 100 * 1024;
    Pair p = connectPair(config);
    ASSERT_EQ(p.client->send(Channels::Assets, Message(TYPE_A, std::vector<uint8_t>(1024 * 1024))), SendResult::Ok);
    EXPECT_TRUE(waitFor([&] { return !p.server->isConnected(); }));
    EXPECT_NE(p.server->disconnectReason().find("too large"), std::string::npos);
}

TEST(StreamTransportTest, APortInUseSaysSo) {
    auto first = Listener::listen(0);
    ASSERT_NE(first, nullptr);
    std::string error;
    auto second = Listener::listen(first->port(), {}, &error);
    EXPECT_EQ(second, nullptr);
    EXPECT_FALSE(error.empty());
}

TEST(StreamTransportTest, ManyClientsAtOnce) {
    auto listener = Listener::listen(0);
    ASSERT_NE(listener, nullptr);

    constexpr int CLIENTS = 16;
    std::vector<std::unique_ptr<Connection>> clients, servers;
    for (int i = 0; i < CLIENTS; ++i) clients.push_back(Connection::connect("127.0.0.1", listener->port()));
    ASSERT_TRUE(waitFor([&] {
        while (auto server = listener->accept()) servers.push_back(std::move(server));
        return servers.size() == CLIENTS;
    }));
    ASSERT_TRUE(waitFor([&] {
        for (auto& client : clients) {
            if (!client->isConnected()) return false;
        }
        return true;
    }));

    std::atomic<int> arrived{0};
    for (auto& server : servers) server->onMessage(TYPE_A, [&](const Message&) { ++arrived; });
    for (auto& client : clients) {
        for (int i = 0; i < 100; ++i) client->send(Channels::Chat, Message(TYPE_A));
    }
    EXPECT_TRUE(waitFor([&] {
        for (auto& server : servers) server->update();
        return arrived == CLIENTS * 100;
    }));
}

TEST(StreamTransportTest, WakeSignalFiresWhenAMessageArrives) {
    Pair p = connectPair();
    WakeSignal signal;
    p.server->setWakeSignal(&signal);

    std::thread sender([&] {
        std::this_thread::sleep_for(50ms);
        p.client->send(Channels::Chat, Message(TYPE_A));
    });
    auto started = std::chrono::steady_clock::now();
    bool woke = false;
    while (std::chrono::steady_clock::now() - started < 2s) {
        signal.waitFor(2000ms);
        if (!p.server->inboundQueue(Channels::Chat).empty()) {
            woke = true;
            break;
        }
    }
    sender.join();
    p.server->setWakeSignal(nullptr);
    EXPECT_TRUE(woke);
    EXPECT_LT(std::chrono::steady_clock::now() - started, 1s);
}

TEST(LocalPendingBytesTest, ReportsWhatThePeerHasntTaken) {
    auto [client, server] = Connection::createLocalPair();
    EXPECT_EQ(client->pendingBytes(), 0u);
    client->send(Channels::Chat, Message(TYPE_A, std::vector<uint8_t>(1000)));
    EXPECT_GE(client->pendingBytes(), 1000u);
    EXPECT_GE(client->pendingBytes(Channels::Chat), 1000u);
    EXPECT_EQ(client->pendingBytes(Channels::World), 0u);
    server->update();
    EXPECT_EQ(client->pendingBytes(), 0u);
}
