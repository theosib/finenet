#include <gtest/gtest.h>
#include "finenet/connection.hpp"
#include "finenet/message_builder.hpp"
#include "finenet/message_reader.hpp"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace finenet;

TEST(ConnectionTest, CreateLocalPairBidirectionalSendAndReceive) {
    auto [client, server] = Connection::createLocalPair();

    ASSERT_NE(client, nullptr);
    ASSERT_NE(server, nullptr);
    EXPECT_TRUE(client->isConnected());
    EXPECT_TRUE(server->isConnected());
    EXPECT_TRUE(client->isLocal());
    EXPECT_TRUE(server->isLocal());

    int32_t serverReceivedX = 0, serverReceivedY = 0, serverReceivedZ = 0;
    std::string clientReceivedChat;

    server->onMessage(MessageType::PlayerAction, [&](const Message& msg) {
        MessageReader r(msg);
        r.readBlockCoord(serverReceivedX, serverReceivedY, serverReceivedZ);
    });

    client->onMessage(MessageType::ChatMessage, [&](const Message& msg) {
        MessageReader r(msg);
        clientReceivedChat = std::string(r.readString());
    });

    EXPECT_EQ(client->send(Channels::Actions,
        MessageBuilder(MessageType::PlayerAction)
            .writeBlockCoord(10, 64, -20)
            .build()), SendResult::Ok);

    EXPECT_EQ(server->send(Channels::Chat,
        MessageBuilder(MessageType::ChatMessage)
            .writeString("Welcome to Shattered Lands")
            .build()), SendResult::Ok);

    server->update();
    client->update();

    EXPECT_EQ(serverReceivedX, 10);
    EXPECT_EQ(serverReceivedY, 64);
    EXPECT_EQ(serverReceivedZ, -20);
    EXPECT_EQ(clientReceivedChat, "Welcome to Shattered Lands");

    auto clientStats = client->stats();
    auto serverStats = server->stats();
    EXPECT_EQ(clientStats.packetsSent, 1u);
    EXPECT_EQ(clientStats.packetsReceived, 1u);
    EXPECT_EQ(serverStats.packetsSent, 1u);
    EXPECT_EQ(serverStats.packetsReceived, 1u);
}

TEST(ConnectionTest, PriorityOrderingOnUpdate) {
    auto [client, server] = Connection::createLocalPair();

    std::vector<uint32_t> deliveryOrder;
    server->onMessage(MessageType::SoundEvent, [&](const Message& msg) {
        MessageReader r(msg);
        deliveryOrder.push_back(r.readU32());
    });

    // Send messages from lowest urgency to highest urgency before server->update()
    client->send(Channels::Assets,
        MessageBuilder(MessageType::SoundEvent).writeU32(50).build()); // Bulk (4)
    client->send(Channels::World,
        MessageBuilder(MessageType::SoundEvent).writeU32(40).build()); // Low (3)
    client->send(Channels::Effects,
        MessageBuilder(MessageType::SoundEvent).writeU32(30).build()); // Normal (2)
    client->send(Channels::Actions,
        MessageBuilder(MessageType::SoundEvent).writeU32(20).build()); // High (1)
    client->send(Channels::Input,
        MessageBuilder(MessageType::SoundEvent).writeU32(10).build()); // Critical (0)

    // Also test per-message priority override: send on World (Low) but overridden to Critical (0)
    client->send(Channels::World,
        MessageBuilder(MessageType::SoundEvent).writeU32(15).build(),
        Priority::Critical);

    server->update();

    // Expected order: Critical (10, then 15 by arrival order), High (20), Normal (30), Low (40), Bulk (50)
    const std::vector<uint32_t> expected = {10, 15, 20, 30, 40, 50};
    EXPECT_EQ(deliveryOrder, expected);
}

TEST(ConnectionTest, UnreliableMaxQueuedMessagesDropsOldest) {
    ConnectionConfig cfg;
    ChannelConfig unreliableCh = Channels::Effects;
    unreliableCh.maxQueuedMessages = 3;
    cfg.channels = {unreliableCh};

    auto [client, server] = Connection::createLocalPair(cfg);

    for (uint32_t i = 1; i <= 5; ++i) {
        EXPECT_EQ(client->send(Channels::Effects,
            MessageBuilder(MessageType::SoundEvent).writeU32(i).build()), SendResult::Ok);
    }

    std::vector<uint32_t> received;
    server->onChannel(Channels::Effects, [&](const Message& msg) {
        MessageReader r(msg);
        received.push_back(r.readU32());
    });

    server->update();

    // Oldest 2 messages (1, 2) should have been dropped; 3, 4, 5 delivered
    const std::vector<uint32_t> expected = {3, 4, 5};
    EXPECT_EQ(received, expected);
    EXPECT_EQ(server->stats().perChannel[Channels::Effects.id].droppedMessages, 2u);
}

TEST(ConnectionTest, DebugSerializationModeRoundTripsWireBytes) {
    ConnectionConfig cfg;
    cfg.debugSerialization = true;

    auto [client, server] = Connection::createLocalPair(cfg);

    bool received = false;
    server->onMessage(MessageType::ChunkData, [&](const Message& msg) {
        EXPECT_EQ(msg.channel(), Channels::World.id);
        EXPECT_EQ(msg.priority(), Priority::Low);
        EXPECT_TRUE(msg.isReliable());
        EXPECT_TRUE(msg.isOrdered());

        MessageReader r(msg);
        int32_t x = 0, y = 0, z = 0;
        r.readBlockCoord(x, y, z);
        EXPECT_EQ(x, -3);
        EXPECT_EQ(y, 2);
        EXPECT_EQ(z, 9);
        EXPECT_EQ(r.readString(), "wire_round_trip");
        EXPECT_TRUE(r.valid());
        received = true;
    });

    EXPECT_EQ(client->send(Channels::World,
        MessageBuilder(MessageType::ChunkData)
            .writeBlockCoord(-3, 2, 9)
            .writeString("wire_round_trip")
            .build()), SendResult::Ok);

    server->update();
    EXPECT_TRUE(received);
}

TEST(ConnectionTest, InboundQueueWithWakeSignal) {
    auto [client, server] = Connection::createLocalPair();

    WakeSignal wake;
    server->inboundQueue(Channels::Input).attach(&wake);

    std::atomic<uint32_t> receivedVal{0};
    std::thread serverThread([&]() {
        wake.wait();
        if (auto msg = server->inboundQueue(Channels::Input).tryPop()) {
            MessageReader r(*msg);
            receivedVal = r.readU32();
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    client->send(Channels::Input,
        MessageBuilder(MessageType::PlayerInput).writeU32(777).build());

    serverThread.join();
    EXPECT_EQ(receivedVal.load(), 777u);
}

TEST(ConnectionTest, DisconnectNotifiesBothPeersAndRejectsFurtherSends) {
    auto [client, server] = Connection::createLocalPair();

    ConnectionState clientNewState = ConnectionState::Connected;
    ConnectionState serverNewState = ConnectionState::Connected;

    client->onStateChange([&](ConnectionState s) { clientNewState = s; });
    server->onStateChange([&](ConnectionState s) { serverNewState = s; });

    client->disconnect("Player quit");

    EXPECT_FALSE(client->isConnected());
    EXPECT_FALSE(server->isConnected());
    EXPECT_EQ(clientNewState, ConnectionState::Disconnected);
    EXPECT_EQ(serverNewState, ConnectionState::Disconnected);
    EXPECT_EQ(server->disconnectReason(), "Player quit");

    EXPECT_EQ(client->send(Channels::Control,
        MessageBuilder(MessageType::KeepAlive).build()), SendResult::NotConnected);
    EXPECT_EQ(server->send(Channels::Control,
        MessageBuilder(MessageType::KeepAlive).build()), SendResult::NotConnected);
}
