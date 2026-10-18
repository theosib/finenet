#include <gtest/gtest.h>
#include "finenet/finenet.hpp"

#include <string>
#include <vector>

using namespace finenet;

namespace {

struct SoundEvt {
    uint32_t soundId = 0;
    float x = 0.0f;
    float y = 0.0f;
    float z = 0.0f;
    float volume = 1.0f;
    bool positional = true;
};

}  // namespace

TEST(QueueBridgeTest, SingleItemFlushAndReceiveRoundTrip) {
    ConnectionConfig cfg;
    cfg.debugSerialization = true;
    auto [client, server] = Connection::createLocalPair(cfg);

    Queue<SoundEvt> serverOutQueue;
    Queue<SoundEvt> clientInQueue;

    QueueBridge<SoundEvt> serverBridge(
        serverOutQueue, *server, Channels::Effects, MessageType::SoundEvent);
    serverBridge.setEncoder([](const SoundEvt& ev) {
        return MessageBuilder(MessageType::SoundEvent)
            .writeU32(ev.soundId)
            .writeVec3(ev.x, ev.y, ev.z)
            .writeF32(ev.volume)
            .writeBool(ev.positional)
            .build();
    });

    QueueBridge<SoundEvt> clientBridge(
        clientInQueue, *client, Channels::Effects, MessageType::SoundEvent);
    clientBridge.setDecoder([](MessageReader& r) {
        SoundEvt ev;
        ev.soundId = r.readU32();
        r.readVec3(ev.x, ev.y, ev.z);
        ev.volume = r.readF32();
        ev.positional = r.readBool();
        return ev;
    });

    serverOutQueue.push(SoundEvt{101, 1.0f, 2.0f, 3.0f, 0.8f, true});
    serverOutQueue.push(SoundEvt{202, -4.0f, 5.0f, 6.0f, 0.5f, false});

    EXPECT_EQ(serverBridge.flushToNetwork(), 2u);
    EXPECT_TRUE(serverOutQueue.empty());

    client->update();

    auto received = clientInQueue.drainAll();
    ASSERT_EQ(received.size(), 2u);

    EXPECT_EQ(received[0].soundId, 101u);
    EXPECT_FLOAT_EQ(received[0].x, 1.0f);
    EXPECT_FLOAT_EQ(received[0].y, 2.0f);
    EXPECT_FLOAT_EQ(received[0].z, 3.0f);
    EXPECT_FLOAT_EQ(received[0].volume, 0.8f);
    EXPECT_TRUE(received[0].positional);

    EXPECT_EQ(received[1].soundId, 202u);
    EXPECT_FLOAT_EQ(received[1].x, -4.0f);
    EXPECT_FLOAT_EQ(received[1].y, 5.0f);
    EXPECT_FLOAT_EQ(received[1].z, 6.0f);
    EXPECT_FLOAT_EQ(received[1].volume, 0.5f);
    EXPECT_FALSE(received[1].positional);
}

TEST(QueueBridgeTest, BatchModePacksMultipleItemsIntoFewerMessages) {
    ConnectionConfig cfg;
    cfg.debugSerialization = true;
    auto [client, server] = Connection::createLocalPair(cfg);

    Queue<uint32_t> sendQueue;
    Queue<uint32_t> recvQueue;

    QueueBridge<uint32_t> sendBridge(
        sendQueue, *server, Channels::Entities, MessageType::EntityBatch);
    sendBridge.setBatchMode(true, 3); // Max 3 items per batch message
    sendBridge.setEncoder([](const uint32_t& v) {
        return MessageBuilder(MessageType::EntityPosition)
            .writeU32(v)
            .build();
    });

    QueueBridge<uint32_t> recvBridge(
        recvQueue, *client, Channels::Entities, MessageType::EntityBatch);
    recvBridge.setDecoder([](MessageReader& r) {
        return r.readU32();
    });

    for (uint32_t i = 1; i <= 7; ++i) {
        sendQueue.push(i * 10);
    }

    EXPECT_EQ(sendBridge.flushToNetwork(), 7u);
    // 7 items with maxBatchSize=3 should send 3 messages (3 + 3 + 1)
    EXPECT_EQ(server->stats().packetsSent, 3u);

    client->update();

    auto items = recvQueue.drainAll();
    const std::vector<uint32_t> expected = {10, 20, 30, 40, 50, 60, 70};
    EXPECT_EQ(items, expected);
}
