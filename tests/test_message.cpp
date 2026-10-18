#include <gtest/gtest.h>
#include "finenet/message.hpp"
#include "finenet/message_builder.hpp"
#include "finenet/message_reader.hpp"

#include <cmath>
#include <cstdint>
#include <limits>
#include <numbers>
#include <string>
#include <vector>

using namespace finenet;

TEST(MessageTest, MessageTypePrefixEncodingRanges) {
    // 1-byte range: 0x00 - 0x7F
    EXPECT_EQ(encodedMessageTypeSize(0x00u), 1u);
    EXPECT_EQ(encodedMessageTypeSize(0x7Fu), 1u);
    EXPECT_EQ(encodedMessageTypeSize(MessageType::PlayerInput), 1u);
    EXPECT_EQ(encodedMessageTypeSize(MessageType::VitalStats), 1u);

    // 2-byte range: 0x80 - 0x3FFF
    EXPECT_EQ(encodedMessageTypeSize(0x80u), 2u);
    EXPECT_EQ(encodedMessageTypeSize(0x3FFFu), 2u);

    // 3-byte range: 0x4000 - 0x1FFFFF
    EXPECT_EQ(encodedMessageTypeSize(0x4000u), 3u);
    EXPECT_EQ(encodedMessageTypeSize(0x8001u), 3u);
    EXPECT_EQ(encodedMessageTypeSize(0x1FFFFFu), 3u);

    // 4-byte range: > 0x1FFFFF
    EXPECT_EQ(encodedMessageTypeSize(0x200000u), 4u);
    EXPECT_EQ(encodedMessageTypeSize(MessageType::ClientHello), 4u);
    EXPECT_EQ(encodedMessageTypeSize(MessageType::ScriptBundle), 4u);

    const std::vector<uint32_t> testIds = {
        0x00u, 0x01u, 0x12u, 0x7Fu,
        0x80u, 0x123u, 0x3FFFu,
        0x4000u, 0x8001u, 0x802Cu, 0x1FFFFFu,
        0x200000u, 0xC00001u, 0xC00012u, 0x1FFFFFFFu
    };

    std::vector<uint8_t> buf;
    for (uint32_t id : testIds) {
        size_t written = encodeMessageType(id, buf);
        EXPECT_EQ(written, encodedMessageTypeSize(id));
    }

    size_t offset = 0;
    for (uint32_t expected : testIds) {
        uint32_t decoded = 0;
        ASSERT_TRUE(decodeMessageType(buf, offset, decoded));
        EXPECT_EQ(decoded, expected);
    }
    EXPECT_EQ(offset, buf.size());
}

TEST(MessageTest, PrimitivesRoundTrip) {
    Message msg = MessageBuilder(MessageType::PlayerAction)
        .writeU8(0xABu)
        .writeU16(0x1234u)
        .writeU32(0x89ABCDEFu)
        .writeU64(0x0123456789ABCDEFull)
        .writeI8(-42)
        .writeI16(-12345)
        .writeI32(-1000000)
        .writeI64(-9000000000000000000ll)
        .writeF32(3.1415926f)
        .writeF64(-2.718281828459045)
        .writeBool(true)
        .writeBool(false)
        .build();

    EXPECT_EQ(msg.type(), MessageType::PlayerAction);

    MessageReader r(msg);
    EXPECT_EQ(r.type(), MessageType::PlayerAction);
    EXPECT_TRUE(r.valid());
    EXPECT_TRUE(r.hasMore());

    EXPECT_EQ(r.readU8(), 0xABu);
    EXPECT_EQ(r.readU16(), 0x1234u);
    EXPECT_EQ(r.readU32(), 0x89ABCDEFu);
    EXPECT_EQ(r.readU64(), 0x0123456789ABCDEFull);
    EXPECT_EQ(r.readI8(), -42);
    EXPECT_EQ(r.readI16(), -12345);
    EXPECT_EQ(r.readI32(), -1000000);
    EXPECT_EQ(r.readI64(), -9000000000000000000ll);
    EXPECT_FLOAT_EQ(r.readF32(), 3.1415926f);
    EXPECT_DOUBLE_EQ(r.readF64(), -2.718281828459045);
    EXPECT_TRUE(r.readBool());
    EXPECT_FALSE(r.readBool());

    EXPECT_TRUE(r.valid());
    EXPECT_FALSE(r.hasMore());
    EXPECT_EQ(r.remaining(), 0u);
}

TEST(MessageTest, CompoundAndSpatialFieldsRoundTrip) {
    const std::vector<uint8_t> rawBytes = {0xDE, 0xAD, 0xBE, 0xEF};
    const std::vector<uint8_t> cborData = {0xA1, 0x61, 0x6B, 0x01};

    Message msg = MessageBuilder(MessageType::EntityPosition)
        .writeString("finevox:stone_bricks")
        .writeBytes(rawBytes)
        .writeCBOR(cborData)
        .writeVec3(1.5f, -2.25f, 100.0f)
        .writeDVec3(100000.125, -64.5, 250000.75)
        .writeBlockCoord(-120, 64, 9999)
        .writeQuantizedPos(1234.5625, -45.25, 8192.125)
        .writeQuantizedAngle(1.234f)
        .writeQuantizedAngle(-1.05f)
        .writeVarInt(0u)
        .writeVarInt(127u)
        .writeVarInt(16384u)
        .writeVarInt(0xFFFFFFFFFFFFull)
        .writeVarSint(-123456789ll)
        .writeArrayHeader(3)
        .writeMapHeader(2)
        .build();

    MessageReader r(msg);
    EXPECT_EQ(r.readString(), "finevox:stone_bricks");

    auto b = r.readBytes();
    ASSERT_EQ(b.size(), rawBytes.size());
    EXPECT_EQ(std::vector<uint8_t>(b.begin(), b.end()), rawBytes);

    auto cbor = r.readCBOR();
    ASSERT_EQ(cbor.size(), cborData.size());
    EXPECT_EQ(std::vector<uint8_t>(cbor.begin(), cbor.end()), cborData);

    float vx = 0, vy = 0, vz = 0;
    r.readVec3(vx, vy, vz);
    EXPECT_FLOAT_EQ(vx, 1.5f);
    EXPECT_FLOAT_EQ(vy, -2.25f);
    EXPECT_FLOAT_EQ(vz, 100.0f);

    double dx = 0, dy = 0, dz = 0;
    r.readDVec3(dx, dy, dz);
    EXPECT_DOUBLE_EQ(dx, 100000.125);
    EXPECT_DOUBLE_EQ(dy, -64.5);
    EXPECT_DOUBLE_EQ(dz, 250000.75);

    int32_t bx = 0, by = 0, bz = 0;
    r.readBlockCoord(bx, by, bz);
    EXPECT_EQ(bx, -120);
    EXPECT_EQ(by, 64);
    EXPECT_EQ(bz, 9999);

    double qx = 0, qy = 0, qz = 0;
    r.readQuantizedPos(qx, qy, qz);
    EXPECT_NEAR(qx, 1234.5625, 1.0 / 4096.0);
    EXPECT_NEAR(qy, -45.25, 1.0 / 4096.0);
    EXPECT_NEAR(qz, 8192.125, 1.0 / 4096.0);

    EXPECT_NEAR(r.readQuantizedAngle(), 1.234f, 1e-3f);
    EXPECT_NEAR(r.readQuantizedAngle(), -1.05f, 1e-3f);

    EXPECT_EQ(r.readVarInt(), 0u);
    EXPECT_EQ(r.readVarInt(), 127u);
    EXPECT_EQ(r.readVarInt(), 16384u);
    EXPECT_EQ(r.readVarInt(), 0xFFFFFFFFFFFFull);
    EXPECT_EQ(r.readVarSint(), -123456789ll);
    EXPECT_EQ(r.readArrayHeader(), 3u);
    EXPECT_EQ(r.readMapHeader(), 2u);

    EXPECT_TRUE(r.valid());
    EXPECT_FALSE(r.hasMore());
}

TEST(MessageTest, ReaderBoundsCheckAndSkip) {
    Message msg = MessageBuilder(MessageType::KeepAlive)
        .writeU32(0x11223344u)
        .writeU16(0x5566u)
        .build();

    MessageReader r(msg);
    r.skip(4);
    EXPECT_EQ(r.remaining(), 2u);
    EXPECT_EQ(r.readU16(), 0x5566u);
    EXPECT_TRUE(r.valid());

    // Reading past the end must mark reader invalid and populate error()
    EXPECT_EQ(r.readU8(), 0u);
    EXPECT_FALSE(r.valid());
    EXPECT_FALSE(r.error().empty());

    // Subsequent reads stay invalid and return defaults
    EXPECT_EQ(r.readU32(), 0u);
    EXPECT_FALSE(r.valid());
}

TEST(MessageTest, WireBytesFramingRoundTrip) {
    Message orig = MessageBuilder(MessageType::ChunkData)
        .writeBlockCoord(4, -1, 12)
        .writeString("chunk_payload")
        .build();
    orig.setChannel(Channels::World.id);
    orig.setPriority(Priority::Low);
    orig.setSequenceNumber(77);
    orig.setReliable(true);
    orig.setOrdered(true);

    std::vector<uint8_t> wire = orig.toWireBytes();
    auto decoded = Message::fromWireBytes(wire);
    ASSERT_TRUE(decoded.has_value());

    EXPECT_EQ(decoded->type(), MessageType::ChunkData);
    EXPECT_EQ(decoded->channel(), Channels::World.id);
    EXPECT_EQ(decoded->priority(), Priority::Low);
    EXPECT_EQ(decoded->sequenceNumber(), 77u);
    EXPECT_TRUE(decoded->isReliable());
    EXPECT_TRUE(decoded->isOrdered());

    MessageReader r(*decoded);
    int32_t x = 0, y = 0, z = 0;
    r.readBlockCoord(x, y, z);
    EXPECT_EQ(x, 4);
    EXPECT_EQ(y, -1);
    EXPECT_EQ(z, 12);
    EXPECT_EQ(r.readString(), "chunk_payload");
    EXPECT_TRUE(r.valid());

    // Truncated wire bytes should fail cleanly
    std::span<const uint8_t> truncated(wire.data(), wire.size() - 2);
    EXPECT_FALSE(Message::fromWireBytes(truncated).has_value());
}
