#pragma once

/**
 * @file message_builder.hpp
 * @brief Chainable binary serializer for constructing finenet::Message objects
 */

#include "finenet/message.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace finenet {

/**
 * @brief Chainable builder for encoding message payloads
 *
 * Multi-byte primitive types are encoded in little-endian byte order.
 * Strings and byte spans are prefixed with a LEB128 VarInt byte length.
 */
class MessageBuilder {
public:
    explicit MessageBuilder(MessageType type)
        : typeId_(static_cast<uint32_t>(type)) {}

    explicit MessageBuilder(uint32_t typeId)
        : typeId_(typeId) {}

    // ── Primitive fields (little-endian) ──
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

    // ── Variable-length integers (LEB128 / ZigZag LEB128) ──
    MessageBuilder& writeVarInt(uint64_t v);
    MessageBuilder& writeVarSint(int64_t v);

    // ── Length-prefixed compound fields ──
    MessageBuilder& writeString(std::string_view s);
    MessageBuilder& writeBytes(std::span<const uint8_t> data);
    MessageBuilder& writeBytes(const void* data, size_t len);

    // ── Opaque sub-format blobs (CBOR, etc. — wire-identical to writeBytes) ──
    MessageBuilder& writeBlob(std::span<const uint8_t> data) { return writeBytes(data); }
    MessageBuilder& writeCBOR(std::span<const uint8_t> cbor) { return writeBytes(cbor); }

    // ── Raw unprefixed bytes (for batch concatenation) ──
    MessageBuilder& writeRawBytes(std::span<const uint8_t> data);

    // ── Spatial & quantized helpers ──
    MessageBuilder& writeVec3(float x, float y, float z);
    MessageBuilder& writeDVec3(double x, double y, double z);
    MessageBuilder& writeBlockCoord(int32_t x, int32_t y, int32_t z);

    /// 20.12 fixed-point position (3 × int32_t, 1/4096 block precision)
    MessageBuilder& writeQuantizedPos(double x, double y, double z);

    /// 16-bit quantized angle in radians
    MessageBuilder& writeQuantizedAngle(float radians);

    // ── Structured collection headers ──
    MessageBuilder& writeArrayHeader(uint32_t count);
    MessageBuilder& writeMapHeader(uint32_t count);

    // ── Finalize ──
    [[nodiscard]] Message build();

    // ── Query ──
    [[nodiscard]] size_t currentSize() const noexcept { return buffer_.size(); }
    [[nodiscard]] uint32_t typeId() const noexcept { return typeId_; }
    [[nodiscard]] MessageType type() const noexcept { return static_cast<MessageType>(typeId_); }

private:
    uint32_t typeId_ = 0;
    std::vector<uint8_t> buffer_;
};

}  // namespace finenet
