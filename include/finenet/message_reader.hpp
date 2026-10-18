#pragma once

/**
 * @file message_reader.hpp
 * @brief Bounds-checked binary decoder mirroring MessageBuilder
 */

#include "finenet/message.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace finenet {

/**
 * @brief Sequential bounds-checked decoder for Message payloads
 *
 * Once any read fails (buffer underflow or invalid encoding), valid() becomes
 * false, error() holds a descriptive message, and all subsequent reads fail
 * safely by returning zero/empty values.
 */
class MessageReader {
public:
    explicit MessageReader(const Message& msg)
        : typeId_(msg.typeId()), data_(msg.payload()) {}

    explicit MessageReader(std::span<const uint8_t> data, uint32_t typeId = 0)
        : typeId_(typeId), data_(data) {}

    // ── Type identification ──
    [[nodiscard]] MessageType type() const noexcept {
        return static_cast<MessageType>(typeId_);
    }

    [[nodiscard]] uint32_t typeId() const noexcept {
        return typeId_;
    }

    // ── Primitive reads ──
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

    // ── Variable-length integers ──
    uint64_t readVarInt();
    int64_t  readVarSint();

    // ── Compound reads (zero-copy views into payload buffer) ──
    std::string_view readString();
    std::span<const uint8_t> readBytes();

    // ── Opaque sub-format blobs ──
    std::span<const uint8_t> readBlob() { return readBytes(); }
    std::span<const uint8_t> readCBOR() { return readBytes(); }

    // ── Spatial & quantized reads ──
    void readVec3(float& x, float& y, float& z);
    void readDVec3(double& x, double& y, double& z);
    void readBlockCoord(int32_t& x, int32_t& y, int32_t& z);
    void readQuantizedPos(double& x, double& y, double& z);
    float readQuantizedAngle();

    // ── Structured collection headers ──
    uint32_t readArrayHeader();
    uint32_t readMapHeader();

    // ── Cursor control ──
    [[nodiscard]] size_t remaining() const noexcept {
        return cursor_ <= data_.size() ? (data_.size() - cursor_) : 0;
    }

    [[nodiscard]] bool hasMore() const noexcept {
        return valid_ && cursor_ < data_.size();
    }

    void skip(size_t bytes);

    // ── Error state ──
    [[nodiscard]] bool valid() const noexcept { return valid_; }
    [[nodiscard]] std::string_view error() const noexcept { return error_; }

private:
    bool requireBytes(size_t count, std::string_view context);
    void setError(std::string_view msg);

    uint32_t typeId_ = 0;
    std::span<const uint8_t> data_;
    size_t cursor_ = 0;
    bool valid_ = true;
    std::string error_;
};

}  // namespace finenet
