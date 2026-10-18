#include "finenet/message.hpp"
#include "finenet/message_builder.hpp"
#include "finenet/message_reader.hpp"

#include <cmath>
#include <cstring>
#include <limits>
#include <numbers>

namespace finenet {

namespace {

constexpr double kFixedPoint20_12Scale = 4096.0;
constexpr float kTwoPi = 2.0f * std::numbers::pi_v<float>;
constexpr float kAngleScale = 32767.0f / kTwoPi;

void appendU32LE(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xFFu));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFFu));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFFu));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFFu));
}

bool readU32LE(std::span<const uint8_t> data, size_t& offset, uint32_t& out) {
    if (offset + 4 > data.size()) {
        return false;
    }
    out = static_cast<uint32_t>(data[offset]) |
          (static_cast<uint32_t>(data[offset + 1]) << 8) |
          (static_cast<uint32_t>(data[offset + 2]) << 16) |
          (static_cast<uint32_t>(data[offset + 3]) << 24);
    offset += 4;
    return true;
}

}  // namespace

// ============================================================================
// MessageType Variable-Length Prefix Encoding
// ============================================================================

size_t encodeMessageType(uint32_t typeId, std::vector<uint8_t>& out) {
    if (typeId <= 0x7Fu) {
        out.push_back(static_cast<uint8_t>(typeId));
        return 1;
    }
    if (typeId <= 0x3FFFu) {
        out.push_back(static_cast<uint8_t>(0x80u | ((typeId >> 8) & 0x3Fu)));
        out.push_back(static_cast<uint8_t>(typeId & 0xFFu));
        return 2;
    }
    if (typeId <= 0x1FFFFFu) {
        out.push_back(static_cast<uint8_t>(0xC0u | ((typeId >> 16) & 0x1Fu)));
        out.push_back(static_cast<uint8_t>((typeId >> 8) & 0xFFu));
        out.push_back(static_cast<uint8_t>(typeId & 0xFFu));
        return 3;
    }
    // 4-byte encoding for values > 0x1FFFFF (up to 29 bits: 0x1FFFFFFF)
    out.push_back(static_cast<uint8_t>(0xE0u | ((typeId >> 24) & 0x1Fu)));
    out.push_back(static_cast<uint8_t>((typeId >> 16) & 0xFFu));
    out.push_back(static_cast<uint8_t>((typeId >> 8) & 0xFFu));
    out.push_back(static_cast<uint8_t>(typeId & 0xFFu));
    return 4;
}

bool decodeMessageType(std::span<const uint8_t> data, size_t& offset, uint32_t& outTypeId) {
    if (offset >= data.size()) {
        return false;
    }
    const uint8_t b0 = data[offset];
    if ((b0 & 0x80u) == 0) {
        outTypeId = b0;
        offset += 1;
        return true;
    }
    if ((b0 & 0xC0u) == 0x80u) {
        if (offset + 2 > data.size()) return false;
        outTypeId = (static_cast<uint32_t>(b0 & 0x3Fu) << 8) |
                    static_cast<uint32_t>(data[offset + 1]);
        offset += 2;
        return true;
    }
    if ((b0 & 0xE0u) == 0xC0u) {
        if (offset + 3 > data.size()) return false;
        outTypeId = (static_cast<uint32_t>(b0 & 0x1Fu) << 16) |
                    (static_cast<uint32_t>(data[offset + 1]) << 8) |
                    static_cast<uint32_t>(data[offset + 2]);
        offset += 3;
        return true;
    }
    // 0xE0–0xFF: 4-byte encoding
    if (offset + 4 > data.size()) return false;
    outTypeId = (static_cast<uint32_t>(b0 & 0x1Fu) << 24) |
                (static_cast<uint32_t>(data[offset + 1]) << 16) |
                (static_cast<uint32_t>(data[offset + 2]) << 8) |
                static_cast<uint32_t>(data[offset + 3]);
    offset += 4;
    return true;
}

// ============================================================================
// Message Wire Framing
// ============================================================================

std::vector<uint8_t> Message::toWireBytes() const {
    std::vector<uint8_t> out;
    out.reserve(11 + encodedMessageTypeSize(typeId_) + payload_.size());

    // Frame header: channel (1B), priority (1B), flags (1B), seqNum (4B LE)
    out.push_back(channel_);
    out.push_back(static_cast<uint8_t>(priority_));
    out.push_back(flags_);
    appendU32LE(out, sequenceNumber_);

    // Prefix-coded MessageType (1–4B)
    encodeMessageType(typeId_, out);

    // Payload length (4B LE) + payload bytes
    appendU32LE(out, static_cast<uint32_t>(payload_.size()));
    out.insert(out.end(), payload_.begin(), payload_.end());

    return out;
}

std::optional<Message> Message::fromWireBytes(std::span<const uint8_t> bytes) {
    // Minimum size: 1 (ch) + 1 (prio) + 1 (flags) + 4 (seq) + 1 (type) + 4 (len) = 12 bytes
    if (bytes.size() < 12) {
        return std::nullopt;
    }

    size_t offset = 0;
    const ChannelId channel = bytes[offset++];
    const uint8_t rawPriority = bytes[offset++];
    if (rawPriority > static_cast<uint8_t>(Priority::Bulk)) {
        return std::nullopt;
    }
    const Priority priority = static_cast<Priority>(rawPriority);
    const uint8_t flags = bytes[offset++];

    uint32_t seqNum = 0;
    if (!readU32LE(bytes, offset, seqNum)) {
        return std::nullopt;
    }

    uint32_t typeId = 0;
    if (!decodeMessageType(bytes, offset, typeId)) {
        return std::nullopt;
    }

    uint32_t payloadLen = 0;
    if (!readU32LE(bytes, offset, payloadLen)) {
        return std::nullopt;
    }

    if (offset + payloadLen != bytes.size()) {
        return std::nullopt;
    }

    std::vector<uint8_t> payload(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                                 bytes.end());
    Message msg(typeId, std::move(payload));
    msg.setChannel(channel);
    msg.setPriority(priority);
    msg.setFlags(flags);
    msg.setSequenceNumber(seqNum);
    return msg;
}

// ============================================================================
// MessageBuilder
// ============================================================================

MessageBuilder& MessageBuilder::writeU8(uint8_t v) {
    buffer_.push_back(v);
    return *this;
}

MessageBuilder& MessageBuilder::writeU16(uint16_t v) {
    buffer_.push_back(static_cast<uint8_t>(v & 0xFFu));
    buffer_.push_back(static_cast<uint8_t>((v >> 8) & 0xFFu));
    return *this;
}

MessageBuilder& MessageBuilder::writeU32(uint32_t v) {
    buffer_.push_back(static_cast<uint8_t>(v & 0xFFu));
    buffer_.push_back(static_cast<uint8_t>((v >> 8) & 0xFFu));
    buffer_.push_back(static_cast<uint8_t>((v >> 16) & 0xFFu));
    buffer_.push_back(static_cast<uint8_t>((v >> 24) & 0xFFu));
    return *this;
}

MessageBuilder& MessageBuilder::writeU64(uint64_t v) {
    for (int shift = 0; shift < 64; shift += 8) {
        buffer_.push_back(static_cast<uint8_t>((v >> shift) & 0xFFu));
    }
    return *this;
}

MessageBuilder& MessageBuilder::writeI8(int8_t v) {
    return writeU8(static_cast<uint8_t>(v));
}

MessageBuilder& MessageBuilder::writeI16(int16_t v) {
    return writeU16(static_cast<uint16_t>(v));
}

MessageBuilder& MessageBuilder::writeI32(int32_t v) {
    return writeU32(static_cast<uint32_t>(v));
}

MessageBuilder& MessageBuilder::writeI64(int64_t v) {
    return writeU64(static_cast<uint64_t>(v));
}

MessageBuilder& MessageBuilder::writeF32(float v) {
    uint32_t bits = 0;
    std::memcpy(&bits, &v, sizeof(float));
    return writeU32(bits);
}

MessageBuilder& MessageBuilder::writeF64(double v) {
    uint64_t bits = 0;
    std::memcpy(&bits, &v, sizeof(double));
    return writeU64(bits);
}

MessageBuilder& MessageBuilder::writeBool(bool v) {
    return writeU8(v ? 1u : 0u);
}

MessageBuilder& MessageBuilder::writeVarInt(uint64_t v) {
    do {
        uint8_t byte = static_cast<uint8_t>(v & 0x7Fu);
        v >>= 7;
        if (v != 0) {
            byte |= 0x80u;
        }
        buffer_.push_back(byte);
    } while (v != 0);
    return *this;
}

MessageBuilder& MessageBuilder::writeVarSint(int64_t v) {
    // ZigZag encoding maps signed integers to unsigned integers
    const uint64_t zigzag = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
    return writeVarInt(zigzag);
}

MessageBuilder& MessageBuilder::writeString(std::string_view s) {
    writeVarInt(s.size());
    const auto* ptr = reinterpret_cast<const uint8_t*>(s.data());
    buffer_.insert(buffer_.end(), ptr, ptr + s.size());
    return *this;
}

MessageBuilder& MessageBuilder::writeBytes(std::span<const uint8_t> data) {
    writeVarInt(data.size());
    buffer_.insert(buffer_.end(), data.begin(), data.end());
    return *this;
}

MessageBuilder& MessageBuilder::writeBytes(const void* data, size_t len) {
    const auto* ptr = static_cast<const uint8_t*>(data);
    return writeBytes(std::span<const uint8_t>(ptr, len));
}

MessageBuilder& MessageBuilder::writeRawBytes(std::span<const uint8_t> data) {
    buffer_.insert(buffer_.end(), data.begin(), data.end());
    return *this;
}

MessageBuilder& MessageBuilder::writeVec3(float x, float y, float z) {
    return writeF32(x).writeF32(y).writeF32(z);
}

MessageBuilder& MessageBuilder::writeDVec3(double x, double y, double z) {
    return writeF64(x).writeF64(y).writeF64(z);
}

MessageBuilder& MessageBuilder::writeBlockCoord(int32_t x, int32_t y, int32_t z) {
    return writeI32(x).writeI32(y).writeI32(z);
}

MessageBuilder& MessageBuilder::writeQuantizedPos(double x, double y, double z) {
    const auto qx = static_cast<int32_t>(std::llround(x * kFixedPoint20_12Scale));
    const auto qy = static_cast<int32_t>(std::llround(y * kFixedPoint20_12Scale));
    const auto qz = static_cast<int32_t>(std::llround(z * kFixedPoint20_12Scale));
    return writeI32(qx).writeI32(qy).writeI32(qz);
}

MessageBuilder& MessageBuilder::writeQuantizedAngle(float radians) {
    float wrapped = std::fmod(radians, kTwoPi);
    const auto quantized = static_cast<int16_t>(std::lround(wrapped * kAngleScale));
    return writeI16(quantized);
}

MessageBuilder& MessageBuilder::writeArrayHeader(uint32_t count) {
    return writeVarInt(count);
}

MessageBuilder& MessageBuilder::writeMapHeader(uint32_t count) {
    return writeVarInt(count);
}

Message MessageBuilder::build() {
    return Message(typeId_, std::move(buffer_));
}

// ============================================================================
// MessageReader
// ============================================================================

void MessageReader::setError(std::string_view msg) {
    if (valid_) {
        valid_ = false;
        error_ = std::string(msg);
    }
}

bool MessageReader::requireBytes(size_t count, std::string_view context) {
    if (!valid_) {
        return false;
    }
    if (count > data_.size() || cursor_ > data_.size() - count) {
        setError(std::string("Buffer underflow while reading ") + std::string(context));
        return false;
    }
    return true;
}

uint8_t MessageReader::readU8() {
    if (!requireBytes(1, "uint8")) return 0;
    return data_[cursor_++];
}

uint16_t MessageReader::readU16() {
    if (!requireBytes(2, "uint16")) return 0;
    const uint16_t v = static_cast<uint16_t>(data_[cursor_]) |
                       static_cast<uint16_t>(static_cast<uint16_t>(data_[cursor_ + 1]) << 8);
    cursor_ += 2;
    return v;
}

uint32_t MessageReader::readU32() {
    if (!requireBytes(4, "uint32")) return 0;
    const uint32_t v = static_cast<uint32_t>(data_[cursor_]) |
                       (static_cast<uint32_t>(data_[cursor_ + 1]) << 8) |
                       (static_cast<uint32_t>(data_[cursor_ + 2]) << 16) |
                       (static_cast<uint32_t>(data_[cursor_ + 3]) << 24);
    cursor_ += 4;
    return v;
}

uint64_t MessageReader::readU64() {
    if (!requireBytes(8, "uint64")) return 0;
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(data_[cursor_ + i]) << (i * 8);
    }
    cursor_ += 8;
    return v;
}

int8_t MessageReader::readI8() {
    return static_cast<int8_t>(readU8());
}

int16_t MessageReader::readI16() {
    return static_cast<int16_t>(readU16());
}

int32_t MessageReader::readI32() {
    return static_cast<int32_t>(readU32());
}

int64_t MessageReader::readI64() {
    return static_cast<int64_t>(readU64());
}

float MessageReader::readF32() {
    const uint32_t bits = readU32();
    if (!valid_) return 0.0f;
    float v = 0.0f;
    std::memcpy(&v, &bits, sizeof(float));
    return v;
}

double MessageReader::readF64() {
    const uint64_t bits = readU64();
    if (!valid_) return 0.0;
    double v = 0.0;
    std::memcpy(&v, &bits, sizeof(double));
    return v;
}

bool MessageReader::readBool() {
    const uint8_t b = readU8();
    if (!valid_) return false;
    if (b > 1u) {
        setError("Invalid boolean byte value (expected 0 or 1)");
        return false;
    }
    return b == 1u;
}

uint64_t MessageReader::readVarInt() {
    if (!valid_) return 0;

    uint64_t result = 0;
    int shift = 0;
    while (true) {
        if (cursor_ >= data_.size()) {
            setError("Buffer underflow while reading VarInt");
            return 0;
        }
        const uint8_t byte = data_[cursor_++];
        if (shift == 63 && (byte & 0x7Eu) != 0) {
            setError("VarInt overflow (> 64 bits)");
            return 0;
        }
        result |= static_cast<uint64_t>(byte & 0x7Fu) << shift;
        if ((byte & 0x80u) == 0) {
            return result;
        }
        shift += 7;
        if (shift >= 64) {
            setError("VarInt overflow (> 64 bits)");
            return 0;
        }
    }
}

int64_t MessageReader::readVarSint() {
    const uint64_t zigzag = readVarInt();
    if (!valid_) return 0;
    return static_cast<int64_t>((zigzag >> 1) ^ (~(zigzag & 1u) + 1u));
}

std::string_view MessageReader::readString() {
    const uint64_t len = readVarInt();
    if (!valid_) return {};
    if (len > static_cast<uint64_t>(std::numeric_limits<size_t>::max()) ||
        !requireBytes(static_cast<size_t>(len), "string")) {
        return {};
    }
    const auto n = static_cast<size_t>(len);
    std::string_view sv(reinterpret_cast<const char*>(data_.data() + cursor_), n);
    cursor_ += n;
    return sv;
}

std::span<const uint8_t> MessageReader::readBytes() {
    const uint64_t len = readVarInt();
    if (!valid_) return {};
    if (len > static_cast<uint64_t>(std::numeric_limits<size_t>::max()) ||
        !requireBytes(static_cast<size_t>(len), "bytes")) {
        return {};
    }
    const auto n = static_cast<size_t>(len);
    std::span<const uint8_t> slice = data_.subspan(cursor_, n);
    cursor_ += n;
    return slice;
}

void MessageReader::readVec3(float& x, float& y, float& z) {
    x = readF32();
    y = readF32();
    z = readF32();
    if (!valid_) {
        x = y = z = 0.0f;
    }
}

void MessageReader::readDVec3(double& x, double& y, double& z) {
    x = readF64();
    y = readF64();
    z = readF64();
    if (!valid_) {
        x = y = z = 0.0;
    }
}

void MessageReader::readBlockCoord(int32_t& x, int32_t& y, int32_t& z) {
    x = readI32();
    y = readI32();
    z = readI32();
    if (!valid_) {
        x = y = z = 0;
    }
}

void MessageReader::readQuantizedPos(double& x, double& y, double& z) {
    const int32_t qx = readI32();
    const int32_t qy = readI32();
    const int32_t qz = readI32();
    if (!valid_) {
        x = y = z = 0.0;
        return;
    }
    x = static_cast<double>(qx) / kFixedPoint20_12Scale;
    y = static_cast<double>(qy) / kFixedPoint20_12Scale;
    z = static_cast<double>(qz) / kFixedPoint20_12Scale;
}

float MessageReader::readQuantizedAngle() {
    const int16_t q = readI16();
    if (!valid_) return 0.0f;
    return static_cast<float>(q) / kAngleScale;
}

uint32_t MessageReader::readArrayHeader() {
    const uint64_t count = readVarInt();
    if (!valid_) return 0;
    if (count > std::numeric_limits<uint32_t>::max()) {
        setError("Array header count exceeds uint32_t max");
        return 0;
    }
    return static_cast<uint32_t>(count);
}

uint32_t MessageReader::readMapHeader() {
    const uint64_t count = readVarInt();
    if (!valid_) return 0;
    if (count > std::numeric_limits<uint32_t>::max()) {
        setError("Map header count exceeds uint32_t max");
        return 0;
    }
    return static_cast<uint32_t>(count);
}

void MessageReader::skip(size_t bytes) {
    if (!requireBytes(bytes, "skip")) return;
    cursor_ += bytes;
}

}  // namespace finenet
