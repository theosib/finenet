#pragma once

/**
 * @file message.hpp
 * @brief MessageType prefix encoding and immutable Message container
 */

#include "finenet/channel.hpp"
#include "finenet/priority.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace finenet {

/**
 * @brief Standard MessageType identifiers
 *
 * Variable-length prefix encoding on the wire:
 * - 0x00–0x7F:       1 byte
 * - 0x80–0x3FFF:     2 bytes
 * - 0x4000–0x1FFFFF: 3 bytes
 * - >= 0x200000:     4 bytes
 */
enum class MessageType : uint32_t {
    None                = 0x00,

    // === 1-byte types (high frequency) ===
    PlayerInput         = 0x01,
    InputAck            = 0x02,
    EntityPosition      = 0x03,
    EntityBatch         = 0x04,
    BlockChange         = 0x05,
    LightUpdate         = 0x06,
    KeepAlive           = 0x07,
    SoundEvent          = 0x08,
    EffectTrigger       = 0x09,
    ParticleSpawn       = 0x0A,
    WorldTimeSync       = 0x0B,
    DamageEvent         = 0x0C,
    CollapseEvent       = 0x0D,
    FluidUpdate         = 0x0E,
    WeatherUpdate       = 0x0F,
    EnvironmentEvent    = 0x10,
    SignalBatch         = 0x11,
    VitalStats          = 0x12,

    // === Medium frequency types ===
    ChunkData           = 0x8001,
    ChunkUnload         = 0x8002,
    PlayerAction        = 0x8003,
    ActionResult        = 0x8004,
    EntitySpawn         = 0x8005,
    EntityDespawn       = 0x8006,
    EntityAnimation     = 0x8007,
    PlayerCorrection    = 0x8008,
    BlockCorrection     = 0x8009,
    OpenUI              = 0x8010,
    CloseUI             = 0x8011,
    UIUpdate            = 0x8012,
    UIAction            = 0x8013,
    ChatMessage         = 0x8014,
    InventoryUpdate     = 0x8015,
    MachineState        = 0x8016,
    PowerGridState      = 0x8017,
    BeltSegmentState    = 0x8018,
    EntityBatchSpawn    = 0x8019,
    EntityBatchDespawn  = 0x801A,
    VehicleState        = 0x801B,
    MapData             = 0x801C,
    WaypointAction      = 0x801D,
    DimensionTransition = 0x801E,
    DimensionReady      = 0x801F,
    QuestUpdate         = 0x8020,
    TradeAction         = 0x8021,
    NPCInteraction      = 0x8022,
    MerchantInventory   = 0x8023,
    StatusEffect        = 0x8024,
    ClaimEvent          = 0x8025,
    CraftingState       = 0x8026,
    MinigameAction      = 0x8027,
    GameEvent           = 0x8028,
    BossState           = 0x8029,
    RespawnRequest      = 0x802A,
    VehicleMount        = 0x802B,
    PartyUpdate         = 0x802C,

    // === Rare / setup / asset types ===
    ClientHello         = 0xC00001,
    ServerHello         = 0xC00002,
    AssetRequest        = 0xC00003,
    AssetResponse       = 0xC00004,
    BlockAppearanceList = 0xC00005,
    ItemAppearanceList  = 0xC00006,
    EntityModelDef      = 0xC00007,
    TextureAtlas        = 0xC00008,
    AudioSample         = 0xC00009,
    UIDefinition        = 0xC0000A,
    ClientRuleDef       = 0xC0000B,
    NameRegistrySync    = 0xC0000C,
    ServerConfig        = 0xC0000D,
    Disconnect          = 0xC0000E,
    BiomeDefinitions    = 0xC0000F,
    RecipeList          = 0xC00010,
    LootTableDefs       = 0xC00011,
    ScriptBundle        = 0xC00012,
};

/**
 * @brief Compute wire byte count for a prefix-coded MessageType ID
 *
 * - 1 byte  for 0x00–0x7F
 * - 2 bytes for 0x80–0x3FFF
 * - 3 bytes for 0x4000–0x1FFFFF
 * - 4 bytes above 0x1FFFFF
 */
constexpr size_t encodedMessageTypeSize(uint32_t typeId) noexcept {
    if (typeId <= 0x7Fu) return 1;
    if (typeId <= 0x3FFFu) return 2;
    if (typeId <= 0x1FFFFFu) return 3;
    return 4;
}

constexpr size_t encodedMessageTypeSize(MessageType type) noexcept {
    return encodedMessageTypeSize(static_cast<uint32_t>(type));
}

/**
 * @brief Append prefix-coded MessageType to byte buffer
 * @return Number of bytes written (1–4)
 */
size_t encodeMessageType(uint32_t typeId, std::vector<uint8_t>& out);

inline size_t encodeMessageType(MessageType type, std::vector<uint8_t>& out) {
    return encodeMessageType(static_cast<uint32_t>(type), out);
}

/**
 * @brief Decode prefix-coded MessageType from byte span, advancing offset
 * @return true on success, false if truncated
 */
bool decodeMessageType(std::span<const uint8_t> data, size_t& offset, uint32_t& outTypeId);

inline bool decodeMessageType(std::span<const uint8_t> data, size_t& offset, MessageType& outType) {
    uint32_t raw = 0;
    if (!decodeMessageType(data, offset, raw)) {
        return false;
    }
    outType = static_cast<MessageType>(raw);
    return true;
}

/**
 * @brief Built network message with type ID, transport metadata, and binary payload
 */
class Message {
public:
    enum Flags : uint8_t {
        FlagNone         = 0,
        FlagReliable     = 1 << 0,
        FlagOrdered      = 1 << 1,
        FlagFragment     = 1 << 2,
        FlagCompressed   = 1 << 3,
        FlagLastFragment = 1 << 4,
        FlagBatch        = 1 << 5,
    };

    Message() = default;

    explicit Message(uint32_t typeId, std::vector<uint8_t> payload = {})
        : typeId_(typeId), payload_(std::move(payload)) {}

    explicit Message(MessageType type, std::vector<uint8_t> payload = {})
        : typeId_(static_cast<uint32_t>(type)), payload_(std::move(payload)) {}

    // ── Type identification ──
    [[nodiscard]] MessageType type() const noexcept {
        return static_cast<MessageType>(typeId_);
    }

    [[nodiscard]] uint32_t typeId() const noexcept {
        return typeId_;
    }

    void setType(MessageType t) noexcept {
        typeId_ = static_cast<uint32_t>(t);
    }

    void setTypeId(uint32_t id) noexcept {
        typeId_ = id;
    }

    // ── Transport metadata ──
    [[nodiscard]] ChannelId channel() const noexcept { return channel_; }
    void setChannel(ChannelId ch) noexcept { channel_ = ch; }

    [[nodiscard]] Priority priority() const noexcept { return priority_; }
    void setPriority(Priority prio) noexcept { priority_ = prio; }

    [[nodiscard]] uint32_t sequenceNumber() const noexcept { return sequenceNumber_; }
    void setSequenceNumber(uint32_t seq) noexcept { sequenceNumber_ = seq; }

    [[nodiscard]] uint8_t flags() const noexcept { return flags_; }
    void setFlags(uint8_t f) noexcept { flags_ = f; }

    [[nodiscard]] bool isReliable() const noexcept { return (flags_ & FlagReliable) != 0; }
    void setReliable(bool v) noexcept { setFlagBit(FlagReliable, v); }

    [[nodiscard]] bool isOrdered() const noexcept { return (flags_ & FlagOrdered) != 0; }
    void setOrdered(bool v) noexcept { setFlagBit(FlagOrdered, v); }

    [[nodiscard]] bool isCompressed() const noexcept { return (flags_ & FlagCompressed) != 0; }
    void setCompressed(bool v) noexcept { setFlagBit(FlagCompressed, v); }

    [[nodiscard]] bool isFragmented() const noexcept { return (flags_ & FlagFragment) != 0; }
    void setFragmented(bool v) noexcept { setFlagBit(FlagFragment, v); }

    [[nodiscard]] bool isBatch() const noexcept { return (flags_ & FlagBatch) != 0; }
    void setBatch(bool v) noexcept { setFlagBit(FlagBatch, v); }

    // ── Payload access ──
    [[nodiscard]] std::span<const uint8_t> payload() const noexcept {
        return payload_;
    }

    [[nodiscard]] size_t payloadSize() const noexcept {
        return payload_.size();
    }

    /// Total logical size including prefix-encoded MessageType header and payload
    [[nodiscard]] size_t size() const noexcept {
        return encodedMessageTypeSize(typeId_) + payload_.size();
    }

    // ── Wire serialization (used by network transport and debugSerialization mode) ──
    [[nodiscard]] std::vector<uint8_t> toWireBytes() const;
    [[nodiscard]] static std::optional<Message> fromWireBytes(std::span<const uint8_t> bytes);

private:
    friend class MessageBuilder;

    void setFlagBit(uint8_t mask, bool enabled) noexcept {
        if (enabled) {
            flags_ |= mask;
        } else {
            flags_ &= static_cast<uint8_t>(~mask);
        }
    }

    uint32_t typeId_ = 0;
    ChannelId channel_ = 0;
    Priority priority_ = Priority::Normal;
    uint8_t flags_ = FlagNone;
    uint32_t sequenceNumber_ = 0;
    std::vector<uint8_t> payload_;
};

}  // namespace finenet
