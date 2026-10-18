#pragma once

/**
 * @file channel.hpp
 * @brief Logical channel configuration and standard channel definitions
 */

#include "finenet/priority.hpp"

#include <cstdint>
#include <string_view>
#include <vector>

namespace finenet {

/// Logical channel identifier (0–255 per connection)
using ChannelId = uint8_t;

/**
 * @brief Configuration for a logical channel
 */
struct ChannelConfig {
    ChannelId id = 0;
    std::string_view name = "";

    // Delivery guarantees
    Reliability reliability = Reliability::ReliableOrdered;
    bool ordered = true;

    // Priority
    Priority defaultPriority = Priority::Normal;
    bool allowPriorityOverride = true;

    // Compression
    CompressionPolicy compression = CompressionPolicy::Never;
    uint32_t compressionThreshold = 0;

    // Fragmentation
    bool allowFragmentation = false;
    uint32_t maxMessageSize = 0;  ///< Hard limit in bytes (0 = unlimited)

    // Flow control
    uint32_t maxUnackedBytes = 0;    ///< Backpressure threshold (0 = unlimited)
    uint32_t maxQueuedMessages = 0;  ///< For Unreliable: drop oldest when exceeded (0 = unlimited)

    /// Implicit conversion to ChannelId so ChannelConfig constants can be passed directly to send()
    constexpr operator ChannelId() const noexcept { return id; }
};

/**
 * @brief Standard logical channels for client/server communication
 */
namespace Channels {

/// Control (handshake, keepalive, disconnect): reliable ordered, critical
inline constexpr ChannelConfig Control {
    .id = 0,
    .name = "control",
    .reliability = Reliability::ReliableOrdered,
    .ordered = true,
    .defaultPriority = Priority::Critical,
    .allowPriorityOverride = true,
    .compression = CompressionPolicy::Never,
    .compressionThreshold = 0,
    .allowFragmentation = false,
    .maxMessageSize = 0,
    .maxUnackedBytes = 0,
    .maxQueuedMessages = 0,
};

/// Player input: must arrive, must be in order, highest priority
inline constexpr ChannelConfig Input {
    .id = 1,
    .name = "input",
    .reliability = Reliability::ReliableOrdered,
    .ordered = true,
    .defaultPriority = Priority::Critical,
    .allowPriorityOverride = true,
    .compression = CompressionPolicy::Never,
    .compressionThreshold = 0,
    .allowFragmentation = false,
    .maxMessageSize = 256,
    .maxUnackedBytes = 0,
    .maxQueuedMessages = 0,
};

/// Game actions (break/place/use): reliable + ordered
inline constexpr ChannelConfig Actions {
    .id = 2,
    .name = "actions",
    .reliability = Reliability::ReliableOrdered,
    .ordered = true,
    .defaultPriority = Priority::High,
    .allowPriorityOverride = true,
    .compression = CompressionPolicy::Never,
    .compressionThreshold = 0,
    .allowFragmentation = false,
    .maxMessageSize = 512,
    .maxUnackedBytes = 0,
    .maxQueuedMessages = 0,
};

/// Entity state: reliable but unordered (latest wins)
inline constexpr ChannelConfig Entities {
    .id = 3,
    .name = "entities",
    .reliability = Reliability::Reliable,
    .ordered = false,
    .defaultPriority = Priority::Normal,
    .allowPriorityOverride = true,
    .compression = CompressionPolicy::SizeThreshold,
    .compressionThreshold = 128,
    .allowFragmentation = false,
    .maxMessageSize = 0,
    .maxUnackedBytes = 0,
    .maxQueuedMessages = 0,
};

/// Effects (sounds, particles): unreliable, normal priority
inline constexpr ChannelConfig Effects {
    .id = 4,
    .name = "effects",
    .reliability = Reliability::Unreliable,
    .ordered = false,
    .defaultPriority = Priority::Normal,
    .allowPriorityOverride = true,
    .compression = CompressionPolicy::Never,
    .compressionThreshold = 0,
    .allowFragmentation = false,
    .maxMessageSize = 0,
    .maxUnackedBytes = 0,
    .maxQueuedMessages = 64,
};

/// World data (chunks, lighting): reliable ordered, low priority, compressed
inline constexpr ChannelConfig World {
    .id = 5,
    .name = "world",
    .reliability = Reliability::ReliableOrdered,
    .ordered = true,
    .defaultPriority = Priority::Low,
    .allowPriorityOverride = true,
    .compression = CompressionPolicy::Always,
    .compressionThreshold = 0,
    .allowFragmentation = true,
    .maxMessageSize = 0,
    .maxUnackedBytes = 0,
    .maxQueuedMessages = 0,
};

/// UI: reliable ordered, high priority
inline constexpr ChannelConfig UI {
    .id = 6,
    .name = "ui",
    .reliability = Reliability::ReliableOrdered,
    .ordered = true,
    .defaultPriority = Priority::High,
    .allowPriorityOverride = true,
    .compression = CompressionPolicy::SizeThreshold,
    .compressionThreshold = 256,
    .allowFragmentation = false,
    .maxMessageSize = 0,
    .maxUnackedBytes = 0,
    .maxQueuedMessages = 0,
};

/// Asset streaming: reliable, bulk priority, compressed, fragmented
inline constexpr ChannelConfig Assets {
    .id = 7,
    .name = "assets",
    .reliability = Reliability::Reliable,
    .ordered = false,
    .defaultPriority = Priority::Bulk,
    .allowPriorityOverride = true,
    .compression = CompressionPolicy::SizeThreshold,
    .compressionThreshold = 1024,
    .allowFragmentation = true,
    .maxMessageSize = 0,
    .maxUnackedBytes = 0,
    .maxQueuedMessages = 0,
};

/// Chat: reliable ordered, normal priority
inline constexpr ChannelConfig Chat {
    .id = 8,
    .name = "chat",
    .reliability = Reliability::ReliableOrdered,
    .ordered = true,
    .defaultPriority = Priority::Normal,
    .allowPriorityOverride = true,
    .compression = CompressionPolicy::Never,
    .compressionThreshold = 0,
    .allowFragmentation = false,
    .maxMessageSize = 4096,
    .maxUnackedBytes = 0,
    .maxQueuedMessages = 0,
};

/// Return all standard channels as a vector
inline std::vector<ChannelConfig> defaultChannels() {
    return {
        Control,
        Input,
        Actions,
        Entities,
        Effects,
        World,
        UI,
        Assets,
        Chat,
    };
}

}  // namespace Channels

}  // namespace finenet
