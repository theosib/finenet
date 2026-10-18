#pragma once

/**
 * @file connection.hpp
 * @brief Connection lifecycle, channel routing, and in-process local transport
 */

#include "finenet/channel.hpp"
#include "finenet/message.hpp"
#include "finenet/priority.hpp"
#include "finenet/queue.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace finenet {

/**
 * @brief Connection lifecycle states
 */
enum class ConnectionState : uint8_t {
    Disconnected  = 0,
    Connecting    = 1,
    Connected     = 2,
    Disconnecting = 3,
};

/**
 * @brief Underlying transport backend type
 */
enum class TransportType : uint8_t {
    Local = 0,  ///< In-process concurrent queue pair
    Udp   = 1,  ///< UDP socket transport
    Tcp   = 2,  ///< TCP socket transport
};

/**
 * @brief Transport selection preference for network connections
 */
enum class TransportMode : uint8_t {
    PreferUdp = 0,
    UdpOnly   = 1,
    TcpOnly   = 2,
};

/**
 * @brief Result of a Connection::send() call
 */
enum class SendResult : uint8_t {
    Ok              = 0,  ///< Queued / delivered successfully
    ChannelFull     = 1,  ///< Channel queue is at capacity (backpressure)
    MessageTooLarge = 2,  ///< Exceeds channel's maxMessageSize
    NotConnected    = 3,  ///< Connection is not in the Connected state
};

/**
 * @brief Bandwidth allocation settings
 */
struct BandwidthConfig {
    uint32_t maxBytesPerSecond = 0;  ///< 0 = unlimited
    float criticalReserve = 0.2f;
    float bulkMaxFraction = 0.5f;
};

/**
 * @brief Configuration for creating a Connection
 */
struct ConnectionConfig {
    TransportMode transport = TransportMode::PreferUdp;
    bool preferUdp = true;
    uint32_t maxBandwidthBps = 0;        ///< 0 = unlimited
    uint32_t connectionTimeoutMs = 15000;
    uint32_t keepAliveIntervalMs = 5000;
    uint32_t mtu = 1200;
    uint32_t compressionThreads = 2;
    uint32_t maxRetransmissions = 15;
    float coalescingWindowMs = 0.0f;

    /// Force local connections to encode/decode messages through wire bytes
    bool debugSerialization = false;

    BandwidthConfig bandwidth{};
    std::vector<ChannelConfig> channels = Channels::defaultChannels();
};

/**
 * @brief Per-channel and aggregate connection statistics
 */
struct ConnectionStats {
    float rttMs = 0.0f;
    float packetLossPercent = 0.0f;
    uint64_t bytesSent = 0;
    uint64_t bytesReceived = 0;
    uint64_t packetsSent = 0;
    uint64_t packetsReceived = 0;
    uint32_t pendingReliableMessages = 0;
    uint32_t pendingReliablePackets = 0;

    struct ChannelStats {
        uint64_t messagesSent = 0;
        uint64_t messagesReceived = 0;
        uint64_t bytesSent = 0;
        uint64_t bytesReceived = 0;
        uint32_t retransmissions = 0;
        uint32_t droppedMessages = 0;
    };

    std::unordered_map<ChannelId, ChannelStats> perChannel;
};

/**
 * @brief Maps MessageType IDs to logical channels with optional fallback handler
 */
class ChannelRouter {
public:
    using FallbackHandler = std::function<void(const Message&)>;

    void assign(uint32_t typeId, ChannelId channel) {
        routes_[typeId] = channel;
    }

    void assign(MessageType type, ChannelId channel) {
        assign(static_cast<uint32_t>(type), channel);
    }

    void assignRange(uint32_t typeIdStart, uint32_t typeIdEnd, ChannelId channel) {
        for (uint32_t id = typeIdStart; id <= typeIdEnd; ++id) {
            routes_[id] = channel;
        }
    }

    [[nodiscard]] ChannelId resolve(uint32_t typeId, ChannelId defaultChannel = 0) const {
        auto it = routes_.find(typeId);
        return it != routes_.end() ? it->second : defaultChannel;
    }

    [[nodiscard]] ChannelId resolve(MessageType type, ChannelId defaultChannel = 0) const {
        return resolve(static_cast<uint32_t>(type), defaultChannel);
    }

    [[nodiscard]] bool hasMapping(uint32_t typeId) const {
        return routes_.find(typeId) != routes_.end();
    }

    void setFallback(FallbackHandler handler) {
        fallback_ = std::move(handler);
    }

    void invokeFallback(const Message& msg) const {
        if (fallback_) {
            fallback_(msg);
        }
    }

    [[nodiscard]] static ChannelRouter createStandardRouter();

private:
    std::unordered_map<uint32_t, ChannelId> routes_;
    FallbackHandler fallback_;
};

/**
 * @brief Endpoint of a client/server connection (local in-process or network)
 */
class Connection {
public:
    using MessageHandler = std::function<void(const Message&)>;
    using StateHandler = std::function<void(ConnectionState)>;

    /**
     * @brief Create a connected in-process pair of Connections {client, server}
     */
    static std::pair<std::unique_ptr<Connection>, std::unique_ptr<Connection>>
    createLocalPair(const ConnectionConfig& config = {});

    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&&) = delete;
    Connection& operator=(Connection&&) = delete;

    // ── State & Transport Info ──
    [[nodiscard]] ConnectionState state() const;
    [[nodiscard]] bool isConnected() const;
    [[nodiscard]] std::string disconnectReason() const;

    [[nodiscard]] TransportType transportType() const noexcept;
    [[nodiscard]] bool isLocal() const noexcept {
        return transportType() == TransportType::Local;
    }

    [[nodiscard]] const ConnectionConfig& config() const noexcept;
    [[nodiscard]] std::optional<ChannelConfig> channelConfig(ChannelId channel) const;

    // ── Sending ──
    SendResult send(ChannelId channel, Message msg);
    SendResult send(ChannelId channel, Message msg, Priority overridePriority);

    // ── Callback Registration (dispatched during update()) ──
    void onMessage(MessageType type, MessageHandler handler);
    void onMessage(uint32_t typeId, MessageHandler handler);
    [[nodiscard]] bool hasMessageHandler(MessageType type) const;
    [[nodiscard]] bool hasMessageHandler(uint32_t typeId) const;

    void onChannel(ChannelId channel, MessageHandler handler);
    void onFallback(MessageHandler handler);
    void onStateChange(StateHandler handler);

    // ── Direct Inbound Queue Access (supports WakeSignal attachment) ──
    Queue<Message>& inboundQueue();
    Queue<Message>& inboundQueue(ChannelId channel);

    // ── Lifecycle ──
    void update();
    void disconnect(std::string_view reason = "");

    // ── Statistics ──
    [[nodiscard]] ConnectionStats stats() const;

private:
    struct Impl;
    explicit Connection(std::shared_ptr<Impl> impl);

    SendResult sendInternal(ChannelId channel, Message msg, std::optional<Priority> overridePriority);

    std::shared_ptr<Impl> impl_;
};

}  // namespace finenet
