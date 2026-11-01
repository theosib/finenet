#pragma once

/**
 * @file stream_transport.hpp
 * @brief A connection over a byte stream: framing, scheduling, liveness. Internal to finenet.
 *
 * Channels are multiplexed over one stream by splitting messages into frames, so that a huge
 * message can't hold up a small one. Each transport owns one socket and one I/O thread, which
 * also answers pings: a peer stays alive while its application is busy.
 */

#include "finenet/channel.hpp"
#include "finenet/connection.hpp"
#include "finenet/message.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace finenet::detail {

/// Sockets ignore SIGPIPE, and close on exec
void prepareSocket(int fd);

class StreamTransport {
public:
    struct Handlers {
        std::function<void()> connected;
        std::function<void(Message)> message;
        std::function<void(std::string reason)> closed;
    };

    StreamTransport(const ConnectionConfig& config, Handlers handlers);
    ~StreamTransport();

    StreamTransport(const StreamTransport&) = delete;
    StreamTransport& operator=(const StreamTransport&) = delete;

    /// Resolve and connect on the I/O thread, so this doesn't block
    void startConnect(std::string host, uint16_t port);
    /// Take over a socket from accept()
    void startAccepted(int fd, std::string peerAddress);

    /// Queue a message to send. Thread-safe.
    SendResult send(const Message& message, const ChannelConfig& channel, Priority priority);

    /// Send what's queued and a Close, and stop. Waits for the thread, at most the linger time.
    void close(std::string_view reason, bool linger);

    [[nodiscard]] size_t pendingBytes() const;
    [[nodiscard]] size_t pendingBytes(ChannelId channel) const;
    [[nodiscard]] float rttMs() const { return rttMs_.load(std::memory_order_relaxed); }
    [[nodiscard]] std::string peerAddress() const;

private:
    static constexpr ChannelId TRANSPORT_CHANNEL = 255;
    static constexpr size_t SLICE_BYTES = 16 * 1024;
    static constexpr size_t MAX_FRAME_BYTES = 65536;
    static constexpr size_t READ_BYTES = 64 * 1024;

    struct Outbound {
        std::vector<uint8_t> bytes;
        size_t sent = 0;
        Priority priority = Priority::Normal;
    };

    struct ChannelState {
        std::deque<Outbound> queue;
        size_t bytes = 0;
        std::vector<uint8_t> partial;  ///< Inbound frames of a message not yet whole
        bool started = false;
    };

    void run();
    void loop();
    [[nodiscard]] bool dial();
    void finish(std::string reason);
    void wake();
    void pumpRead();
    void pumpWrite();
    void handleFrame(ChannelId channel, uint8_t flags, const uint8_t* data, size_t size);
    void handleTransport(const uint8_t* data, size_t size);
    void deliver(ChannelId channel);
    bool nextFrame();
    void queueTransportFrame(uint8_t kind, const uint8_t* data, size_t size);

    const ConnectionConfig config_;
    Handlers handlers_;

    std::string host_;
    uint16_t port_ = 0;
    int socket_ = -1;
    int wakeRead_ = -1;
    int wakeWrite_ = -1;
    std::thread thread_;

    mutable std::mutex mutex_;
    std::array<ChannelState, 256> channels_{};
    std::deque<std::vector<uint8_t>> transportFrames_;
    size_t pendingBytes_ = 0;
    uint8_t roundRobin_ = 0;
    bool closing_ = false;
    bool closeQueued_ = false;
    bool lingerOnClose_ = true;
    std::string closeReason_;
    std::chrono::steady_clock::time_point closeDeadline_;
    std::string peerAddress_;

    // I/O thread only
    std::vector<uint8_t> frameOut_;
    size_t frameSent_ = 0;
    std::vector<uint8_t> inbound_;
    size_t inboundUsed_ = 0;
    bool preambleSeen_ = false;
    bool notifiedConnected_ = false;
    bool peerClosed_ = false;
    std::string failure_;
    std::chrono::steady_clock::time_point lastReceived_;
    std::chrono::steady_clock::time_point lastSent_;

    std::atomic<float> rttMs_{0.0f};
    std::atomic<bool> stop_{false};
};

}  // namespace finenet::detail
