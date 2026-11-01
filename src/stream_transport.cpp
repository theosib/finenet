#include "stream_transport.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0  // macOS uses SO_NOSIGPIPE, in prepareSocket
#endif

namespace finenet::detail {

namespace {

using Clock = std::chrono::steady_clock;

constexpr uint8_t PREAMBLE[8] = {'F', 'N', 'E', 'T', 1, 0, 0, 0};
constexpr uint8_t FLAG_FIN = 1;
constexpr uint8_t KIND_PING = 1;
constexpr uint8_t KIND_PONG = 2;
constexpr uint8_t KIND_CLOSE = 3;

constexpr auto LINGER_LIMIT = std::chrono::seconds(1);
constexpr int DIAL_MS_PER_ADDRESS = 3000;
/// How much may sit unsent in the kernel. Past this, a late urgent message would queue behind it.
constexpr int KERNEL_UNSENT_BYTES = 32 * 1024;

void setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

std::string describeAddress(const sockaddr* addr) {
    char text[INET6_ADDRSTRLEN] = {0};
    if (addr->sa_family == AF_INET) {
        inet_ntop(AF_INET, &reinterpret_cast<const sockaddr_in*>(addr)->sin_addr, text, sizeof(text));
    } else if (addr->sa_family == AF_INET6) {
        inet_ntop(AF_INET6, &reinterpret_cast<const sockaddr_in6*>(addr)->sin6_addr, text, sizeof(text));
    }
    return text;
}

uint64_t monotonicMicros() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now().time_since_epoch()).count());
}

}  // namespace

void prepareSocket(int fd) {
    fcntl(fd, F_SETFD, FD_CLOEXEC);
#ifdef SO_NOSIGPIPE
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
}

StreamTransport::StreamTransport(const ConnectionConfig& config, Handlers handlers)
    : config_(config), handlers_(std::move(handlers)) {
    int fds[2];
    if (pipe(fds) == 0) {
        wakeRead_ = fds[0];
        wakeWrite_ = fds[1];
        setNonBlocking(wakeRead_);
        setNonBlocking(wakeWrite_);
    }
    inbound_.resize(READ_BYTES * 2);
}

StreamTransport::~StreamTransport() {
    close("", false);
    if (socket_ >= 0) ::close(socket_);
    if (wakeRead_ >= 0) ::close(wakeRead_);
    if (wakeWrite_ >= 0) ::close(wakeWrite_);
}

void StreamTransport::startConnect(std::string host, uint16_t port) {
    host_ = std::move(host);
    port_ = port;
    thread_ = std::thread([this] { run(); });
}

void StreamTransport::startAccepted(int fd, std::string peerAddress) {
    socket_ = fd;
    peerAddress_ = std::move(peerAddress);
    thread_ = std::thread([this] { run(); });
}

void StreamTransport::wake() {
    uint8_t byte = 1;
    // A full pipe means it's already going to wake
    [[maybe_unused]] auto written = ::write(wakeWrite_, &byte, 1);
}

SendResult StreamTransport::send(const Message& message, const ChannelConfig& channel, Priority priority) {
    Outbound out;
    out.priority = priority;
    out.bytes.reserve(encodedMessageTypeSize(message.typeId()) + message.payloadSize());
    encodeMessageType(message.typeId(), out.bytes);
    auto payload = message.payload();
    out.bytes.insert(out.bytes.end(), payload.begin(), payload.end());

    {
        std::lock_guard lock(mutex_);
        if (closing_ || stop_.load(std::memory_order_relaxed)) return SendResult::NotConnected;
        ChannelState& state = channels_[channel.id];

        if (channel.reliability == Reliability::Unreliable && channel.maxQueuedMessages > 0) {
            // Oldest goes first, but not one that's partly on the wire
            while (state.queue.size() >= channel.maxQueuedMessages) {
                size_t victim = state.queue.front().sent > 0 ? 1 : 0;
                if (victim >= state.queue.size()) break;
                state.bytes -= state.queue[victim].bytes.size();
                pendingBytes_ -= state.queue[victim].bytes.size();
                state.queue.erase(state.queue.begin() + static_cast<std::ptrdiff_t>(victim));
            }
        } else if (channel.maxUnackedBytes > 0 && state.bytes >= channel.maxUnackedBytes) {
            return SendResult::ChannelFull;
        }

        state.bytes += out.bytes.size();
        pendingBytes_ += out.bytes.size();
        state.queue.push_back(std::move(out));
    }
    wake();
    return SendResult::Ok;
}

void StreamTransport::close(std::string_view reason, bool linger) {
    {
        std::lock_guard lock(mutex_);
        if (!closing_) {
            closing_ = true;
            lingerOnClose_ = linger;
            closeReason_ = std::string(reason);
            closeDeadline_ = Clock::now() + LINGER_LIMIT;
        }
    }
    wake();
    if (thread_.joinable() && thread_.get_id() != std::this_thread::get_id()) thread_.join();
}

size_t StreamTransport::pendingBytes() const {
    std::lock_guard lock(mutex_);
    return pendingBytes_;
}

size_t StreamTransport::pendingBytes(ChannelId channel) const {
    std::lock_guard lock(mutex_);
    return channels_[channel].bytes;
}

std::string StreamTransport::peerAddress() const {
    std::lock_guard lock(mutex_);
    return peerAddress_;
}

void StreamTransport::run() {
    std::string reason;
    if (socket_ < 0 && !dial()) {
        reason = failure_;
    } else {
        loop();
        reason = failure_;
    }
    stop_.store(true, std::memory_order_relaxed);
    finish(std::move(reason));
}

bool StreamTransport::dial() {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* found = nullptr;
    std::string service = std::to_string(port_);
    int resolved = getaddrinfo(host_.c_str(), service.c_str(), &hints, &found);
    if (resolved != 0) {
        failure_ = "can't find " + host_ + ": " + gai_strerror(resolved);
        return false;
    }

    failure_ = "couldn't connect to " + host_;
    for (addrinfo* entry = found; entry; entry = entry->ai_next) {
        int fd = ::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
        if (fd < 0) continue;
        prepareSocket(fd);
        setNonBlocking(fd);

        int result = ::connect(fd, entry->ai_addr, entry->ai_addrlen);
        if (result != 0 && errno != EINPROGRESS) {
            failure_ = "couldn't connect to " + host_ + ": " + std::strerror(errno);
            ::close(fd);
            continue;
        }
        if (result != 0) {
            // A wait the application can cut short by closing
            bool ready = false;
            auto deadline = Clock::now() + std::chrono::milliseconds(DIAL_MS_PER_ADDRESS);
            while (Clock::now() < deadline) {
                pollfd fds[2] = {{fd, POLLOUT, 0}, {wakeRead_, POLLIN, 0}};
                poll(fds, 2, 100);
                if (fds[0].revents != 0) {
                    ready = true;
                    break;
                }
                if (fds[1].revents != 0) {
                    uint8_t drain[32];
                    while (::read(wakeRead_, drain, sizeof(drain)) > 0) {}
                }
                std::lock_guard lock(mutex_);
                if (closing_) break;
            }
            int error = ETIMEDOUT;
            if (ready) {
                socklen_t size = sizeof(error);
                getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size);
            }
            if (!ready || error != 0) {
                failure_ = "couldn't connect to " + host_ + ": " + std::strerror(error);
                ::close(fd);
                std::lock_guard lock(mutex_);
                if (closing_) break;
                continue;
            }
        }

        socket_ = fd;
        failure_.clear();
        std::lock_guard lock(mutex_);
        peerAddress_ = describeAddress(entry->ai_addr);
        break;
    }
    freeaddrinfo(found);
    return socket_ >= 0;
}

void StreamTransport::loop() {
    setNonBlocking(socket_);
    int on = 1;
    setsockopt(socket_, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
#ifdef TCP_NOTSENT_LOWAT
    int unsent = KERNEL_UNSENT_BYTES;
    setsockopt(socket_, IPPROTO_TCP, TCP_NOTSENT_LOWAT, &unsent, sizeof(unsent));
#endif

    lastReceived_ = lastSent_ = Clock::now();
    frameOut_.assign(PREAMBLE, PREAMBLE + sizeof(PREAMBLE));
    frameSent_ = 0;

    auto keepAlive = std::chrono::milliseconds(config_.keepAliveIntervalMs);
    auto timeout = std::chrono::milliseconds(config_.connectionTimeoutMs);

    while (failure_.empty()) {
        bool closing;
        {
            std::lock_guard lock(mutex_);
            closing = closing_;
            if (closing && !closeQueued_) {
                if (!lingerOnClose_) break;
                closeQueued_ = true;
            }
        }

        // Pending work first, so that a Close goes after everything queued before it
        if (frameSent_ >= frameOut_.size()) nextFrame();
        bool hasOutput = frameSent_ < frameOut_.size();

        if (closing && !hasOutput) {
            std::lock_guard lock(mutex_);
            if (closeReason_.empty() || peerClosed_) break;
            std::string reason = std::exchange(closeReason_, {});
            std::vector<uint8_t> body(reason.begin(), reason.end());
            transportFrames_.clear();
            frameOut_.clear();
            uint32_t length = static_cast<uint32_t>(body.size() + 3);
            for (int i = 0; i < 4; ++i) frameOut_.push_back(static_cast<uint8_t>(length >> (i * 8)));
            frameOut_.push_back(TRANSPORT_CHANNEL);
            frameOut_.push_back(FLAG_FIN);
            frameOut_.push_back(KIND_CLOSE);
            frameOut_.insert(frameOut_.end(), body.begin(), body.end());
            frameSent_ = 0;
            hasOutput = true;
        }
        if (closing && Clock::now() >= closeDeadline_) break;

        pollfd fds[2] = {{socket_, static_cast<short>(POLLIN | (hasOutput ? POLLOUT : 0)), 0},
                         {wakeRead_, POLLIN, 0}};
        int ready = poll(fds, 2, 100);
        if (ready < 0 && errno != EINTR) {
            failure_ = std::string("poll failed: ") + std::strerror(errno);
            break;
        }
        if (fds[1].revents != 0) {
            uint8_t drain[64];
            while (::read(wakeRead_, drain, sizeof(drain)) > 0) {}
        }
        if (fds[0].revents & (POLLIN | POLLHUP | POLLERR)) pumpRead();
        if (failure_.empty() && (fds[0].revents & POLLOUT)) pumpWrite();

        auto now = Clock::now();
        if (preambleSeen_ && now - lastSent_ >= keepAlive) {
            uint64_t stamp = monotonicMicros();
            uint8_t body[8];
            std::memcpy(body, &stamp, 8);
            {
                std::lock_guard lock(mutex_);
                queueTransportFrame(KIND_PING, body, 8);
            }
            lastSent_ = now;
        }
        if (now - lastReceived_ >= timeout) failure_ = "timed out";
    }
    if (failure_.empty()) {
        std::lock_guard lock(mutex_);
        failure_ = closeReason_.empty() ? "closed" : closeReason_;
    }
}

void StreamTransport::queueTransportFrame(uint8_t kind, const uint8_t* data, size_t size) {
    std::vector<uint8_t> frame;
    uint32_t length = static_cast<uint32_t>(size + 3);
    for (int i = 0; i < 4; ++i) frame.push_back(static_cast<uint8_t>(length >> (i * 8)));
    frame.push_back(TRANSPORT_CHANNEL);
    frame.push_back(FLAG_FIN);
    frame.push_back(kind);
    frame.insert(frame.end(), data, data + size);
    transportFrames_.push_back(std::move(frame));
}

bool StreamTransport::nextFrame() {
    std::lock_guard lock(mutex_);

    // Pings measure the link, so they mustn't wait behind a world download
    if (!transportFrames_.empty()) {
        frameOut_ = std::move(transportFrames_.front());
        transportFrames_.pop_front();
        frameSent_ = 0;
        return true;
    }
    if (pendingBytes_ == 0) return false;

    // Most urgent channel, taking turns among equals so that none starves another
    int best = -1;
    uint8_t bestRank = 255;
    for (int offset = 0; offset < 255; ++offset) {
        int index = (roundRobin_ + offset) % 255;
        const ChannelState& state = channels_[index];
        if (state.queue.empty()) continue;
        uint8_t rank = static_cast<uint8_t>(state.queue.front().priority);
        if (rank < bestRank) {
            bestRank = rank;
            best = index;
        }
    }
    if (best < 0) return false;
    roundRobin_ = static_cast<uint8_t>((best + 1) % 255);

    ChannelState& state = channels_[best];
    Outbound& message = state.queue.front();
    size_t remaining = message.bytes.size() - message.sent;
    size_t take = std::min(remaining, SLICE_BYTES);
    bool last = take == remaining;

    frameOut_.clear();
    frameOut_.reserve(6 + take);
    uint32_t length = static_cast<uint32_t>(take + 2);
    for (int i = 0; i < 4; ++i) frameOut_.push_back(static_cast<uint8_t>(length >> (i * 8)));
    frameOut_.push_back(static_cast<uint8_t>(best));
    frameOut_.push_back(last ? FLAG_FIN : 0);
    frameOut_.insert(frameOut_.end(), message.bytes.begin() + static_cast<std::ptrdiff_t>(message.sent),
                     message.bytes.begin() + static_cast<std::ptrdiff_t>(message.sent + take));
    frameSent_ = 0;

    message.sent += take;
    state.bytes -= take;
    pendingBytes_ -= take;
    if (last) state.queue.pop_front();
    return true;
}

void StreamTransport::pumpWrite() {
    // Until the kernel says stop, or there's nothing left
    while (frameSent_ < frameOut_.size() || nextFrame()) {
        ssize_t sent = ::send(socket_, frameOut_.data() + frameSent_, frameOut_.size() - frameSent_,
                              MSG_NOSIGNAL);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
            failure_ = std::string("connection lost: ") + std::strerror(errno);
            return;
        }
        frameSent_ += static_cast<size_t>(sent);
        lastSent_ = Clock::now();
    }
}

void StreamTransport::pumpRead() {
    while (failure_.empty()) {
        if (inbound_.size() - inboundUsed_ < READ_BYTES) inbound_.resize(inboundUsed_ + READ_BYTES);
        ssize_t got = ::recv(socket_, inbound_.data() + inboundUsed_, READ_BYTES, 0);
        if (got == 0) {
            std::lock_guard lock(mutex_);
            failure_ = closeReason_.empty() ? "connection closed" : closeReason_;
            return;
        }
        if (got < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) return;
            failure_ = std::string("connection lost: ") + std::strerror(errno);
            return;
        }
        inboundUsed_ += static_cast<size_t>(got);
        lastReceived_ = Clock::now();

        size_t offset = 0;
        if (!preambleSeen_) {
            if (inboundUsed_ < sizeof(PREAMBLE)) continue;
            if (std::memcmp(inbound_.data(), PREAMBLE, 4) != 0) {
                failure_ = "not a finenet peer";
                return;
            }
            if (inbound_[4] != PREAMBLE[4]) {
                failure_ = "peer speaks finenet version " + std::to_string(inbound_[4]);
                return;
            }
            offset = sizeof(PREAMBLE);
            preambleSeen_ = true;
            if (!notifiedConnected_) {
                notifiedConnected_ = true;
                if (handlers_.connected) handlers_.connected();
            }
        }

        while (failure_.empty() && inboundUsed_ - offset >= 4) {
            uint32_t length = 0;
            for (int i = 0; i < 4; ++i) {
                length |= static_cast<uint32_t>(inbound_[offset + i]) << (i * 8);
            }
            if (length < 2 || length > MAX_FRAME_BYTES) {
                failure_ = "bad frame from peer";
                return;
            }
            if (inboundUsed_ - offset - 4 < length) break;
            const uint8_t* frame = inbound_.data() + offset + 4;
            handleFrame(frame[0], frame[1], frame + 2, length - 2);
            offset += 4 + length;
        }

        if (offset > 0) {
            std::memmove(inbound_.data(), inbound_.data() + offset, inboundUsed_ - offset);
            inboundUsed_ -= offset;
        }
    }
}

void StreamTransport::handleFrame(ChannelId channel, uint8_t flags, const uint8_t* data, size_t size) {
    if (channel == TRANSPORT_CHANNEL) {
        handleTransport(data, size);
        return;
    }

    ChannelState& state = channels_[channel];
    if (state.partial.size() + size > config_.maxMessageBytes) {
        failure_ = "peer sent too large a message";
        return;
    }
    state.partial.insert(state.partial.end(), data, data + size);
    if (flags & FLAG_FIN) deliver(channel);
}

void StreamTransport::deliver(ChannelId channel) {
    ChannelState& state = channels_[channel];
    uint32_t typeId = 0;
    size_t offset = 0;
    if (!decodeMessageType(state.partial, offset, typeId)) {
        failure_ = "bad message from peer";
        return;
    }
    Message message(typeId, std::vector<uint8_t>(state.partial.begin() + static_cast<std::ptrdiff_t>(offset),
                                                 state.partial.end()));
    message.setChannel(channel);
    state.partial.clear();
    if (handlers_.message) handlers_.message(std::move(message));
}

void StreamTransport::handleTransport(const uint8_t* data, size_t size) {
    if (size == 0) return;
    switch (data[0]) {
        case KIND_PING: {
            std::lock_guard lock(mutex_);
            queueTransportFrame(KIND_PONG, data + 1, size - 1);
            break;
        }
        case KIND_PONG:
            if (size == 9) {
                uint64_t stamp = 0;
                std::memcpy(&stamp, data + 1, 8);
                float ms = static_cast<float>(monotonicMicros() - stamp) / 1000.0f;
                float old = rttMs_.load(std::memory_order_relaxed);
                rttMs_.store(old == 0.0f ? ms : old * 0.8f + ms * 0.2f, std::memory_order_relaxed);
            }
            break;
        case KIND_CLOSE: {
            std::lock_guard lock(mutex_);
            peerClosed_ = true;
            closeReason_.assign(reinterpret_cast<const char*>(data + 1), size - 1);
            failure_ = closeReason_.empty() ? "connection closed" : closeReason_;
            break;
        }
        default:
            break;  // A later version's: nothing here depends on it
    }
}

void StreamTransport::finish(std::string reason) {
    if (socket_ >= 0) {
        ::shutdown(socket_, SHUT_RDWR);
    }
    if (handlers_.closed) handlers_.closed(std::move(reason));
}

}  // namespace finenet::detail
