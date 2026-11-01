#include "finenet/listener.hpp"

#include "finenet/wake_signal.hpp"
#include "stream_transport.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace finenet {

using Clock = std::chrono::steady_clock;

struct Listener::Impl {
    ListenerConfig config;
    std::vector<int> sockets;
    uint16_t port = 0;
    std::thread thread;
    std::atomic<bool> stopping{false};

    struct Handshaking {
        std::unique_ptr<Connection> connection;
        Clock::time_point started;
    };
    std::vector<Handshaking> handshaking;  // Listener thread only

    std::mutex mutex;
    std::deque<std::unique_ptr<Connection>> ready;
    WakeSignal* signal = nullptr;

    ~Impl() {
        stop();
    }

    void stop() {
        stopping = true;
        if (thread.joinable()) thread.join();
        for (int fd : sockets) ::close(fd);
        sockets.clear();
    }

    void run() {
        while (!stopping) {
            std::vector<pollfd> fds;
            for (int fd : sockets) fds.push_back({fd, POLLIN, 0});
            poll(fds.data(), static_cast<nfds_t>(fds.size()), 50);

            for (const pollfd& entry : fds) {
                if (entry.revents & POLLIN) acceptFrom(entry.fd);
            }
            checkHandshakes();
        }
    }

    void acceptFrom(int listenFd) {
        sockaddr_storage peer{};
        socklen_t size = sizeof(peer);
        int fd = ::accept(listenFd, reinterpret_cast<sockaddr*>(&peer), &size);
        if (fd < 0) return;
        detail::prepareSocket(fd);

        if (handshaking.size() >= config.maxHandshaking) {
            ::close(fd);
            return;
        }

        char text[INET6_ADDRSTRLEN] = {0};
        if (peer.ss_family == AF_INET) {
            inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(&peer)->sin_addr, text, sizeof(text));
        } else if (peer.ss_family == AF_INET6) {
            inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6*>(&peer)->sin6_addr, text, sizeof(text));
        }
        handshaking.push_back({Connection::fromAccepted(fd, text, config.connection), Clock::now()});
    }

    void checkHandshakes() {
        auto limit = std::chrono::milliseconds(config.handshakeTimeoutMs);
        auto now = Clock::now();
        for (size_t i = 0; i < handshaking.size();) {
            Handshaking& entry = handshaking[i];
            ConnectionState state = entry.connection->state();
            if (state == ConnectionState::Connected) {
                WakeSignal* toWake = nullptr;
                {
                    std::lock_guard lock(mutex);
                    ready.push_back(std::move(entry.connection));
                    toWake = signal;
                }
                if (toWake) toWake->signal();
            } else if (state == ConnectionState::Connecting && now - entry.started < limit) {
                ++i;
                continue;
            }
            handshaking.erase(handshaking.begin() + static_cast<std::ptrdiff_t>(i));
        }
    }
};

Listener::Listener(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

Listener::~Listener() = default;

std::unique_ptr<Listener> Listener::listen(uint16_t port, const ListenerConfig& config, std::string* error) {
    auto impl = std::make_unique<Impl>();
    impl->config = config;

    auto fail = [&](const std::string& what) {
        if (error) *error = what + ": " + std::strerror(errno);
        return nullptr;
    };

    // One dual-stack socket takes IPv4 too. Where IPv6 is off, that's what's left.
    int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
    if (fd >= 0) {
        detail::prepareSocket(fd);
        int on = 1, off = 0;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
        sockaddr_in6 addr{};
        addr.sin6_family = AF_INET6;
        addr.sin6_addr = in6addr_any;
        addr.sin6_port = htons(port);
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            int saved = errno;
            ::close(fd);
            fd = -1;
            if (saved == EADDRINUSE) {
                errno = saved;
                return fail("port " + std::to_string(port) + " is in use");
            }
        }
    }
    if (fd < 0) {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return fail("couldn't make a socket");
        detail::prepareSocket(fd);
        int on = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            std::string what = "couldn't bind port " + std::to_string(port);
            auto result = fail(what);
            ::close(fd);
            return result;
        }
    }

    if (::listen(fd, 16) != 0) {
        auto result = fail("couldn't listen");
        ::close(fd);
        return result;
    }
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    sockaddr_storage bound{};
    socklen_t size = sizeof(bound);
    getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &size);
    impl->port = ntohs(bound.ss_family == AF_INET6 ? reinterpret_cast<sockaddr_in6*>(&bound)->sin6_port
                                                   : reinterpret_cast<sockaddr_in*>(&bound)->sin_port);
    impl->sockets.push_back(fd);

    Impl* raw = impl.get();
    impl->thread = std::thread([raw] { raw->run(); });
    return std::unique_ptr<Listener>(new Listener(std::move(impl)));
}

uint16_t Listener::port() const {
    return impl_->port;
}

std::unique_ptr<Connection> Listener::accept() {
    std::lock_guard lock(impl_->mutex);
    if (impl_->ready.empty()) return nullptr;
    auto connection = std::move(impl_->ready.front());
    impl_->ready.pop_front();
    return connection;
}

void Listener::setWakeSignal(WakeSignal* signal) {
    std::lock_guard lock(impl_->mutex);
    impl_->signal = signal;
}

void Listener::stop() {
    impl_->stop();
}

}  // namespace finenet
