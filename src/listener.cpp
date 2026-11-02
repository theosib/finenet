#include "finenet/listener.hpp"

#include "finenet/wake_signal.hpp"
#include "socket_compat.hpp"
#include "stream_transport.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

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
        for (int fd : sockets) sock::close(fd);
        sockets.clear();
    }

    void run() {
        while (!stopping) {
            std::vector<pollfd> fds;
            for (int fd : sockets) fds.push_back(sock::entry(fd, POLLIN));
            sock::poll(fds.data(), fds.size(), 50);

            for (size_t i = 0; i < fds.size(); ++i) {
                if (fds[i].revents & POLLIN) acceptFrom(sockets[i]);
            }
            checkHandshakes();
        }
    }

    void acceptFrom(int listenFd) {
        sockaddr_storage peer{};
        socklen_t size = sizeof(peer);
        int fd = sock::accept(listenFd, reinterpret_cast<sockaddr*>(&peer), &size);
        if (fd < 0) return;
        detail::prepareSocket(fd);

        if (handshaking.size() >= config.maxHandshaking) {
            sock::close(fd);
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
    sock::startup();
    auto impl = std::make_unique<Impl>();
    impl->config = config;

    auto fail = [&](const std::string& what, int code) {
        if (error) *error = what + ": " + sock::errorText(code);
        return nullptr;
    };
#ifdef _WIN32
    // SO_REUSEADDR on Windows lets a second server take a port that's in use, and a listening
    // port there is never held by TIME_WAIT anyway. This is what keeps it to one.
    auto reuse = [](int fd) {
        int on = 1;
        sock::setOption(fd, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, &on, sizeof(on));
    };
    auto bindTo = [](int fd, const void* addr, size_t size) {
        return ::bind(static_cast<SOCKET>(fd), static_cast<const sockaddr*>(addr), static_cast<int>(size));
    };
#else
    auto reuse = [](int fd) {
        int on = 1;
        sock::setOption(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    };
    auto bindTo = [](int fd, const void* addr, size_t size) {
        return ::bind(fd, static_cast<const sockaddr*>(addr), static_cast<socklen_t>(size));
    };
#endif

    // One dual-stack socket takes IPv4 too. Where IPv6 is off, that's what's left.
    int fd = sock::open(AF_INET6, SOCK_STREAM, 0);
    if (fd >= 0) {
        detail::prepareSocket(fd);
        int off = 0;
        reuse(fd);
        sock::setOption(fd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
        sockaddr_in6 addr{};
        addr.sin6_family = AF_INET6;
        addr.sin6_addr = in6addr_any;
        addr.sin6_port = htons(port);
        if (bindTo(fd, &addr, sizeof(addr)) != 0) {
            int saved = sock::lastError();
            sock::close(fd);
            fd = -1;
            if (sock::addressInUse(saved)) return fail("port " + std::to_string(port) + " is in use", saved);
        }
    }
    if (fd < 0) {
        fd = sock::open(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return fail("couldn't make a socket", sock::lastError());
        detail::prepareSocket(fd);
        reuse(fd);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);
        if (bindTo(fd, &addr, sizeof(addr)) != 0) {
            auto result = fail("couldn't bind port " + std::to_string(port), sock::lastError());
            sock::close(fd);
            return result;
        }
    }

#ifdef _WIN32
    int listened = ::listen(static_cast<SOCKET>(fd), 16);
#else
    int listened = ::listen(fd, 16);
#endif
    if (listened != 0) {
        auto result = fail("couldn't listen", sock::lastError());
        sock::close(fd);
        return result;
    }
    sock::setNonBlocking(fd);

    sockaddr_storage bound{};
    socklen_t size = sizeof(bound);
#ifdef _WIN32
    getsockname(static_cast<SOCKET>(fd), reinterpret_cast<sockaddr*>(&bound), &size);
#else
    getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &size);
#endif
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
