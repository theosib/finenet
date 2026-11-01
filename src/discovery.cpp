#include "finenet/discovery.hpp"

#include "stream_transport.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <thread>

#include <arpa/inet.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace finenet {

namespace {

using Clock = std::chrono::steady_clock;

constexpr char MAGIC[4] = {'F', 'N', 'D', 'S'};
constexpr uint8_t VERSION = 1;
constexpr uint8_t KIND_QUERY = 0;
constexpr uint8_t KIND_ANNOUNCE = 1;
constexpr uint8_t KIND_BYE = 2;

constexpr const char* GROUP_V4 = "239.255.70.86";
constexpr const char* GROUP_V6 = "ff02::4656:5854";

constexpr auto ANNOUNCE_INTERVAL = std::chrono::seconds(2);
constexpr auto SWEEP_INTERVAL = std::chrono::seconds(15);
constexpr size_t MAX_DATAGRAM = 1400;
/// Datagrams of a sweep at a go, so that a /24 doesn't land on a Wi-Fi link in one burst
constexpr int SWEEP_BATCH = 16;

/// Delays between queries while looking: fast, because the first answer is what people see
constexpr int QUERY_SCHEDULE_MS[] = {0, 250, 750, 1000, 2000};
constexpr auto QUERY_SETTLED = std::chrono::seconds(5);

struct Interface {
    std::string name;
    unsigned index = 0;
    in_addr address{};
    in_addr netmask{};
    in_addr broadcast{};
    bool canBroadcast = false;
    bool canMulticast = false;
    bool pointToPoint = false;
};

std::vector<Interface> ipv4Interfaces() {
    std::vector<Interface> out;
    ifaddrs* list = nullptr;
    if (getifaddrs(&list) != 0) return out;
    for (ifaddrs* entry = list; entry; entry = entry->ifa_next) {
        if (!entry->ifa_addr || entry->ifa_addr->sa_family != AF_INET) continue;
        if (!(entry->ifa_flags & IFF_UP) || !(entry->ifa_flags & IFF_RUNNING)) continue;
        if (entry->ifa_flags & IFF_LOOPBACK) continue;

        Interface iface;
        iface.name = entry->ifa_name;
        iface.index = if_nametoindex(entry->ifa_name);
        iface.address = reinterpret_cast<sockaddr_in*>(entry->ifa_addr)->sin_addr;
        if (entry->ifa_netmask) iface.netmask = reinterpret_cast<sockaddr_in*>(entry->ifa_netmask)->sin_addr;
        iface.canMulticast = (entry->ifa_flags & IFF_MULTICAST) != 0;
        iface.pointToPoint = (entry->ifa_flags & IFF_POINTOPOINT) != 0;
        if ((entry->ifa_flags & IFF_BROADCAST) && entry->ifa_broadaddr) {
            iface.canBroadcast = true;
            iface.broadcast = reinterpret_cast<sockaddr_in*>(entry->ifa_broadaddr)->sin_addr;
        }
        out.push_back(iface);
    }
    freeifaddrs(list);
    return out;
}

std::vector<unsigned> ipv6Interfaces() {
    std::vector<unsigned> out;
    ifaddrs* list = nullptr;
    if (getifaddrs(&list) != 0) return out;
    for (ifaddrs* entry = list; entry; entry = entry->ifa_next) {
        if (!entry->ifa_addr || entry->ifa_addr->sa_family != AF_INET6) continue;
        if (!(entry->ifa_flags & IFF_UP) || !(entry->ifa_flags & IFF_RUNNING)) continue;
        if (!(entry->ifa_flags & IFF_MULTICAST) || (entry->ifa_flags & IFF_LOOPBACK)) continue;
        unsigned index = if_nametoindex(entry->ifa_name);
        if (std::find(out.begin(), out.end(), index) == out.end()) out.push_back(index);
    }
    freeifaddrs(list);
    return out;
}

void setCommonOptions(int fd) {
    detail::prepareSocket(fd);
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
#ifdef SO_REUSEPORT
    // Everyone on the machine who wants the discovery port gets to share it
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
#endif
}

/// A socket on the discovery port, for hearing what's sent to the group or broadcast
int openBoundV4(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return -1;
    setCommonOptions(fd);
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_BROADCAST, &on, sizeof(on));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

int openBoundV6(uint16_t port) {
    int fd = ::socket(AF_INET6, SOCK_DGRAM, 0);
    if (fd < 0) return -1;
    setCommonOptions(fd);
    int on = 1;
    setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &on, sizeof(on));
    sockaddr_in6 addr{};
    addr.sin6_family = AF_INET6;
    addr.sin6_addr = in6addr_any;
    addr.sin6_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

/// Tracks joined groups, so that a network that came up since last time gets joined too
void joinGroups(int fd4, int fd6, std::vector<uint32_t>& joined4, std::vector<unsigned>& joined6) {
    if (fd4 >= 0) {
        for (const Interface& iface : ipv4Interfaces()) {
            if (!iface.canMulticast) continue;
            if (std::find(joined4.begin(), joined4.end(), iface.address.s_addr) != joined4.end()) continue;
            ip_mreq request{};
            inet_pton(AF_INET, GROUP_V4, &request.imr_multiaddr);
            request.imr_interface = iface.address;
            if (setsockopt(fd4, IPPROTO_IP, IP_ADD_MEMBERSHIP, &request, sizeof(request)) == 0) {
                joined4.push_back(iface.address.s_addr);
            }
        }
    }
    if (fd6 >= 0) {
        for (unsigned index : ipv6Interfaces()) {
            if (std::find(joined6.begin(), joined6.end(), index) != joined6.end()) continue;
            ipv6_mreq request{};
            inet_pton(AF_INET6, GROUP_V6, &request.ipv6mr_multiaddr);
            request.ipv6mr_interface = index;
            if (setsockopt(fd6, IPPROTO_IPV6, IPV6_JOIN_GROUP, &request, sizeof(request)) == 0) {
                joined6.push_back(index);
            }
        }
    }
}

struct SendTally {
    int attempts = 0;
    int blocked = 0;
    void note(ssize_t result) {
        ++attempts;
        // What a send gets when the OS has refused this process the network
        if (result < 0 && (errno == EHOSTUNREACH || errno == EPERM || errno == EACCES ||
                           errno == ENETUNREACH)) {
            ++blocked;
        }
    }
};

/// One datagram to everyone who might hear it, once by each way there is
void sendEverywhere(int fd4, int fd6, const std::vector<uint8_t>& datagram, uint16_t port, SendTally& tally) {
    if (fd4 >= 0) {
        sockaddr_in target{};
        target.sin_family = AF_INET;
        target.sin_port = htons(port);

        for (const Interface& iface : ipv4Interfaces()) {
            if (iface.canMulticast) {
                setsockopt(fd4, IPPROTO_IP, IP_MULTICAST_IF, &iface.address, sizeof(iface.address));
                inet_pton(AF_INET, GROUP_V4, &target.sin_addr);
                tally.note(sendto(fd4, datagram.data(), datagram.size(), 0,
                                  reinterpret_cast<sockaddr*>(&target), sizeof(target)));
            }
            if (iface.canBroadcast) {
                target.sin_addr = iface.broadcast;
                tally.note(sendto(fd4, datagram.data(), datagram.size(), 0,
                                  reinterpret_cast<sockaddr*>(&target), sizeof(target)));
            }
        }

        target.sin_addr.s_addr = htonl(INADDR_BROADCAST);
        sendto(fd4, datagram.data(), datagram.size(), 0, reinterpret_cast<sockaddr*>(&target), sizeof(target));

        // A server on this very machine, with no network at all
        target.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        sendto(fd4, datagram.data(), datagram.size(), 0, reinterpret_cast<sockaddr*>(&target), sizeof(target));
    }

    if (fd6 >= 0) {
        sockaddr_in6 target{};
        target.sin6_family = AF_INET6;
        target.sin6_port = htons(port);
        inet_pton(AF_INET6, GROUP_V6, &target.sin6_addr);
        for (unsigned index : ipv6Interfaces()) {
            setsockopt(fd6, IPPROTO_IPV6, IPV6_MULTICAST_IF, &index, sizeof(index));
            target.sin6_scope_id = index;
            sendto(fd6, datagram.data(), datagram.size(), 0, reinterpret_cast<sockaddr*>(&target), sizeof(target));
        }
    }
}

void appendU16(std::vector<uint8_t>& out, uint16_t value) {
    out.push_back(static_cast<uint8_t>(value));
    out.push_back(static_cast<uint8_t>(value >> 8));
}

void appendVar(std::vector<uint8_t>& out, uint64_t value) {
    while (value >= 0x80) {
        out.push_back(static_cast<uint8_t>(value) | 0x80);
        value >>= 7;
    }
    out.push_back(static_cast<uint8_t>(value));
}

std::vector<uint8_t> encodeDatagram(uint8_t kind, std::string_view app, uint64_t instance, uint16_t gamePort,
                                    std::span<const uint8_t> listing) {
    std::vector<uint8_t> out;
    out.insert(out.end(), MAGIC, MAGIC + 4);
    out.push_back(VERSION);
    out.push_back(kind);
    appendVar(out, app.size());
    out.insert(out.end(), app.begin(), app.end());
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<uint8_t>(instance >> (i * 8)));
    appendU16(out, gamePort);
    appendVar(out, listing.size());
    out.insert(out.end(), listing.begin(), listing.end());
    return out;
}

struct Datagram {
    uint8_t kind = 0;
    std::string app;
    uint64_t instance = 0;
    uint16_t gamePort = 0;
    std::vector<uint8_t> listing;
};

std::optional<uint64_t> readVar(const uint8_t* data, size_t size, size_t& offset) {
    uint64_t value = 0;
    for (int shift = 0; shift < 64; shift += 7) {
        if (offset >= size) return std::nullopt;
        uint8_t byte = data[offset++];
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if (!(byte & 0x80)) return value;
    }
    return std::nullopt;
}

std::optional<Datagram> decodeDatagram(const uint8_t* data, size_t size) {
    if (size < 6 || std::memcmp(data, MAGIC, 4) != 0 || data[4] != VERSION) return std::nullopt;
    Datagram out;
    out.kind = data[5];
    size_t offset = 6;
    auto appSize = readVar(data, size, offset);
    if (!appSize || offset + *appSize + 10 > size) return std::nullopt;
    out.app.assign(reinterpret_cast<const char*>(data + offset), *appSize);
    offset += *appSize;
    for (int i = 0; i < 8; ++i) out.instance |= static_cast<uint64_t>(data[offset + i]) << (i * 8);
    offset += 8;
    out.gamePort = static_cast<uint16_t>(data[offset] | (data[offset + 1] << 8));
    offset += 2;
    auto listingSize = readVar(data, size, offset);
    if (!listingSize || offset + *listingSize > size) return std::nullopt;
    out.listing.assign(data + offset, data + offset + *listingSize);
    return out;
}

std::string addressText(const sockaddr_storage& from) {
    char text[INET6_ADDRSTRLEN] = {0};
    if (from.ss_family == AF_INET) {
        inet_ntop(AF_INET, &reinterpret_cast<const sockaddr_in*>(&from)->sin_addr, text, sizeof(text));
        return text;
    }
    if (from.ss_family == AF_INET6) {
        const auto* v6 = reinterpret_cast<const sockaddr_in6*>(&from);
        inet_ntop(AF_INET6, &v6->sin6_addr, text, sizeof(text));
        std::string out = text;
        // A link-local address means nothing without the interface it's on
        if (IN6_IS_ADDR_LINKLOCAL(&v6->sin6_addr) && v6->sin6_scope_id != 0) {
            char name[IF_NAMESIZE] = {0};
            if (if_indextoname(v6->sin6_scope_id, name)) out += std::string("%") + name;
        }
        return out;
    }
    return {};
}

/// Lower is better: a real LAN first, then link-local, then this machine
int addressRank(const std::string& text) {
    if (text == "127.0.0.1" || text == "::1") return 3;
    if (text.find(':') != std::string::npos) return 2;
    if (text.rfind("169.254.", 0) == 0) return 1;
    return 0;
}

}  // namespace

// ============================================================================
// DiscoveryBeacon
// ============================================================================

struct DiscoveryBeacon::Impl {
    std::string app;
    uint16_t gamePort = 0;
    uint16_t discoveryPort = 0;
    uint64_t instance = 0;

    int fd4 = -1, fd6 = -1;
    std::vector<uint32_t> joined4;
    std::vector<unsigned> joined6;

    std::mutex mutex;
    std::condition_variable changed;
    std::vector<uint8_t> listing;
    bool stopping = false;
    bool listingChanged = false;
    std::thread thread;

    void announce() {
        std::vector<uint8_t> copy;
        {
            std::lock_guard lock(mutex);
            copy = listing;
        }
        SendTally tally;
        sendEverywhere(fd4, fd6, encodeDatagram(KIND_ANNOUNCE, app, instance, gamePort, copy), discoveryPort, tally);
    }

    void answer(int fd, const sockaddr_storage& to, socklen_t size) {
        std::vector<uint8_t> copy;
        {
            std::lock_guard lock(mutex);
            copy = listing;
        }
        auto bytes = encodeDatagram(KIND_ANNOUNCE, app, instance, gamePort, copy);
        sendto(fd, bytes.data(), bytes.size(), 0, reinterpret_cast<const sockaddr*>(&to), size);
    }

    void drain(int fd) {
        uint8_t buffer[MAX_DATAGRAM + 64];
        for (;;) {
            sockaddr_storage from{};
            socklen_t size = sizeof(from);
            ssize_t got = recvfrom(fd, buffer, sizeof(buffer), 0, reinterpret_cast<sockaddr*>(&from), &size);
            if (got <= 0) return;
            auto datagram = decodeDatagram(buffer, static_cast<size_t>(got));
            if (!datagram || datagram->kind != KIND_QUERY) continue;
            if (!datagram->app.empty() && datagram->app != app) continue;
            answer(fd, from, size);
        }
    }

    void run() {
        auto nextAnnounce = Clock::now();
        while (true) {
            {
                std::lock_guard lock(mutex);
                if (stopping) break;
            }
            auto now = Clock::now();
            bool due = now >= nextAnnounce;
            {
                std::lock_guard lock(mutex);
                due = due || std::exchange(listingChanged, false);
            }
            if (due) {
                joinGroups(fd4, fd6, joined4, joined6);
                announce();
                nextAnnounce = now + ANNOUNCE_INTERVAL;
            }

            pollfd fds[2] = {{fd4, POLLIN, 0}, {fd6, POLLIN, 0}};
            poll(fds, 2, 100);
            if (fds[0].revents & POLLIN) drain(fd4);
            if (fds[1].revents & POLLIN) drain(fd6);
        }

        std::vector<uint8_t> bye = encodeDatagram(KIND_BYE, app, instance, gamePort, {});
        SendTally tally;
        sendEverywhere(fd4, fd6, bye, discoveryPort, tally);
    }

    void stop() {
        {
            std::lock_guard lock(mutex);
            stopping = true;
        }
        if (thread.joinable()) thread.join();
        if (fd4 >= 0) ::close(fd4);
        if (fd6 >= 0) ::close(fd6);
        fd4 = fd6 = -1;
    }
};

DiscoveryBeacon::DiscoveryBeacon(std::string app, uint16_t gamePort, uint16_t discoveryPort)
    : impl_(std::make_unique<Impl>()) {
    impl_->app = std::move(app);
    impl_->gamePort = gamePort;
    impl_->discoveryPort = discoveryPort;
    impl_->instance = (static_cast<uint64_t>(std::random_device{}()) << 32) | std::random_device{}();
    impl_->fd4 = openBoundV4(discoveryPort);
    impl_->fd6 = openBoundV6(discoveryPort);
    Impl* raw = impl_.get();
    impl_->thread = std::thread([raw] { raw->run(); });
}

DiscoveryBeacon::~DiscoveryBeacon() {
    impl_->stop();
}

void DiscoveryBeacon::setListing(std::vector<uint8_t> listing) {
    std::lock_guard lock(impl_->mutex);
    impl_->listing = std::move(listing);
    impl_->listingChanged = true;
}

void DiscoveryBeacon::stop() {
    impl_->stop();
}

uint64_t DiscoveryBeacon::instance() const {
    return impl_->instance;
}

// ============================================================================
// DiscoveryBrowser
// ============================================================================

struct DiscoveryBrowser::Impl {
    Config config;

    int fd4 = -1, fd6 = -1;          // Bound to the discovery port, to overhear announcements
    int reply4 = -1, reply6 = -1;    // Ephemeral, for answers to our own queries
    std::vector<uint32_t> joined4;
    std::vector<unsigned> joined6;

    struct Known {
        DiscoveredServer server;
        Clock::time_point lastHeard;
    };

    mutable std::mutex mutex;
    std::map<uint64_t, Known> known;
    std::vector<std::pair<std::string, uint16_t>> probes;
    bool stopping = false;
    bool refreshRequested = false;
    std::atomic<bool> blockedFlag{false};
    std::thread thread;

    Clock::time_point lastQuery{};
    uint32_t sweepHost = 0;       // Next host of the sweep in progress
    size_t sweepInterface = 0;
    Clock::time_point nextSweep{};

    void query() {
        auto bytes = encodeDatagram(KIND_QUERY, config.app, 0, 0, {});
        SendTally tally;
        sendEverywhere(reply4, reply6, bytes, config.discoveryPort, tally);
        if (tally.attempts > 0) blockedFlag = tally.blocked == tally.attempts;

        std::vector<std::pair<std::string, uint16_t>> targets;
        {
            std::lock_guard lock(mutex);
            targets = probes;
        }
        for (const auto& [host, port] : targets) probe(bytes, host, port);
        lastQuery = Clock::now();
    }

    void probe(const std::vector<uint8_t>& bytes, const std::string& host, uint16_t port) {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_DGRAM;
        hints.ai_flags = AI_NUMERICHOST;
        addrinfo* found = nullptr;
        // Names would block this thread, so only addresses are probed. Remembered servers are
        // kept by the address they were last seen on.
        if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &found) != 0) return;
        for (addrinfo* entry = found; entry; entry = entry->ai_next) {
            int fd = entry->ai_family == AF_INET6 ? reply6 : reply4;
            if (fd >= 0) sendto(fd, bytes.data(), bytes.size(), 0, entry->ai_addr, entry->ai_addrlen);
        }
        freeaddrinfo(found);
    }

    /// A little of a slow sweep of the local /24s, for networks that block everything else
    void sweepSome() {
        if (!config.sweep || reply4 < 0) return;
        auto now = Clock::now();
        if (now < nextSweep) return;

        std::vector<Interface> interfaces;
        for (const Interface& iface : ipv4Interfaces()) {
            // A VPN's peers aren't neighbours, and a point-to-point link has none
            if (!iface.pointToPoint && iface.canBroadcast) interfaces.push_back(iface);
        }
        if (sweepInterface >= interfaces.size()) {
            sweepInterface = 0;
            sweepHost = 0;
            nextSweep = now + SWEEP_INTERVAL;
            return;
        }

        auto bytes = encodeDatagram(KIND_QUERY, config.app, 0, 0, {});
        const Interface& iface = interfaces[sweepInterface];
        uint32_t base = ntohl(iface.address.s_addr) & 0xFFFFFF00u;
        sockaddr_in target{};
        target.sin_family = AF_INET;
        target.sin_port = htons(config.discoveryPort);
        for (int i = 0; i < SWEEP_BATCH && sweepHost < 254; ++i) {
            target.sin_addr.s_addr = htonl(base + ++sweepHost);
            sendto(reply4, bytes.data(), bytes.size(), 0, reinterpret_cast<sockaddr*>(&target), sizeof(target));
        }
        if (sweepHost >= 254) {
            sweepHost = 0;
            ++sweepInterface;
        }
    }

    void receive(int fd, bool fromQuery) {
        uint8_t buffer[MAX_DATAGRAM + 64];
        for (;;) {
            sockaddr_storage from{};
            socklen_t size = sizeof(from);
            ssize_t got = recvfrom(fd, buffer, sizeof(buffer), 0, reinterpret_cast<sockaddr*>(&from), &size);
            if (got <= 0) return;
            auto datagram = decodeDatagram(buffer, static_cast<size_t>(got));
            if (!datagram) continue;
            if (!config.app.empty() && datagram->app != config.app) continue;

            std::lock_guard lock(mutex);
            if (datagram->kind == KIND_BYE) {
                known.erase(datagram->instance);
                continue;
            }
            if (datagram->kind != KIND_ANNOUNCE) continue;

            Known& entry = known[datagram->instance];
            entry.lastHeard = Clock::now();
            entry.server.app = datagram->app;
            entry.server.instance = datagram->instance;
            entry.server.port = datagram->gamePort;
            entry.server.listing = std::move(datagram->listing);
            if (fromQuery) {
                float ms = std::chrono::duration<float, std::milli>(Clock::now() - lastQuery).count();
                if (entry.server.rttMs < 0.0f || ms < entry.server.rttMs) entry.server.rttMs = ms;
            }
            std::string address = addressText(from);
            auto& addresses = entry.server.addresses;
            if (!address.empty() && std::find(addresses.begin(), addresses.end(), address) == addresses.end()) {
                addresses.push_back(std::move(address));
                std::stable_sort(addresses.begin(), addresses.end(), [](const std::string& a, const std::string& b) {
                    return addressRank(a) < addressRank(b);
                });
            }
        }
    }

    void forgetSilent() {
        auto cutoff = Clock::now() - config.expiry;
        std::lock_guard lock(mutex);
        std::erase_if(known, [&](const auto& entry) { return entry.second.lastHeard < cutoff; });
    }

    void run() {
        auto started = Clock::now();
        size_t step = 0;
        auto nextQuery = started;
        auto nextJoin = started;

        for (;;) {
            bool refresh;
            {
                std::lock_guard lock(mutex);
                if (stopping) return;
                refresh = std::exchange(refreshRequested, false);
            }
            if (refresh) {
                step = 0;
                nextQuery = Clock::now();
            }

            auto now = Clock::now();
            if (now >= nextJoin) {
                joinGroups(fd4, fd6, joined4, joined6);
                nextJoin = now + std::chrono::seconds(2);
            }
            if (now >= nextQuery) {
                query();
                ++step;
                constexpr size_t STEPS = std::size(QUERY_SCHEDULE_MS);
                nextQuery = now + (step < STEPS ? std::chrono::duration_cast<Clock::duration>(
                                                      std::chrono::milliseconds(QUERY_SCHEDULE_MS[step]))
                                                : std::chrono::duration_cast<Clock::duration>(QUERY_SETTLED));
            }

            bool empty;
            {
                std::lock_guard lock(mutex);
                empty = known.empty();
            }
            if (empty && now - started >= config.sweepAfter) sweepSome();

            pollfd fds[4] = {{fd4, POLLIN, 0}, {fd6, POLLIN, 0}, {reply4, POLLIN, 0}, {reply6, POLLIN, 0}};
            poll(fds, 4, 50);
            if (fds[0].revents & POLLIN) receive(fd4, false);
            if (fds[1].revents & POLLIN) receive(fd6, false);
            if (fds[2].revents & POLLIN) receive(reply4, true);
            if (fds[3].revents & POLLIN) receive(reply6, true);
            forgetSilent();
        }
    }
};

DiscoveryBrowser::DiscoveryBrowser(Config config) : impl_(std::make_unique<Impl>()) {
    impl_->config = std::move(config);
    impl_->fd4 = openBoundV4(impl_->config.discoveryPort);
    impl_->fd6 = openBoundV6(impl_->config.discoveryPort);
    impl_->reply4 = openBoundV4(0);
    impl_->reply6 = openBoundV6(0);
    Impl* raw = impl_.get();
    impl_->thread = std::thread([raw] { raw->run(); });
}

DiscoveryBrowser::~DiscoveryBrowser() {
    {
        std::lock_guard lock(impl_->mutex);
        impl_->stopping = true;
    }
    if (impl_->thread.joinable()) impl_->thread.join();
    for (int fd : {impl_->fd4, impl_->fd6, impl_->reply4, impl_->reply6}) {
        if (fd >= 0) ::close(fd);
    }
}

void DiscoveryBrowser::addProbe(std::string host, uint16_t discoveryPort) {
    std::lock_guard lock(impl_->mutex);
    impl_->probes.emplace_back(std::move(host), discoveryPort);
    impl_->refreshRequested = true;
}

void DiscoveryBrowser::refresh() {
    std::lock_guard lock(impl_->mutex);
    impl_->refreshRequested = true;
}

std::vector<DiscoveredServer> DiscoveryBrowser::servers() const {
    std::lock_guard lock(impl_->mutex);
    std::vector<DiscoveredServer> out;
    out.reserve(impl_->known.size());
    for (const auto& [instance, entry] : impl_->known) out.push_back(entry.server);
    return out;
}

bool DiscoveryBrowser::blocked() const {
    return impl_->blockedFlag.load();
}

}  // namespace finenet
