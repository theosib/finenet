#pragma once

/**
 * @file discovery.hpp
 * @brief Finding servers on the local network, with no configuration
 *
 * A LAN fails in many quiet ways, and most of them only break one mechanism. A switch filters
 * multicast, broadcast leaves by the wrong interface, a VPN owns the default route, a datagram gets
 * lost. So everything runs at once, and any one of them getting through is enough:
 *
 *  - The browser queries and the beacon announces, so neither side depends on the other's timing.
 *  - Each goes by multicast, by directed broadcast on every interface, and by limited broadcast.
 *  - Interfaces are read afresh each cycle, so a network that comes up later is found.
 *  - Replies are unicast to the asker, so several servers on one machine all answer.
 *  - With nothing found after a few seconds, the browser asks each host on its local /24s directly.
 *  - Servers already known are asked directly, always.
 *
 * What a server is called, how full it is and so on is an opaque listing, which the application
 * defines. A found server's addresses are those its datagrams came from, which are known to route,
 * not anything it claims about itself.
 */

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace finenet {

inline constexpr uint16_t DISCOVERY_PORT = 7811;

/**
 * @brief Makes a server findable. Runs its own thread.
 */
class DiscoveryBeacon {
public:
    /**
     * @param app Only browsers looking for this app, or for any, hear about this server
     * @param gamePort What a client should connect to
     * @param discoveryPort The port browsers ask on
     */
    DiscoveryBeacon(std::string app, uint16_t gamePort, uint16_t discoveryPort = DISCOVERY_PORT);
    ~DiscoveryBeacon();

    DiscoveryBeacon(const DiscoveryBeacon&) = delete;
    DiscoveryBeacon& operator=(const DiscoveryBeacon&) = delete;

    /// What to tell browsers. Goes out in the next answer, and at once to everyone.
    void setListing(std::vector<uint8_t> listing);

    /// Say goodbye, so that browsers forget this server now rather than when it times out
    void stop();

    [[nodiscard]] uint64_t instance() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

struct DiscoveredServer {
    std::string app;
    uint64_t instance = 0;
    uint16_t port = 0;                   ///< Game port
    std::vector<std::string> addresses;  ///< Best first: every address it's been heard from
    std::vector<uint8_t> listing;
    float rttMs = -1.0f;                 ///< From a query's answer. Negative if there's no sample.
};

/**
 * @brief Finds servers. Runs its own thread.
 */
class DiscoveryBrowser {
public:
    struct Config {
        std::string app;                    ///< Empty hears every app
        uint16_t discoveryPort = DISCOVERY_PORT;
        std::chrono::seconds expiry{10};    ///< Forget a server not heard from for this long
        bool sweep = true;                  ///< Probe the local /24s when nothing else finds anything
        std::chrono::seconds sweepAfter{3};
    };

    explicit DiscoveryBrowser(Config config);
    ~DiscoveryBrowser();

    DiscoveryBrowser(const DiscoveryBrowser&) = delete;
    DiscoveryBrowser& operator=(const DiscoveryBrowser&) = delete;

    /// Ask this host directly from now on, as well as everyone else. For remembered servers.
    void addProbe(std::string host, uint16_t discoveryPort = DISCOVERY_PORT);

    /// Ask again now, as a refresh button should
    void refresh();

    [[nodiscard]] std::vector<DiscoveredServer> servers() const;

    /// Every send failed with a permission or routing error, as when macOS has denied Local Network
    /// access. Nothing will ever be found, so the UI should say why.
    [[nodiscard]] bool blocked() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace finenet
