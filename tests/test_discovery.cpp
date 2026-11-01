#include <gtest/gtest.h>
#include "finenet/discovery.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <random>
#include <string>
#include <thread>

using namespace finenet;
using namespace std::chrono_literals;

namespace {

template <typename Predicate>
bool waitFor(Predicate predicate, std::chrono::milliseconds limit = 5000ms) {
    auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(5ms);
    }
    return predicate();
}

std::vector<uint8_t> bytes(const std::string& text) {
    return {text.begin(), text.end()};
}

/// A port no other run of these tests is using
uint16_t freshPort() {
    static uint16_t base = static_cast<uint16_t>(40000 + std::random_device{}() % 20000);
    return ++base;
}

DiscoveryBrowser::Config config(const std::string& app, uint16_t port) {
    DiscoveryBrowser::Config out;
    out.app = app;
    out.discoveryPort = port;
    out.sweep = false;
    return out;
}

/// Send raw bytes to the discovery port, as a peer with no manners might
void sendRaw(const std::vector<uint8_t>& datagram, uint16_t port) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in target{};
    target.sin_family = AF_INET;
    target.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    target.sin_port = htons(port);
    sendto(fd, datagram.data(), datagram.size(), 0, reinterpret_cast<sockaddr*>(&target), sizeof(target));
    close(fd);
}

/// An Announce from a server that will never be heard from again, and won't say goodbye
std::vector<uint8_t> announceOnce(const std::string& app, uint64_t instance) {
    std::vector<uint8_t> out = {'F', 'N', 'D', 'S', 1, 1};
    out.push_back(static_cast<uint8_t>(app.size()));
    out.insert(out.end(), app.begin(), app.end());
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<uint8_t>(instance >> (i * 8)));
    out.push_back(0x82);  // Game port 7810
    out.push_back(0x1E);
    out.push_back(0);     // No listing
    return out;
}

const DiscoveredServer* find(const std::vector<DiscoveredServer>& servers, uint64_t instance) {
    for (const auto& server : servers) {
        if (server.instance == instance) return &server;
    }
    return nullptr;
}

}  // namespace

TEST(DiscoveryTest, FindsAServerWithItsListing) {
    uint16_t port = freshPort();
    DiscoveryBeacon beacon("testapp", 7810, port);
    beacon.setListing(bytes("my server"));
    DiscoveryBrowser browser(config("testapp", port));

    ASSERT_TRUE(waitFor([&] { return find(browser.servers(), beacon.instance()) != nullptr; }));
    auto servers = browser.servers();
    const auto* found = find(servers, beacon.instance());
    EXPECT_EQ(found->port, 7810);
    EXPECT_EQ(found->app, "testapp");
    EXPECT_EQ(found->listing, bytes("my server"));
    EXPECT_FALSE(found->addresses.empty());
}

TEST(DiscoveryTest, FindsAServerThatStartsAfterTheBrowser) {
    uint16_t port = freshPort();
    DiscoveryBrowser browser(config("testapp", port));
    std::this_thread::sleep_for(200ms);

    DiscoveryBeacon beacon("testapp", 7810, port);
    EXPECT_TRUE(waitFor([&] { return find(browser.servers(), beacon.instance()) != nullptr; }));
}

TEST(DiscoveryTest, FindsEveryServerOnOneMachine) {
    uint16_t port = freshPort();
    DiscoveryBeacon first("testapp", 7001, port);
    DiscoveryBeacon second("testapp", 7002, port);
    DiscoveryBeacon third("testapp", 7003, port);
    DiscoveryBrowser browser(config("testapp", port));

    EXPECT_TRUE(waitFor([&] {
        auto servers = browser.servers();
        return find(servers, first.instance()) && find(servers, second.instance()) &&
               find(servers, third.instance());
    }));
}

TEST(DiscoveryTest, OtherAppsAreIgnored) {
    uint16_t port = freshPort();
    DiscoveryBeacon other("otherapp", 7810, port);
    DiscoveryBeacon mine("testapp", 7811, port);
    DiscoveryBrowser browser(config("testapp", port));

    ASSERT_TRUE(waitFor([&] { return find(browser.servers(), mine.instance()) != nullptr; }));
    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(find(browser.servers(), other.instance()), nullptr);
}

TEST(DiscoveryTest, AnEmptyAppHearsThemAll) {
    uint16_t port = freshPort();
    DiscoveryBeacon first("one", 7810, port);
    DiscoveryBeacon second("two", 7811, port);
    DiscoveryBrowser browser(config("", port));

    EXPECT_TRUE(waitFor([&] {
        auto servers = browser.servers();
        return find(servers, first.instance()) && find(servers, second.instance());
    }));
}

TEST(DiscoveryTest, AChangedListingArrivesPromptly) {
    uint16_t port = freshPort();
    DiscoveryBeacon beacon("testapp", 7810, port);
    beacon.setListing(bytes("before"));
    DiscoveryBrowser browser(config("testapp", port));
    ASSERT_TRUE(waitFor([&] { return find(browser.servers(), beacon.instance()) != nullptr; }));

    beacon.setListing(bytes("after"));
    EXPECT_TRUE(waitFor([&] {
        auto servers = browser.servers();
        const auto* found = find(servers, beacon.instance());
        return found && found->listing == bytes("after");
    }, 1500ms));
}

TEST(DiscoveryTest, AServerThatSaysGoodbyeGoesAtOnce) {
    uint16_t port = freshPort();
    DiscoveryBrowser browser(config("testapp", port));
    uint64_t instance;
    {
        DiscoveryBeacon beacon("testapp", 7810, port);
        instance = beacon.instance();
        ASSERT_TRUE(waitFor([&] { return find(browser.servers(), instance) != nullptr; }));
    }
    EXPECT_TRUE(waitFor([&] { return find(browser.servers(), instance) == nullptr; }, 1000ms));
}

TEST(DiscoveryTest, AServerThatJustVanishesIsForgotten) {
    uint16_t port = freshPort();
    auto settings = config("testapp", port);
    settings.expiry = 1s;
    DiscoveryBrowser browser(settings);
    std::this_thread::sleep_for(100ms);

    constexpr uint64_t INSTANCE = 0x1234567890ABCDEFull;
    sendRaw(announceOnce("testapp", INSTANCE), port);
    ASSERT_TRUE(waitFor([&] { return find(browser.servers(), INSTANCE) != nullptr; }, 1000ms));
    EXPECT_TRUE(waitFor([&] { return find(browser.servers(), INSTANCE) == nullptr; }, 3000ms));
}

TEST(DiscoveryTest, ADirectProbeFindsAServer) {
    uint16_t port = freshPort();
    DiscoveryBeacon beacon("testapp", 7810, port);
    auto settings = config("testapp", port + 1000);  // Looking in the wrong place altogether
    DiscoveryBrowser browser(settings);
    browser.addProbe("127.0.0.1", port);
    EXPECT_TRUE(waitFor([&] { return find(browser.servers(), beacon.instance()) != nullptr; }));
}

TEST(DiscoveryTest, GarbageIsIgnored) {
    uint16_t port = freshPort();
    DiscoveryBeacon beacon("testapp", 7810, port);
    DiscoveryBrowser browser(config("testapp", port));
    std::this_thread::sleep_for(100ms);

    auto valid = announceOnce("testapp", 99);
    sendRaw({}, port);
    sendRaw({'F', 'N', 'D', 'S'}, port);
    sendRaw({'F', 'N', 'D', 'S', 1, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, port);
    sendRaw(std::vector<uint8_t>(valid.begin(), valid.end() - 4), port);
    std::vector<uint8_t> noise(1200);
    std::mt19937 random(1);
    for (auto& byte : noise) byte = static_cast<uint8_t>(random());
    sendRaw(noise, port);

    // Both are still alive, and the real server is found
    ASSERT_TRUE(waitFor([&] { return find(browser.servers(), beacon.instance()) != nullptr; }));
    EXPECT_EQ(browser.servers().size(), 1u);
    EXPECT_FALSE(browser.blocked());
}
