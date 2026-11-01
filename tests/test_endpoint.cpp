#include <gtest/gtest.h>
#include "finenet/endpoint.hpp"

using namespace finenet;

TEST(EndpointTest, TakesAddressesAsPeopleTypeThem) {
    struct Case {
        const char* text;
        const char* host;
        uint16_t port;
    };
    for (const Case& c : {Case{"192.168.1.5", "192.168.1.5", 7810}, Case{"host.local:9000", "host.local", 9000},
                          Case{"[fe80::1]:9000", "fe80::1", 9000}, Case{"[::1]", "::1", 7810},
                          Case{"::1", "::1", 7810}, Case{"  10.0.0.2:81\n", "10.0.0.2", 81},
                          Case{"finevox://example.com:9000/", "example.com", 9000},
                          Case{"finevox://example.com", "example.com", 7810}}) {
        auto parsed = parseEndpoint(c.text, 7810);
        ASSERT_TRUE(parsed.has_value()) << c.text;
        EXPECT_EQ(parsed->host, c.host) << c.text;
        EXPECT_EQ(parsed->port, c.port) << c.text;
    }
}

TEST(EndpointTest, RefusesWhatCantBeAnAddress) {
    for (const char* text : {"", "   ", ":7810", "host:", "host:0", "host:70000", "host:abc", "[::1", "[::1]x"}) {
        EXPECT_FALSE(parseEndpoint(text, 7810).has_value()) << text;
    }
}

TEST(EndpointTest, KeepsTheScheme) {
    auto parsed = parseEndpoint("wss://hub.example:443/lobby");
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->scheme, "wss");
    EXPECT_EQ(parsed->toString(), "wss://hub.example:443");
}

TEST(EndpointTest, ToStringPutsIpv6BracketsBack) {
    auto parsed = parseEndpoint("[fe80::1]:9000");
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->toString(), "[fe80::1]:9000");
}
