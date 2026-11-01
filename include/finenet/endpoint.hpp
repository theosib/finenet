#pragma once

/**
 * @file endpoint.hpp
 * @brief Where to connect, as people type it
 */

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace finenet {

struct Endpoint {
    std::string scheme;  ///< Empty if none was given
    std::string host;
    uint16_t port = 0;

    /// `scheme://host:port`, or as much of that as is known. IPv6 hosts get their brackets back.
    [[nodiscard]] std::string toString() const;
};

/**
 * @brief Parse an address. All of these work:
 *
 *   192.168.1.5    host.local:7810    [fe80::1]:7810    ::1    finevox://host:7810/
 *
 * The scheme and port are optional, and a missing port becomes `defaultPort`. nullopt if there's
 * no host, or the port isn't a number from 1 to 65535.
 */
[[nodiscard]] std::optional<Endpoint> parseEndpoint(std::string_view text, uint16_t defaultPort = 0);

}  // namespace finenet
