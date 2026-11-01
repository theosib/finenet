#pragma once

/**
 * @file listener.hpp
 * @brief Accepts network connections
 */

#include "finenet/connection.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace finenet {

struct ListenerConfig {
    /// Connections that haven't finished the handshake. Past this, new ones are refused.
    uint32_t maxHandshaking = 32;
    uint32_t handshakeTimeoutMs = 5000;
    ConnectionConfig connection{};
};

class Listener {
public:
    /**
     * @brief Listen on a TCP port, on all addresses, IPv4 and IPv6 alike
     * @param port 0 picks a free one: see port()
     * @param error Why it failed, if it did
     * @return nullptr if the port can't be had
     */
    static std::unique_ptr<Listener> listen(uint16_t port, const ListenerConfig& config = {},
                                             std::string* error = nullptr);

    ~Listener();

    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;

    [[nodiscard]] uint16_t port() const;

    /// The next connection that has finished its handshake, or nullptr. Doesn't block.
    [[nodiscard]] std::unique_ptr<Connection> accept();

    /// Signal this when accept() has something
    void setWakeSignal(WakeSignal* signal);

    void stop();

private:
    struct Impl;
    explicit Listener(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace finenet
