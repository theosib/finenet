#include "finenet/endpoint.hpp"

#include <charconv>

namespace finenet {

std::string Endpoint::toString() const {
    std::string out;
    if (!scheme.empty()) out = scheme + "://";
    bool v6 = host.find(':') != std::string::npos;
    out += v6 ? "[" + host + "]" : host;
    if (port != 0) out += ":" + std::to_string(port);
    return out;
}

std::optional<Endpoint> parseEndpoint(std::string_view text, uint16_t defaultPort) {
    // Pasted addresses come with stray whitespace
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) text.remove_prefix(1);
    while (!text.empty() && (text.back() == ' ' || text.back() == '\t' || text.back() == '\n')) {
        text.remove_suffix(1);
    }

    Endpoint out;
    out.port = defaultPort;

    if (auto at = text.find("://"); at != std::string_view::npos) {
        out.scheme = std::string(text.substr(0, at));
        text.remove_prefix(at + 3);
    }
    if (auto slash = text.find('/'); slash != std::string_view::npos) {
        text = text.substr(0, slash);
    }

    std::string_view portText;
    if (!text.empty() && text.front() == '[') {
        auto close = text.find(']');
        if (close == std::string_view::npos) return std::nullopt;
        out.host = std::string(text.substr(1, close - 1));
        std::string_view rest = text.substr(close + 1);
        if (!rest.empty()) {
            if (rest.front() != ':') return std::nullopt;
            portText = rest.substr(1);
            if (portText.empty()) return std::nullopt;
        }
    } else {
        auto first = text.find(':');
        if (first != std::string_view::npos && text.find(':', first + 1) != std::string_view::npos) {
            // Two colons with no brackets can only be a bare IPv6 address
            out.host = std::string(text);
        } else if (first != std::string_view::npos) {
            out.host = std::string(text.substr(0, first));
            portText = text.substr(first + 1);
            if (portText.empty()) return std::nullopt;
        } else {
            out.host = std::string(text);
        }
    }

    if (out.host.empty()) return std::nullopt;

    if (!portText.empty()) {
        unsigned value = 0;
        auto [end, error] = std::from_chars(portText.data(), portText.data() + portText.size(), value);
        if (error != std::errc{} || end != portText.data() + portText.size() || value == 0 ||
            value > 65535) {
            return std::nullopt;
        }
        out.port = static_cast<uint16_t>(value);
    }
    return out;
}

}  // namespace finenet
