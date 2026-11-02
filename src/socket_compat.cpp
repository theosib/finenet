#include "socket_compat.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>

#ifdef _WIN32
#include <mswsock.h>
#endif

namespace finenet::sock {

#ifdef _WIN32

void startup() {
    static std::once_flag once;
    std::call_once(once, [] {
        WSADATA data;
        WSAStartup(MAKEWORD(2, 2), &data);
    });
}

int lastError() {
    return WSAGetLastError();
}

std::string errorText(int error) {
    char* text = nullptr;
    DWORD size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
                                    FORMAT_MESSAGE_IGNORE_INSERTS,
                                nullptr, static_cast<DWORD>(error), 0, reinterpret_cast<char*>(&text), 0, nullptr);
    std::string out = size && text ? std::string(text, size) : "error " + std::to_string(error);
    if (text) LocalFree(text);
    while (!out.empty() && (out.back() == '\n' || out.back() == '\r' || out.back() == '.')) out.pop_back();
    return out;
}

bool wouldBlock(int error) {
    return error == WSAEWOULDBLOCK || error == WSAEINTR;
}

bool connectPending(int error) {
    return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS;
}

bool addressInUse(int error) {
    return error == WSAEADDRINUSE || error == WSAEACCES;
}

bool networkRefused(int error) {
    return error == WSAEHOSTUNREACH || error == WSAEACCES || error == WSAENETUNREACH;
}

int timedOut() {
    return WSAETIMEDOUT;
}

int open(int family, int type, int protocol) {
    startup();
    SOCKET s = WSASocketW(family, type, protocol, nullptr, 0, WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT);
    return s == INVALID_SOCKET ? -1 : static_cast<int>(s);
}

int accept(int fd, sockaddr* address, socklen_t* size) {
    SOCKET s = ::accept(static_cast<SOCKET>(fd), address, size);
    return s == INVALID_SOCKET ? -1 : static_cast<int>(s);
}

int connect(int fd, const sockaddr* address, size_t size) {
    return ::connect(static_cast<SOCKET>(fd), address, static_cast<int>(size));
}

void close(int fd) {
    if (fd >= 0) closesocket(static_cast<SOCKET>(fd));
}

void setNonBlocking(int fd) {
    u_long on = 1;
    ioctlsocket(static_cast<SOCKET>(fd), FIONBIO, &on);
}

int setOption(int fd, int level, int name, const void* value, size_t size) {
    return setsockopt(static_cast<SOCKET>(fd), level, name, static_cast<const char*>(value), static_cast<int>(size));
}

int getOption(int fd, int level, int name, void* value, socklen_t* size) {
    return getsockopt(static_cast<SOCKET>(fd), level, name, static_cast<char*>(value), size);
}

ssize_t send(int fd, const void* data, size_t size, int flags) {
    return ::send(static_cast<SOCKET>(fd), static_cast<const char*>(data), static_cast<int>(size), flags);
}

ssize_t recv(int fd, void* data, size_t size, int flags) {
    return ::recv(static_cast<SOCKET>(fd), static_cast<char*>(data), static_cast<int>(size), flags);
}

ssize_t sendTo(int fd, const void* data, size_t size, const sockaddr* to, size_t toSize) {
    return ::sendto(static_cast<SOCKET>(fd), static_cast<const char*>(data), static_cast<int>(size), 0, to,
                    static_cast<int>(toSize));
}

ssize_t recvFrom(int fd, void* data, size_t size, sockaddr* from, socklen_t* fromSize) {
    return ::recvfrom(static_cast<SOCKET>(fd), static_cast<char*>(data), static_cast<int>(size), 0, from, fromSize);
}

pollfd entry(int fd, short events) {
    pollfd out{};
    out.fd = fd < 0 ? INVALID_SOCKET : static_cast<SOCKET>(fd);
    out.events = events;
    return out;
}

int poll(pollfd* fds, size_t count, int timeoutMs) {
    bool any = false;
    for (size_t i = 0; i < count; ++i) {
        fds[i].revents = 0;
        if (fds[i].fd != INVALID_SOCKET) any = true;
    }
    if (!any) {
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
        return 0;
    }
    return WSAPoll(fds, static_cast<ULONG>(count), timeoutMs);
}

bool makeWakePair(int& readEnd, int& writeEnd) {
    readEnd = writeEnd = -1;
    int reader = open(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    int writer = open(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (reader < 0 || writer < 0) {
        close(reader);
        close(writer);
        return false;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t size = sizeof(addr);
    if (::bind(static_cast<SOCKET>(reader), reinterpret_cast<sockaddr*>(&addr), size) != 0 ||
        getsockname(static_cast<SOCKET>(reader), reinterpret_cast<sockaddr*>(&addr), &size) != 0 ||
        ::connect(static_cast<SOCKET>(writer), reinterpret_cast<sockaddr*>(&addr), size) != 0) {
        close(reader);
        close(writer);
        return false;
    }
    disableConnReset(reader);
    disableConnReset(writer);
    setNonBlocking(reader);
    setNonBlocking(writer);
    readEnd = reader;
    writeEnd = writer;
    return true;
}

void wakeSend(int writeEnd) {
    uint8_t byte = 1;
    // A full buffer means it's already going to wake
    send(writeEnd, &byte, 1, 0);
}

void wakeDrain(int readEnd) {
    uint8_t drain[64];
    while (recv(readEnd, drain, sizeof(drain), 0) > 0) {}
}

void disableConnReset(int fd) {
    BOOL off = FALSE;
    DWORD returned = 0;
    WSAIoctl(static_cast<SOCKET>(fd), SIO_UDP_CONNRESET, &off, sizeof(off), nullptr, 0, &returned, nullptr, nullptr);
}

#else  // POSIX

void startup() {}

int lastError() {
    return errno;
}

std::string errorText(int error) {
    return std::strerror(error);
}

bool wouldBlock(int error) {
    return error == EAGAIN || error == EWOULDBLOCK || error == EINTR;
}

bool connectPending(int error) {
    return error == EINPROGRESS;
}

bool addressInUse(int error) {
    return error == EADDRINUSE;
}

bool networkRefused(int error) {
    return error == EHOSTUNREACH || error == EPERM || error == EACCES || error == ENETUNREACH;
}

int timedOut() {
    return ETIMEDOUT;
}

int open(int family, int type, int protocol) {
    return ::socket(family, type, protocol);
}

int accept(int fd, sockaddr* address, socklen_t* size) {
    return ::accept(fd, address, size);
}

int connect(int fd, const sockaddr* address, size_t size) {
    return ::connect(fd, address, static_cast<socklen_t>(size));
}

void close(int fd) {
    if (fd >= 0) ::close(fd);
}

void setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int setOption(int fd, int level, int name, const void* value, size_t size) {
    return setsockopt(fd, level, name, value, static_cast<socklen_t>(size));
}

int getOption(int fd, int level, int name, void* value, socklen_t* size) {
    return getsockopt(fd, level, name, value, size);
}

ssize_t send(int fd, const void* data, size_t size, int flags) {
    return ::send(fd, data, size, flags);
}

ssize_t recv(int fd, void* data, size_t size, int flags) {
    return ::recv(fd, data, size, flags);
}

ssize_t sendTo(int fd, const void* data, size_t size, const sockaddr* to, size_t toSize) {
    return ::sendto(fd, data, size, 0, to, static_cast<socklen_t>(toSize));
}

ssize_t recvFrom(int fd, void* data, size_t size, sockaddr* from, socklen_t* fromSize) {
    return ::recvfrom(fd, data, size, 0, from, fromSize);
}

pollfd entry(int fd, short events) {
    pollfd out{};
    out.fd = fd;
    out.events = events;
    return out;
}

int poll(pollfd* fds, size_t count, int timeoutMs) {
    return ::poll(fds, static_cast<nfds_t>(count), timeoutMs);
}

bool makeWakePair(int& readEnd, int& writeEnd) {
    int fds[2];
    if (pipe(fds) != 0) {
        readEnd = writeEnd = -1;
        return false;
    }
    readEnd = fds[0];
    writeEnd = fds[1];
    fcntl(readEnd, F_SETFD, FD_CLOEXEC);
    fcntl(writeEnd, F_SETFD, FD_CLOEXEC);
    setNonBlocking(readEnd);
    setNonBlocking(writeEnd);
    return true;
}

void wakeSend(int writeEnd) {
    uint8_t byte = 1;
    // A full pipe means it's already going to wake
    [[maybe_unused]] auto written = ::write(writeEnd, &byte, 1);
}

void wakeDrain(int readEnd) {
    uint8_t drain[64];
    while (::read(readEnd, drain, sizeof(drain)) > 0) {}
}

void disableConnReset(int) {}

#endif

}  // namespace finenet::sock
