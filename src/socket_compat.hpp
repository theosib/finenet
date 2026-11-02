#pragma once

// The few places sockets differ between POSIX and Winsock, so that the transport, listener and
// discovery read the same on both. A socket is an int everywhere: Windows' SOCKET handles fit in
// 32 bits, and INVALID_SOCKET becomes -1.

#include <cstddef>
#include <cstdint>
#include <string>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0  // macOS uses SO_NOSIGPIPE, in prepareSocket. Windows has no SIGPIPE
#endif
#if defined(_WIN32) && !defined(SHUT_RDWR)
#define SHUT_RDWR SD_BOTH
#endif

namespace finenet {

#ifdef _WIN32
using ssize_t = std::ptrdiff_t;
#endif

namespace sock {

/// Winsock needs starting before anything else. Safe to call from anywhere, any number of times.
void startup();

/// The error of the last socket call on this thread
int lastError();
std::string errorText(int error);

/// Try again later: the call would have blocked, or was interrupted
bool wouldBlock(int error);
/// A non-blocking connect that's under way
bool connectPending(int error);
bool addressInUse(int error);
/// What a send gets when the OS has refused this process the network
bool networkRefused(int error);
/// An error code meaning a wait ran out
int timedOut();

int open(int family, int type, int protocol);
int accept(int fd, sockaddr* address, socklen_t* size);
int connect(int fd, const sockaddr* address, size_t size);
void close(int fd);
void setNonBlocking(int fd);
int setOption(int fd, int level, int name, const void* value, size_t size);
int getOption(int fd, int level, int name, void* value, socklen_t* size);

ssize_t send(int fd, const void* data, size_t size, int flags);
ssize_t recv(int fd, void* data, size_t size, int flags);
ssize_t sendTo(int fd, const void* data, size_t size, const sockaddr* to, size_t toSize);
ssize_t recvFrom(int fd, void* data, size_t size, sockaddr* from, socklen_t* fromSize);

/// A poll entry. A negative fd is skipped, as POSIX poll does.
pollfd entry(int fd, short events);
/// Waits the whole timeout even when there's nothing to wait on, which WSAPoll won't
int poll(pollfd* fds, size_t count, int timeoutMs);

/// Something another thread can write to so that a poll on `readEnd` wakes: a pipe on POSIX, and
/// on Windows, where only sockets can be polled, two loopback UDP sockets.
bool makeWakePair(int& readEnd, int& writeEnd);
void wakeSend(int writeEnd);
void wakeDrain(int readEnd);

/// UDP for discovery: on Windows an ICMP "port unreachable" otherwise fails the next receive
void disableConnReset(int fd);

}  // namespace sock
}  // namespace finenet
