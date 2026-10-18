#pragma once

/**
 * @file priority.hpp
 * @brief Channel delivery guarantees, message priorities, and compression policies
 */

#include <cstdint>

namespace finenet {

/**
 * @brief Delivery reliability mode for a logical channel
 */
enum class Reliability : uint8_t {
    Unreliable      = 0,  ///< Fire-and-forget; no ack, no retransmit
    Reliable        = 1,  ///< Retransmit until acked, no ordering guarantee
    ReliableOrdered = 2,  ///< Retransmit and deliver in send order
};

/**
 * @brief Scheduling priority level (lower numeric value = higher urgency)
 */
enum class Priority : uint8_t {
    Critical = 0,  ///< Player input, corrections, control — never delayed
    High     = 1,  ///< Block changes, action results, UI — minimal delay
    Normal   = 2,  ///< Entity updates, sounds, chat — standard scheduling
    Low      = 3,  ///< Chunk data, light updates — background streaming
    Bulk     = 4,  ///< Asset downloads — fills remaining bandwidth
};

/**
 * @brief Payload compression policy for a channel
 */
enum class CompressionPolicy : uint8_t {
    Never         = 0,  ///< Never compress (already compressed or latency-critical)
    Always        = 1,  ///< Always compress payload
    SizeThreshold = 2,  ///< Compress if payload size exceeds compressionThreshold
};

}  // namespace finenet
