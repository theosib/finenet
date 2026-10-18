#pragma once

/**
 * @file queue_bridge.hpp
 * @brief Generic adapter connecting a local Queue<T> to a finenet::Connection channel
 */

#include "finenet/channel.hpp"
#include "finenet/connection.hpp"
#include "finenet/message.hpp"
#include "finenet/message_builder.hpp"
#include "finenet/message_reader.hpp"
#include "finenet/queue.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace finenet {

/**
 * @brief Bridges a local queue and a Connection channel using Encoder/Decoder callbacks
 *
 * Supports both single-item mode (one Message per item) and batch mode
 * (up to maxBatchSize items packed into a single Message using an array header).
 *
 * @tparam T Item type stored in the local queue
 * @tparam QueueType Queue container type (defaults to finenet::Queue<T>)
 */
template<typename T, typename QueueType = Queue<T>>
class QueueBridge {
public:
    using Encoder = std::function<Message(const T& item)>;
    using Decoder = std::function<T(MessageReader& reader)>;

    QueueBridge(QueueType& localQueue,
                Connection& connection,
                ChannelId channel,
                uint32_t messageTypeId)
        : localQueue_(localQueue),
          connection_(connection),
          channel_(channel),
          messageTypeId_(messageTypeId),
          aliveToken_(std::make_shared<bool>(true)) {}

    QueueBridge(QueueType& localQueue,
                Connection& connection,
                ChannelId channel,
                MessageType messageType)
        : QueueBridge(localQueue, connection, channel, static_cast<uint32_t>(messageType)) {}

    ~QueueBridge() {
        if (aliveToken_) {
            *aliveToken_ = false;
        }
    }

    QueueBridge(const QueueBridge&) = delete;
    QueueBridge& operator=(const QueueBridge&) = delete;

    /**
     * @brief Configure the item-to-Message encoder for flushToNetwork()
     */
    void setEncoder(Encoder enc) {
        encoder_ = std::move(enc);
    }

    /**
     * @brief Configure the MessageReader-to-item decoder for onReceive()
     *
     * Also registers an automatic message handler on the Connection if no
     * handler is currently registered for messageTypeId_.
     */
    void setDecoder(Decoder dec) {
        decoder_ = std::move(dec);
        if (decoder_ && !connection_.hasMessageHandler(messageTypeId_)) {
            std::weak_ptr<bool> weakAlive = aliveToken_;
            QueueBridge* self = this;
            connection_.onMessage(messageTypeId_, [weakAlive, self](const Message& msg) {
                if (auto alive = weakAlive.lock(); alive && *alive) {
                    self->onReceive(msg);
                }
            });
        }
    }

    /**
     * @brief Enable or disable batching multiple items into a single Message
     *
     * @param enabled Whether batch mode is active
     * @param maxBatchSize Maximum items packed per batch message (default 32)
     */
    void setBatchMode(bool enabled, uint32_t maxBatchSize = 32) {
        batchMode_ = enabled;
        maxBatchSize_ = std::max<uint32_t>(1u, maxBatchSize);
    }

    [[nodiscard]] bool isBatchMode() const noexcept {
        return batchMode_;
    }

    [[nodiscard]] uint32_t maxBatchSize() const noexcept {
        return maxBatchSize_;
    }

    /**
     * @brief Drain items from localQueue_, encode them, and send on connection_
     *
     * @return Number of items flushed from localQueue_
     */
    size_t flushToNetwork() {
        if (!encoder_) {
            return 0;
        }

        auto items = localQueue_.drainAll();
        if (items.empty()) {
            return 0;
        }

        if (!batchMode_) {
            size_t sentCount = 0;
            for (const auto& item : items) {
                Message msg = encoder_(item);
                if (connection_.send(channel_, std::move(msg)) == SendResult::Ok) {
                    ++sentCount;
                }
            }
            return sentCount;
        }

        // Batch mode: pack up to maxBatchSize_ items into each batch message
        size_t sentCount = 0;
        const size_t total = items.size();
        for (size_t offset = 0; offset < total; offset += maxBatchSize_) {
            const size_t batchCount = std::min<size_t>(maxBatchSize_, total - offset);
            MessageBuilder builder(messageTypeId_);
            builder.writeArrayHeader(static_cast<uint32_t>(batchCount));

            for (size_t i = 0; i < batchCount; ++i) {
                Message itemMsg = encoder_(items[offset + i]);
                builder.writeRawBytes(itemMsg.payload());
            }

            Message batchMsg = builder.build();
            batchMsg.setBatch(true);
            if (connection_.send(channel_, std::move(batchMsg)) == SendResult::Ok) {
                sentCount += batchCount;
            }
        }

        return sentCount;
    }

    /**
     * @brief Decode an incoming Message and push decoded item(s) into localQueue_
     */
    void onReceive(const Message& msg) {
        if (!decoder_) {
            return;
        }

        MessageReader reader(msg);
        if (msg.isBatch() || batchMode_) {
            const uint32_t count = reader.readArrayHeader();
            if (!reader.valid()) {
                return;
            }
            for (uint32_t i = 0; i < count; ++i) {
                T item = decoder_(reader);
                if (!reader.valid()) {
                    break;
                }
                localQueue_.push(std::move(item));
            }
        } else {
            T item = decoder_(reader);
            if (reader.valid()) {
                localQueue_.push(std::move(item));
            }
        }
    }

private:
    QueueType& localQueue_;
    Connection& connection_;
    ChannelId channel_;
    uint32_t messageTypeId_;

    Encoder encoder_;
    Decoder decoder_;

    bool batchMode_ = false;
    uint32_t maxBatchSize_ = 32;
    std::shared_ptr<bool> aliveToken_;
};

}  // namespace finenet
