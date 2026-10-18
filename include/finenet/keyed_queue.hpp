#pragma once

/**
 * @file keyed_queue.hpp
 * @brief Thread-safe keyed queue with deduplication, alarms, and WakeSignal
 *
 * KeyedQueue<K,D> provides:
 * - Key-based deduplication with merge semantics
 * - Internal condition_variable for waitForWork() blocking
 * - Alarm support for timed wakeups
 * - WakeSignal attachment for multi-queue coordination
 *
 * Example: mesh rebuild queue
 * - Key: ChunkPos (subchunk position)
 * - Data: MeshRebuildRequest (priority, LOD, versions)
 * - Merge: keep higher priority, latest versions
 *
 * @tparam Key Key type for deduplication (must be hashable)
 * @tparam Data Associated data type
 * @tparam Hash Hash function for Key (default: std::hash<Key>)
 * @tparam Equal Equality function for Key (default: std::equal_to<Key>)
 */

#include "finenet/wake_signal.hpp"

#include <deque>
#include <unordered_set>
#include <unordered_map>
#include <optional>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <vector>
#include <algorithm>

namespace finenet {

template<typename Key, typename Data,
         typename Hash = std::hash<Key>, typename Equal = std::equal_to<Key>>
class KeyedQueue {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using MergeFn = std::function<Data(const Data& existing, const Data& incoming)>;

    /**
     * @brief Create queue with default merge (replace with incoming)
     */
    KeyedQueue()
        : merge_([](const Data&, const Data& incoming) { return incoming; }) {}

    /**
     * @brief Create queue with custom merge function
     *
     * @param merge Function that combines existing and incoming data
     */
    explicit KeyedQueue(MergeFn merge)
        : merge_(std::move(merge)) {}

    ~KeyedQueue() = default;

    // Non-copyable, non-movable (owns mutex/condition_variable)
    KeyedQueue(const KeyedQueue&) = delete;
    KeyedQueue& operator=(const KeyedQueue&) = delete;
    KeyedQueue(KeyedQueue&&) = delete;
    KeyedQueue& operator=(KeyedQueue&&) = delete;

    // ========================================================================
    // WakeSignal attachment (for multi-queue consumers)
    // ========================================================================

    /**
     * @brief Attach this queue to a WakeSignal
     *
     * When attached, push() will signal the WakeSignal in addition to the
     * internal condition_variable.
     *
     * If the queue already has items, the signal is notified immediately.
     */
    void attach(WakeSignal* signal) {
        std::lock_guard<std::mutex> lock(mutex_);
        signal_ = signal;

        if (signal_ && !order_.empty()) {
            signal_->signal();
        }
    }

    /**
     * @brief Detach from current WakeSignal
     */
    void detach() {
        std::lock_guard<std::mutex> lock(mutex_);
        signal_ = nullptr;
    }

    /**
     * @brief Check if attached to a WakeSignal
     */
    [[nodiscard]] bool isAttached() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return signal_ != nullptr;
    }

    // ========================================================================
    // Push operations
    // ========================================================================

    /**
     * @brief Push a key-data pair
     *
     * If the key already exists, the data is merged.
     * Wakes waitForWork() and signals attached WakeSignal.
     *
     * @param key Deduplication key
     * @param data Associated data
     * @return true if newly added, false if merged with existing
     */
    bool push(const Key& key, const Data& data) {
        WakeSignal* signalToNotify = nullptr;
        bool isNew = false;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (shutdownFlag_) {
                return false;
            }

            auto it = dataMap_.find(key);
            if (it != dataMap_.end()) {
                it->second = merge_(it->second, data);
                isNew = false;
            } else {
                order_.push_back(key);
                present_.insert(key);
                dataMap_[key] = data;
                isNew = true;
            }

            signalToNotify = signal_;
        }

        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }

        return isNew;
    }

    /**
     * @brief Push with move semantics
     */
    bool push(Key&& key, Data&& data) {
        WakeSignal* signalToNotify = nullptr;
        bool isNew = false;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (shutdownFlag_) {
                return false;
            }

            auto it = dataMap_.find(key);
            if (it != dataMap_.end()) {
                it->second = merge_(it->second, std::move(data));
                isNew = false;
            } else {
                order_.push_back(key);
                present_.insert(key);
                dataMap_.emplace(std::move(key), std::move(data));
                isNew = true;
            }

            signalToNotify = signal_;
        }

        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }

        return isNew;
    }

    // ========================================================================
    // Batch Push operations
    // ========================================================================

    /**
     * @brief Push multiple key-data pairs atomically
     *
     * Items are merged if keys already exist.
     *
     * @param items Pairs of (key, data)
     * @return Number of newly added keys (vs merged)
     */
    size_t pushBatch(std::vector<std::pair<Key, Data>> items) {
        if (items.empty()) return 0;

        WakeSignal* signalToNotify = nullptr;
        size_t newCount = 0;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (shutdownFlag_) {
                return 0;
            }

            for (auto& [key, data] : items) {
                auto it = dataMap_.find(key);
                if (it != dataMap_.end()) {
                    it->second = merge_(it->second, std::move(data));
                } else {
                    order_.push_back(key);
                    present_.insert(key);
                    dataMap_.emplace(std::move(key), std::move(data));
                    ++newCount;
                }
            }

            signalToNotify = signal_;
        }

        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }

        return newCount;
    }

    // ========================================================================
    // Pop operations
    // ========================================================================

    /**
     * @brief Try to pop the front item (non-blocking)
     *
     * @return Pair of (key, data) if available, nullopt if empty
     */
    std::optional<std::pair<Key, Data>> tryPop() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (order_.empty()) {
            return std::nullopt;
        }

        Key key = std::move(order_.front());
        order_.pop_front();
        present_.erase(key);

        auto it = dataMap_.find(key);
        Data data = std::move(it->second);
        dataMap_.erase(it);

        return std::make_pair(std::move(key), std::move(data));
    }

    /**
     * @brief Pop front with blocking wait
     *
     * Waits until data is available or shutdown is signaled.
     *
     * @return Pair of (key, data), or nullopt if shutdown with empty queue
     */
    std::optional<std::pair<Key, Data>> popWait() {
        std::unique_lock<std::mutex> lock(mutex_);

        condition_.wait(lock, [this]() {
            return !order_.empty() || shutdownFlag_;
        });

        if (shutdownFlag_ && order_.empty()) {
            return std::nullopt;
        }

        Key key = std::move(order_.front());
        order_.pop_front();
        present_.erase(key);

        auto it = dataMap_.find(key);
        Data data = std::move(it->second);
        dataMap_.erase(it);

        return std::make_pair(std::move(key), std::move(data));
    }

    // ========================================================================
    // Batch Pop operations
    // ========================================================================

    /**
     * @brief Drain all items at once (non-blocking)
     *
     * @return Vector of (key, data) pairs in queue order
     */
    std::vector<std::pair<Key, Data>> drainAll() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::pair<Key, Data>> result;
        result.reserve(order_.size());

        while (!order_.empty()) {
            Key key = std::move(order_.front());
            order_.pop_front();
            present_.erase(key);

            auto it = dataMap_.find(key);
            Data data = std::move(it->second);
            dataMap_.erase(it);

            result.emplace_back(std::move(key), std::move(data));
        }
        return result;
    }

    /**
     * @brief Drain up to maxItems (non-blocking)
     *
     * @param maxItems Maximum number of items to drain
     * @return Vector of (key, data) pairs in queue order
     */
    std::vector<std::pair<Key, Data>> drainUpTo(size_t maxItems) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::pair<Key, Data>> result;
        size_t count = std::min(maxItems, order_.size());
        result.reserve(count);

        for (size_t i = 0; i < count; ++i) {
            Key key = std::move(order_.front());
            order_.pop_front();
            present_.erase(key);

            auto it = dataMap_.find(key);
            Data data = std::move(it->second);
            dataMap_.erase(it);

            result.emplace_back(std::move(key), std::move(data));
        }
        return result;
    }

    // ========================================================================
    // Alarm operations
    // ========================================================================

    /**
     * @brief Set an alarm to wake at the specified time
     *
     * If an alarm is already pending, keeps the LATER time.
     */
    void setAlarm(TimePoint wakeTime) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!alarmPending_ || wakeTime > alarmTime_) {
            alarmTime_ = wakeTime;
            alarmPending_ = true;
        }
        condition_.notify_all();
    }

    /**
     * @brief Cancel any pending alarm
     */
    void clearAlarm() {
        std::lock_guard<std::mutex> lock(mutex_);
        alarmPending_ = false;
    }

    /**
     * @brief Check if an alarm is pending
     */
    [[nodiscard]] bool hasAlarm() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return alarmPending_;
    }

    // ========================================================================
    // Wait operations (for single-queue consumers)
    // ========================================================================

    /**
     * @brief Block until work available, alarm fires, or shutdown
     *
     * Does NOT pop any items - caller should use tryPop() after waking.
     *
     * @return true if woken normally, false if shutdown was signaled
     */
    bool waitForWork() {
        std::unique_lock<std::mutex> lock(mutex_);

        while (true) {
            if (shutdownFlag_) {
                return false;
            }

            if (!order_.empty()) {
                return true;
            }

            if (alarmPending_) {
                auto status = condition_.wait_until(lock, alarmTime_);

                if (status == std::cv_status::timeout) {
                    alarmPending_ = false;
                    return true;
                }
            } else {
                condition_.wait(lock);
            }
        }
    }

    /**
     * @brief Wait with a maximum timeout
     *
     * @return true if woken normally or timeout, false if shutdown
     */
    bool waitForWork(std::chrono::milliseconds maxWait) {
        std::unique_lock<std::mutex> lock(mutex_);

        auto deadline = Clock::now() + maxWait;

        while (true) {
            if (shutdownFlag_) {
                return false;
            }

            if (!order_.empty()) {
                return true;
            }

            TimePoint waitUntil = deadline;
            if (alarmPending_ && alarmTime_ < deadline) {
                waitUntil = alarmTime_;
            }

            auto status = condition_.wait_until(lock, waitUntil);

            if (status == std::cv_status::timeout) {
                if (alarmPending_ && Clock::now() >= alarmTime_) {
                    alarmPending_ = false;
                }
                return !shutdownFlag_;
            }
        }
    }

    // ========================================================================
    // Query operations
    // ========================================================================

    /**
     * @brief Check if queue is empty
     */
    [[nodiscard]] bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return order_.empty();
    }

    /**
     * @brief Get number of unique items in queue
     */
    [[nodiscard]] size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return order_.size();
    }

    /**
     * @brief Check if a key is currently in the queue
     */
    [[nodiscard]] bool contains(const Key& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return present_.count(key) > 0;
    }

    /**
     * @brief Get data for a key (nullopt if not in queue)
     */
    [[nodiscard]] std::optional<Data> getData(const Key& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = dataMap_.find(key);
        if (it != dataMap_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    // ========================================================================
    // Shutdown support
    // ========================================================================

    /**
     * @brief Signal shutdown - wakes all waiting threads
     *
     * After shutdown:
     * - waitForWork() returns false
     * - push() silently drops items
     * - tryPop() continues to work until queue is drained
     */
    void shutdown() {
        WakeSignal* signalToNotify = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdownFlag_ = true;
            signalToNotify = signal_;
        }
        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
    }

    /**
     * @brief Check if shutdown was called
     */
    [[nodiscard]] bool isShutdown() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return shutdownFlag_;
    }

    /**
     * @brief Reset shutdown state
     */
    void resetShutdown() {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdownFlag_ = false;
    }

    /**
     * @brief Clear all items
     */
    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        order_.clear();
        present_.clear();
        dataMap_.clear();
        alarmPending_ = false;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool shutdownFlag_ = false;

    std::deque<Key> order_;                              // Insertion order
    std::unordered_set<Key, Hash, Equal> present_;       // Fast lookup
    std::unordered_map<Key, Data, Hash, Equal> dataMap_; // Key -> Data

    MergeFn merge_;

    // Alarm state
    bool alarmPending_ = false;
    TimePoint alarmTime_;

    // WakeSignal for multi-queue coordination
    WakeSignal* signal_ = nullptr;
};

}  // namespace finenet
