#pragma once

/**
 * @file keyed_queue.hpp
 * @brief Thread-safe keyed queue with deduplication, optional priority ordering,
 *        bounded capacity, alarms, and WakeSignal support
 *
 * KeyedQueue<K,D> provides:
 * - Key-based deduplication with merge semantics (single hash table: dataMap_)
 * - Strict O(1) FIFO order by default, or optional PriorityFn ordering
 *   (lower priority value = more urgent; ties broken by insertion order;
 *   respects priority upgrades when an existing key is merged)
 * - Optional bounded capacity with non-blocking and blocking push support
 * - Internal condition_variable for waitForWork() blocking
 * - Alarm support for timed wakeups (keeps earliest pending deadline)
 * - WakeSignal attachment for multi-queue coordination
 *
 * @tparam Key Key type for deduplication (must be hashable)
 * @tparam Data Associated data type
 * @tparam Hash Hash function for Key (default: std::hash<Key>)
 * @tparam Equal Equality function for Key (default: std::equal_to<Key>)
 */

#include "finenet/wake_signal.hpp"

#include <deque>
#include <unordered_map>
#include <optional>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <vector>
#include <algorithm>
#include <type_traits>

namespace finenet {

template<typename Key, typename Data,
         typename Hash = std::hash<Key>, typename Equal = std::equal_to<Key>>
class KeyedQueue {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using MergeFn = std::function<Data(const Data& existing, const Data& incoming)>;
    using PriorityFn = std::function<int(const Data& data)>;

    /**
     * @brief Create queue with default merge (replace with incoming) and FIFO order
     */
    KeyedQueue()
        : merge_(defaultMerge()) {}

    /**
     * @brief Create queue with custom merge function and optional priority extractor / capacity
     *
     * @param merge Function that combines existing and incoming data
     * @param priorityFn Optional priority extractor (lower int = higher priority)
     * @param capacity Optional max unique keys (0 = unlimited)
     */
    explicit KeyedQueue(MergeFn merge, PriorityFn priorityFn = nullptr, size_t capacity = 0)
        : merge_(merge ? std::move(merge) : defaultMerge()),
          priorityFn_(std::move(priorityFn)),
          capacity_(capacity) {}

    /**
     * @brief Create queue with priority extractor and default merge
     */
    explicit KeyedQueue(PriorityFn priorityFn, size_t capacity = 0)
        requires (!std::is_same_v<PriorityFn, MergeFn>)
        : merge_(defaultMerge()),
          priorityFn_(std::move(priorityFn)),
          capacity_(capacity) {}

    /**
     * @brief Create queue with bounded capacity, default merge, and FIFO order
     */
    explicit KeyedQueue(size_t capacity)
        : merge_(defaultMerge()),
          capacity_(capacity) {}

    ~KeyedQueue() = default;

    // Non-copyable, non-movable (owns mutex/condition_variable)
    KeyedQueue(const KeyedQueue&) = delete;
    KeyedQueue& operator=(const KeyedQueue&) = delete;
    KeyedQueue(KeyedQueue&&) = delete;
    KeyedQueue& operator=(KeyedQueue&&) = delete;

    // ========================================================================
    // Configuration (Priority, Merge, Capacity)
    // ========================================================================

    /**
     * @brief Configure or clear the priority extractor function
     *
     * Lower integer return value means higher priority (e.g. 0 = immediate,
     * 100 = normal, 1000 = background). Pass nullptr to revert to O(1) FIFO.
     */
    void setPriorityExtractor(PriorityFn priorityFn) {
        std::lock_guard<std::mutex> lock(mutex_);
        priorityFn_ = std::move(priorityFn);
    }

    /**
     * @brief Check if a priority extractor is configured
     */
    [[nodiscard]] bool hasPriorityExtractor() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return static_cast<bool>(priorityFn_);
    }

    /**
     * @brief Configure the merge function used when a duplicate key is pushed
     */
    void setMergeFunction(MergeFn merge) {
        std::lock_guard<std::mutex> lock(mutex_);
        merge_ = merge ? std::move(merge) : defaultMerge();
    }

    /**
     * @brief Set maximum capacity of unique keys (0 = unlimited)
     */
    void setCapacity(size_t maxItems) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            capacity_ = maxItems;
        }
        producerCv_.notify_all();
    }

    /**
     * @brief Get maximum capacity (0 = unlimited)
     */
    [[nodiscard]] size_t capacity() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return capacity_;
    }

    /**
     * @brief Check if queue is at capacity for new keys
     */
    [[nodiscard]] bool isFull() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return capacity_ > 0 && dataMap_.size() >= capacity_;
    }

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

        if (signal_ && !dataMap_.empty()) {
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
     * If the key already exists, the data is merged in-place.
     * Wakes waitForWork() and signals attached WakeSignal.
     *
     * @param key Deduplication key
     * @param data Associated data
     * @return true if newly added, false if merged with existing or dropped
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
                if (capacity_ > 0 && dataMap_.size() >= capacity_) {
                    return false;
                }
                dataMap_.emplace(key, data);
                order_.push_back(key);
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
                if (capacity_ > 0 && dataMap_.size() >= capacity_) {
                    return false;
                }
                auto [emplacedIt, inserted] = dataMap_.try_emplace(std::move(key), std::move(data));
                order_.push_back(emplacedIt->first);
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
     * @brief Push a key-data pair, blocking if a new key would exceed capacity
     *
     * If the key already exists in the queue, merges immediately without blocking.
     *
     * @return true if enqueued or merged, false if shutdown was signaled
     */
    bool pushBlocking(const Key& key, const Data& data) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            producerCv_.wait(lock, [this, &key]() {
                return shutdownFlag_ || capacity_ == 0 ||
                       dataMap_.size() < capacity_ ||
                       dataMap_.find(key) != dataMap_.end();
            });
            if (shutdownFlag_) {
                return false;
            }
            auto it = dataMap_.find(key);
            if (it != dataMap_.end()) {
                it->second = merge_(it->second, data);
            } else {
                dataMap_.emplace(key, data);
                order_.push_back(key);
            }
            signalToNotify = signal_;
        }

        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
        return true;
    }

    /**
     * @brief Push with move semantics, blocking if a new key would exceed capacity
     *
     * @return true if enqueued or merged, false if shutdown was signaled
     */
    bool pushBlocking(Key&& key, Data&& data) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            producerCv_.wait(lock, [this, &key]() {
                return shutdownFlag_ || capacity_ == 0 ||
                       dataMap_.size() < capacity_ ||
                       dataMap_.find(key) != dataMap_.end();
            });
            if (shutdownFlag_) {
                return false;
            }
            auto it = dataMap_.find(key);
            if (it != dataMap_.end()) {
                it->second = merge_(it->second, std::move(data));
            } else {
                auto [emplacedIt, inserted] = dataMap_.try_emplace(std::move(key), std::move(data));
                order_.push_back(emplacedIt->first);
            }
            signalToNotify = signal_;
        }

        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
        return true;
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
        bool anyAddedOrMerged = false;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (shutdownFlag_) {
                return 0;
            }

            for (auto& [key, data] : items) {
                auto it = dataMap_.find(key);
                if (it != dataMap_.end()) {
                    it->second = merge_(it->second, std::move(data));
                    anyAddedOrMerged = true;
                } else {
                    if (capacity_ > 0 && dataMap_.size() >= capacity_) {
                        continue;
                    }
                    auto [emplacedIt, inserted] = dataMap_.try_emplace(std::move(key), std::move(data));
                    order_.push_back(emplacedIt->first);
                    ++newCount;
                    anyAddedOrMerged = true;
                }
            }

            if (anyAddedOrMerged) {
                signalToNotify = signal_;
            }
        }

        if (anyAddedOrMerged) {
            condition_.notify_all();
            if (signalToNotify) {
                signalToNotify->signal();
            }
        }

        return newCount;
    }

    // ========================================================================
    // Pop operations
    // ========================================================================

    /**
     * @brief Try to pop the next item (non-blocking)
     *
     * Pops in FIFO order by default, or highest priority (lowest PriorityFn
     * value, ties broken by insertion order) when a PriorityFn is configured.
     *
     * @return Pair of (key, data) if available, nullopt if empty
     */
    std::optional<std::pair<Key, Data>> tryPop() {
        std::optional<std::pair<Key, Data>> result;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            result = popLocked();
        }
        if (result.has_value()) {
            producerCv_.notify_one();
        }
        return result;
    }

    /**
     * @brief Alias for tryPop() - for API compatibility
     */
    std::optional<std::pair<Key, Data>> pop() {
        return tryPop();
    }

    /**
     * @brief Pop next item with blocking wait
     *
     * Waits until data is available or shutdown is signaled.
     *
     * @return Pair of (key, data), or nullopt if shutdown with empty queue
     */
    std::optional<std::pair<Key, Data>> popWait() {
        std::optional<std::pair<Key, Data>> result;
        {
            std::unique_lock<std::mutex> lock(mutex_);

            condition_.wait(lock, [this]() {
                return !order_.empty() || shutdownFlag_;
            });

            if (order_.empty()) {
                return std::nullopt;
            }

            result = popLocked();
        }
        if (result.has_value()) {
            producerCv_.notify_one();
        }
        return result;
    }

    // ========================================================================
    // Batch Pop operations
    // ========================================================================

    /**
     * @brief Drain all items at once (non-blocking)
     *
     * @return Vector of (key, data) pairs in queue order (or priority order
     *         with insertion-order tie-breaking when PriorityFn is set)
     */
    std::vector<std::pair<Key, Data>> drainAll() {
        std::vector<std::pair<Key, Data>> result;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            const size_t count = order_.size();
            result.reserve(count);

            if (!priorityFn_) {
                while (!order_.empty()) {
                    Key key = std::move(order_.front());
                    order_.pop_front();

                    auto it = dataMap_.find(key);
                    Data data = std::move(it->second);
                    dataMap_.erase(it);

                    result.emplace_back(std::move(key), std::move(data));
                }
            } else {
                struct IndexedEntry {
                    int priority;
                    size_t orderIndex;
                    Key key;
                    Data data;
                };
                std::vector<IndexedEntry> entries;
                entries.reserve(count);
                size_t idx = 0;
                while (!order_.empty()) {
                    Key key = std::move(order_.front());
                    order_.pop_front();

                    auto it = dataMap_.find(key);
                    int prio = priorityFn_(it->second);
                    Data data = std::move(it->second);
                    dataMap_.erase(it);

                    entries.push_back(IndexedEntry{prio, idx++, std::move(key), std::move(data)});
                }
                std::stable_sort(entries.begin(), entries.end(),
                    [](const IndexedEntry& a, const IndexedEntry& b) {
                        return a.priority < b.priority;
                    });
                for (auto& e : entries) {
                    result.emplace_back(std::move(e.key), std::move(e.data));
                }
            }
        }
        if (!result.empty()) {
            producerCv_.notify_all();
        }
        return result;
    }

    /**
     * @brief Drain up to maxItems (non-blocking)
     *
     * @param maxItems Maximum number of items to drain
     * @return Vector of (key, data) pairs in queue/priority order
     */
    std::vector<std::pair<Key, Data>> drainUpTo(size_t maxItems) {
        std::vector<std::pair<Key, Data>> result;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            size_t count = std::min(maxItems, order_.size());
            result.reserve(count);

            for (size_t i = 0; i < count; ++i) {
                auto popped = popLocked();
                if (!popped.has_value()) break;
                result.push_back(std::move(*popped));
            }
        }
        if (!result.empty()) {
            producerCv_.notify_all();
        }
        return result;
    }

    // ========================================================================
    // Alarm operations
    // ========================================================================

    /**
     * @brief Set an alarm to wake at the specified time
     *
     * If an alarm is already pending, keeps the EARLIER time so an urgent
     * wakeup is never ignored and a later alarm does not push back an
     * already-scheduled earlier deadline.
     */
    void setAlarm(TimePoint wakeTime) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!alarmPending_ || wakeTime < alarmTime_) {
                alarmTime_ = wakeTime;
                alarmPending_ = true;
            }
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
     * @brief Alias for clearAlarm()
     */
    void resetAlarm() {
        clearAlarm();
    }

    /**
     * @brief Check if an alarm is pending
     */
    [[nodiscard]] bool hasAlarm() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return alarmPending_;
    }

    /**
     * @brief Get the currently scheduled alarm time
     */
    [[nodiscard]] TimePoint alarmTime() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return alarmTime_;
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
                if (Clock::now() >= alarmTime_) {
                    alarmPending_ = false;
                    return true;
                }

                auto status = condition_.wait_until(lock, alarmTime_);

                if (status == std::cv_status::timeout ||
                    (alarmPending_ && Clock::now() >= alarmTime_)) {
                    alarmPending_ = false;
                    return !shutdownFlag_;
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

            auto now = Clock::now();
            if (alarmPending_ && now >= alarmTime_) {
                alarmPending_ = false;
                return true;
            }
            if (now >= deadline) {
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
        return dataMap_.empty();
    }

    /**
     * @brief Get number of unique items in queue
     */
    [[nodiscard]] size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return dataMap_.size();
    }

    /**
     * @brief Check if a key is currently in the queue
     */
    [[nodiscard]] bool contains(const Key& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return dataMap_.find(key) != dataMap_.end();
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
     * - waitForWork() and pushBlocking() return false
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
        producerCv_.notify_all();
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
        {
            std::lock_guard<std::mutex> lock(mutex_);
            order_.clear();
            dataMap_.clear();
            alarmPending_ = false;
        }
        producerCv_.notify_all();
    }

private:
    static MergeFn defaultMerge() {
        return [](const Data&, const Data& incoming) { return incoming; };
    }

    std::optional<std::pair<Key, Data>> popLocked() {
        if (order_.empty()) {
            return std::nullopt;
        }

        if (!priorityFn_) {
            Key key = std::move(order_.front());
            order_.pop_front();

            auto it = dataMap_.find(key);
            Data data = std::move(it->second);
            dataMap_.erase(it);

            return std::make_pair(std::move(key), std::move(data));
        }

        // Priority mode: find the key in order_ whose current Data in dataMap_
        // has the lowest priority value (ties broken by earliest insertion in order_).
        auto bestOrderIt = order_.begin();
        auto bestMapIt = dataMap_.find(*bestOrderIt);
        int bestPriority = priorityFn_(bestMapIt->second);

        for (auto it = std::next(order_.begin()); it != order_.end(); ++it) {
            auto mapIt = dataMap_.find(*it);
            int prio = priorityFn_(mapIt->second);
            if (prio < bestPriority) {
                bestPriority = prio;
                bestOrderIt = it;
                bestMapIt = mapIt;
            }
        }

        Key key = std::move(*bestOrderIt);
        order_.erase(bestOrderIt);
        Data data = std::move(bestMapIt->second);
        dataMap_.erase(bestMapIt);

        return std::make_pair(std::move(key), std::move(data));
    }

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::condition_variable producerCv_;
    bool shutdownFlag_ = false;

    std::deque<Key> order_;                              // Insertion order
    std::unordered_map<Key, Data, Hash, Equal> dataMap_; // Key -> Data (single hash table)

    MergeFn merge_;
    PriorityFn priorityFn_;
    size_t capacity_ = 0;

    // Alarm state
    bool alarmPending_ = false;
    TimePoint alarmTime_;

    // WakeSignal for multi-queue coordination
    WakeSignal* signal_ = nullptr;
};

}  // namespace finenet
