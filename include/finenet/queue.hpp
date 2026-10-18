#pragma once

/**
 * @file queue.hpp
 * @brief Thread-safe FIFO queue with bounded capacity, alarm, and WakeSignal support
 *
 * Queue<T> provides:
 * - FIFO push/pop semantics (MPSC: multiple producers, single consumer)
 * - Optional bounded capacity with non-blocking backpressure (push) and
 *   blocking backpressure (pushBlocking)
 * - Internal condition_variable for waitForWork() blocking
 * - Alarm support for timed wakeups (keeps earliest pending deadline)
 * - WakeSignal attachment for multi-queue coordination
 *
 * @tparam T Type of items in the queue (must be movable)
 */

#include "finenet/wake_signal.hpp"

#include <deque>
#include <optional>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <vector>
#include <algorithm>
#include <iterator>

namespace finenet {

template<typename T>
class Queue {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    explicit Queue(size_t capacity = 0)
        : capacity_(capacity) {}

    ~Queue() = default;

    // Non-copyable, non-movable (owns mutex/condition_variable)
    Queue(const Queue&) = delete;
    Queue& operator=(const Queue&) = delete;
    Queue(Queue&&) = delete;
    Queue& operator=(Queue&&) = delete;

    // ========================================================================
    // Capacity & Backpressure
    // ========================================================================

    /**
     * @brief Set maximum queue capacity (0 = unlimited)
     */
    void setCapacity(size_t maxItems) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            capacity_ = maxItems;
        }
        producerCv_.notify_all();
    }

    /**
     * @brief Get maximum queue capacity (0 = unlimited)
     */
    [[nodiscard]] size_t capacity() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return capacity_;
    }

    /**
     * @brief Check if queue has reached its capacity limit
     */
    [[nodiscard]] bool isFull() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return capacity_ > 0 && queue_.size() >= capacity_;
    }

    // ========================================================================
    // WakeSignal attachment (for multi-queue consumers)
    // ========================================================================

    /**
     * @brief Attach this queue to a WakeSignal
     *
     * When attached, push() will signal the WakeSignal in addition to the
     * internal condition_variable. This allows multi-queue consumers to
     * wait on a single WakeSignal.
     *
     * If the queue already has items, the signal is notified immediately.
     *
     * @param signal Pointer to WakeSignal (must outlive queue or be detached)
     */
    void attach(WakeSignal* signal) {
        std::lock_guard<std::mutex> lock(mutex_);
        signal_ = signal;

        // If queue already has items, notify immediately
        if (signal_ && !queue_.empty()) {
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
    // Push operations (producer — thread-safe, multiple producers OK)
    // ========================================================================

    /**
     * @brief Push an item to the queue (non-blocking)
     *
     * Wakes any thread blocked in waitForWork() and signals attached WakeSignal.
     * If shutdown has been called or the queue is at capacity, the item is not
     * enqueued and false is returned.
     *
     * @return true if enqueued, false if dropped due to capacity or shutdown
     */
    bool push(const T& item) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdownFlag_) return false;
            if (capacity_ > 0 && queue_.size() >= capacity_) return false;
            queue_.push_back(item);
            signalToNotify = signal_;
        }
        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
        return true;
    }

    /**
     * @brief Push with move semantics (non-blocking)
     *
     * @return true if enqueued, false if dropped due to capacity or shutdown
     */
    bool push(T&& item) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdownFlag_) return false;
            if (capacity_ > 0 && queue_.size() >= capacity_) return false;
            queue_.push_back(std::move(item));
            signalToNotify = signal_;
        }
        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
        return true;
    }

    /**
     * @brief Push an item, blocking if the queue is at capacity
     *
     * Blocks until space becomes available or shutdown() is called.
     *
     * @return true if enqueued, false if shutdown was signaled
     */
    bool pushBlocking(const T& item) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            producerCv_.wait(lock, [this]() {
                return shutdownFlag_ || capacity_ == 0 || queue_.size() < capacity_;
            });
            if (shutdownFlag_) return false;
            queue_.push_back(item);
            signalToNotify = signal_;
        }
        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
        return true;
    }

    /**
     * @brief Push with move semantics, blocking if the queue is at capacity
     *
     * @return true if enqueued, false if shutdown was signaled
     */
    bool pushBlocking(T&& item) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            producerCv_.wait(lock, [this]() {
                return shutdownFlag_ || capacity_ == 0 || queue_.size() < capacity_;
            });
            if (shutdownFlag_) return false;
            queue_.push_back(std::move(item));
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
     * @brief Push multiple items atomically (one lock, one notify)
     *
     * If capacity_ > 0 and the queue cannot accommodate the batch (or is
     * already at capacity), returns false without enqueuing.
     */
    bool pushBatch(std::vector<T> items) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdownFlag_) return false;
            if (capacity_ > 0 && (queue_.size() >= capacity_ || queue_.size() + items.size() > capacity_)) {
                return false;
            }
            if (items.empty()) return true;
            for (auto& item : items) {
                queue_.push_back(std::move(item));
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
     * @brief Push multiple items from iterators
     */
    template<typename Iterator>
    bool pushBatch(Iterator begin, Iterator end) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdownFlag_) return false;
            const auto count = static_cast<size_t>(std::distance(begin, end));
            if (capacity_ > 0 && (queue_.size() >= capacity_ || queue_.size() + count > capacity_)) {
                return false;
            }
            if (count == 0) return true;
            for (auto it = begin; it != end; ++it) {
                queue_.push_back(*it);
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
    // Pop operations (consumer — single consumer only)
    // ========================================================================

    /**
     * @brief Try to pop the front element (non-blocking)
     *
     * @return The front item if available, nullopt if empty
     */
    std::optional<T> tryPop() {
        std::optional<T> item;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (queue_.empty()) {
                return std::nullopt;
            }
            item.emplace(std::move(queue_.front()));
            queue_.pop_front();
        }
        producerCv_.notify_one();
        return item;
    }

    /**
     * @brief Alias for tryPop()
     */
    std::optional<T> pop() {
        return tryPop();
    }

    /**
     * @brief Pop front element with blocking wait
     *
     * Waits until an item is available or shutdown is signaled.
     *
     * @return Front item, or nullopt if shutdown with empty queue
     */
    std::optional<T> popWait() {
        std::optional<T> item;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            condition_.wait(lock, [this]() {
                return !queue_.empty() || shutdownFlag_;
            });
            if (queue_.empty()) {
                return std::nullopt;
            }
            item.emplace(std::move(queue_.front()));
            queue_.pop_front();
        }
        producerCv_.notify_one();
        return item;
    }

    // ========================================================================
    // Batch Pop operations
    // ========================================================================

    /**
     * @brief Drain all items at once (non-blocking)
     *
     * Returns all items for batch processing.
     */
    std::vector<T> drainAll() {
        std::vector<T> result;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            result.reserve(queue_.size());
            while (!queue_.empty()) {
                result.push_back(std::move(queue_.front()));
                queue_.pop_front();
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
     * Useful for processing in bounded batches.
     */
    std::vector<T> drainUpTo(size_t maxItems) {
        std::vector<T> result;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            size_t count = std::min(maxItems, queue_.size());
            result.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                result.push_back(std::move(queue_.front()));
                queue_.pop_front();
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
     *
     * If wakeTime is in the past, the next waitForWork() returns immediately.
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

            if (!queue_.empty()) {
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
     * Useful for periodic health checks.
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

            if (!queue_.empty()) {
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
    // Shutdown support
    // ========================================================================

    /**
     * @brief Signal shutdown - wakes all waiting threads
     *
     * After shutdown:
     * - waitForWork() and pushBlocking() return false
     * - push() drops items and returns false
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
     * @brief Check if shutdown was signaled
     */
    [[nodiscard]] bool isShutdown() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return shutdownFlag_;
    }

    /**
     * @brief Reset shutdown state (allows reuse)
     */
    void resetShutdown() {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdownFlag_ = false;
    }

    // ========================================================================
    // Query operations
    // ========================================================================

    /**
     * @brief Check if queue is empty
     */
    [[nodiscard]] bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    /**
     * @brief Get number of elements in queue
     */
    [[nodiscard]] size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    /**
     * @brief Clear all elements and alarm
     */
    void clear() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.clear();
            alarmPending_ = false;
        }
        producerCv_.notify_all();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::condition_variable producerCv_;
    bool shutdownFlag_ = false;
    size_t capacity_ = 0;

    std::deque<T> queue_;

    // Alarm state
    bool alarmPending_ = false;
    TimePoint alarmTime_;

    // WakeSignal for multi-queue coordination
    WakeSignal* signal_ = nullptr;
};

}  // namespace finenet
