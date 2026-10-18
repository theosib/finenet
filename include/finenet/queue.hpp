#pragma once

/**
 * @file queue.hpp
 * @brief Thread-safe FIFO queue with alarm and WakeSignal support
 *
 * Queue<T> provides:
 * - FIFO push/pop semantics (MPSC: multiple producers, single consumer)
 * - Internal condition_variable for waitForWork() blocking
 * - Alarm support for timed wakeups
 * - WakeSignal attachment for multi-queue coordination
 *
 * When both internal CV and WakeSignal are used:
 * - push() notifies BOTH the internal CV and the attached WakeSignal
 * - waitForWork() uses the internal CV (for single-queue consumers)
 * - Multi-queue consumers attach a WakeSignal and use WakeSignal::wait()
 *
 * Usage (single-queue consumer):
 *   Queue<Request> queue;
 *   queue.setAlarm(now + 10ms);  // Schedule background work
 *
 *   while (running) {
 *       if (auto req = queue.tryPop()) {
 *           process(*req);
 *           continue;
 *       }
 *       queue.waitForWork();  // Blocks until push, alarm, or shutdown
 *   }
 *
 * Usage (multi-queue consumer):
 *   WakeSignal wakeSignal;
 *   Queue<MeshData> meshQueue;
 *   Queue<GuiUpdate> guiQueue;
 *
 *   meshQueue.attach(&wakeSignal);
 *   guiQueue.attach(&wakeSignal);
 *
 *   while (running) {
 *       wakeSignal.wait();  // Wakes when any queue has data
 *       while (auto mesh = meshQueue.tryPop()) process(*mesh);
 *       while (auto gui = guiQueue.tryPop()) process(*gui);
 *   }
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

namespace finenet {

template<typename T>
class Queue {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    Queue() = default;
    ~Queue() = default;

    // Non-copyable, non-movable (owns mutex/condition_variable)
    Queue(const Queue&) = delete;
    Queue& operator=(const Queue&) = delete;
    Queue(Queue&&) = delete;
    Queue& operator=(Queue&&) = delete;

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
     * @brief Push an item to the queue
     *
     * Wakes any thread blocked in waitForWork() and signals attached WakeSignal.
     * If shutdown has been called, the item is silently dropped.
     */
    void push(const T& item) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdownFlag_) return;
            queue_.push_back(item);
            signalToNotify = signal_;
        }
        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
    }

    /**
     * @brief Push with move semantics
     */
    void push(T&& item) {
        WakeSignal* signalToNotify = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdownFlag_) return;
            queue_.push_back(std::move(item));
            signalToNotify = signal_;
        }
        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
    }

    // ========================================================================
    // Batch Push operations
    // ========================================================================

    /**
     * @brief Push multiple items atomically (one lock, one notify)
     *
     * More efficient than multiple push() calls for bulk operations.
     */
    void pushBatch(std::vector<T> items) {
        if (items.empty()) return;
        WakeSignal* signalToNotify = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdownFlag_) return;
            for (auto& item : items) {
                queue_.push_back(std::move(item));
            }
            signalToNotify = signal_;
        }
        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
    }

    /**
     * @brief Push multiple items from iterators
     */
    template<typename Iterator>
    void pushBatch(Iterator begin, Iterator end) {
        if (begin == end) return;
        WakeSignal* signalToNotify = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdownFlag_) return;
            for (auto it = begin; it != end; ++it) {
                queue_.push_back(*it);
            }
            signalToNotify = signal_;
        }
        condition_.notify_all();
        if (signalToNotify) {
            signalToNotify->signal();
        }
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
        std::lock_guard<std::mutex> lock(mutex_);

        if (queue_.empty()) {
            return std::nullopt;
        }

        T item = std::move(queue_.front());
        queue_.pop_front();
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
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<T> result;
        result.reserve(queue_.size());
        while (!queue_.empty()) {
            result.push_back(std::move(queue_.front()));
            queue_.pop_front();
        }
        return result;
    }

    /**
     * @brief Drain up to maxItems (non-blocking)
     *
     * Useful for processing in bounded batches.
     */
    std::vector<T> drainUpTo(size_t maxItems) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<T> result;
        size_t count = std::min(maxItems, queue_.size());
        result.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            result.push_back(std::move(queue_.front()));
            queue_.pop_front();
        }
        return result;
    }

    // ========================================================================
    // Alarm operations
    // ========================================================================

    /**
     * @brief Set an alarm to wake at the specified time
     *
     * If an alarm is already pending, keeps the LATER time (rationale: if
     * we're setting a new alarm while one exists, the worker is busy anyway).
     *
     * If wakeTime is in the past, the next waitForWork() returns immediately.
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

            if (!queue_.empty()) {
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
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.clear();
        alarmPending_ = false;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    bool shutdownFlag_ = false;

    std::deque<T> queue_;

    // Alarm state
    bool alarmPending_ = false;
    TimePoint alarmTime_;

    // WakeSignal for multi-queue coordination
    WakeSignal* signal_ = nullptr;
};

}  // namespace finenet
