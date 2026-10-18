#pragma once

/**
 * @file wake_signal.hpp
 * @brief Multi-queue wake mechanism for producer-consumer patterns
 *
 * WakeSignal allows a consumer to sleep until any of multiple sources
 * produce work. Multiple queues can attach to the same WakeSignal,
 * and any push will wake the consumer.
 *
 * Non-queue signal sources (e.g., GPU fence completion, socket readiness)
 * can also call signal() to participate in the same multi-source wait.
 */

#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>

namespace finenet {

/**
 * @brief Synchronization primitive for multi-queue waiting
 *
 * WakeSignal is similar to a condition variable but designed for the
 * producer-consumer pattern where a consumer waits on multiple sources.
 *
 * Key features:
 * - Multiple producers can signal() independently
 * - Consumer waits until signaled, deadline reached, or shutdown
 * - Deadline support for frame-synchronized rendering
 *
 * Usage:
 *   WakeSignal wakeSignal;
 *   Queue<Mesh> meshQueue;
 *   Queue<GuiUpdate> guiQueue;
 *
 *   meshQueue.attach(&wakeSignal);
 *   guiQueue.attach(&wakeSignal);
 *
 *   // Consumer loop:
 *   while (running) {
 *       wakeSignal.setDeadline(frameEnd);
 *       wakeSignal.wait();
 *
 *       while (auto mesh = meshQueue.tryPop()) upload(*mesh);
 *       while (auto gui = guiQueue.tryPop()) process(*gui);
 *   }
 */
class WakeSignal {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    WakeSignal() = default;
    ~WakeSignal() = default;

    // Non-copyable, non-movable (owns synchronization primitives)
    WakeSignal(const WakeSignal&) = delete;
    WakeSignal& operator=(const WakeSignal&) = delete;
    WakeSignal(WakeSignal&&) = delete;
    WakeSignal& operator=(WakeSignal&&) = delete;

    // ========================================================================
    // Producer API (called by queues on push)
    // ========================================================================

    /**
     * @brief Signal that work is available
     *
     * Called by producers (queues) when new items are pushed.
     * Wakes any thread blocked in wait().
     */
    void signal() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            signaled_ = true;
        }
        cv_.notify_all();
    }

    // ========================================================================
    // Consumer API
    // ========================================================================

    /**
     * @brief Block until signaled, deadline reached, or shutdown
     *
     * After returning, consumers should poll their queues with tryPop().
     * The signaled state is automatically cleared after wait() returns.
     *
     * @return true if woken normally (signal or deadline)
     *         false if shutdown was requested
     */
    bool wait() {
        std::unique_lock<std::mutex> lock(mutex_);

        // Wait predicate: signaled, deadline passed, or shutdown
        auto predicate = [this]() {
            if (shutdown_) return true;
            if (signaled_) return true;
            if (hasDeadline_ && Clock::now() >= deadline_) return true;
            return false;
        };

        if (hasDeadline_) {
            cv_.wait_until(lock, deadline_, predicate);
        } else {
            cv_.wait(lock, predicate);
        }

        // Clear signaled state for next wait
        signaled_ = false;

        return !shutdown_;
    }

    /**
     * @brief Block with explicit timeout
     *
     * Useful when no deadline is set but you want periodic wakeups.
     *
     * @param timeout Maximum time to wait
     * @return true if woken normally, false if shutdown requested
     */
    bool waitFor(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);

        auto deadline = Clock::now() + timeout;

        auto predicate = [this]() {
            return shutdown_ || signaled_;
        };

        cv_.wait_until(lock, deadline, predicate);

        signaled_ = false;
        return !shutdown_;
    }

    // ========================================================================
    // Deadline management
    // ========================================================================

    /**
     * @brief Set a deadline for automatic wakeup
     *
     * If the deadline is in the past, the next wait() returns immediately.
     * Setting a new deadline replaces any existing one.
     *
     * @param when Time point at which to wake
     */
    void setDeadline(TimePoint when) {
        std::lock_guard<std::mutex> lock(mutex_);
        deadline_ = when;
        hasDeadline_ = true;
        // Notify in case someone is already waiting with no deadline
        cv_.notify_all();
    }

    /**
     * @brief Clear any pending deadline
     *
     * After this, wait() will block indefinitely (until signaled or shutdown).
     */
    void clearDeadline() {
        std::lock_guard<std::mutex> lock(mutex_);
        hasDeadline_ = false;
    }

    /**
     * @brief Check if a deadline is currently set
     */
    [[nodiscard]] bool hasDeadline() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return hasDeadline_;
    }

    /**
     * @brief Get the current deadline (undefined if hasDeadline() is false)
     */
    [[nodiscard]] TimePoint deadline() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return deadline_;
    }

    // ========================================================================
    // Shutdown support
    // ========================================================================

    /**
     * @brief Request shutdown
     *
     * All current and future wait() calls will return false.
     * Use this to gracefully terminate consumer threads.
     */
    void requestShutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_ = true;
        }
        cv_.notify_all();
    }

    /**
     * @brief Check if shutdown was requested
     */
    [[nodiscard]] bool isShutdown() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return shutdown_;
    }

    /**
     * @brief Reset shutdown state
     *
     * Allows reuse of the WakeSignal after a previous shutdown.
     */
    void resetShutdown() {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = false;
    }

    /**
     * @brief Reset all state (signaled, deadline, shutdown)
     */
    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        signaled_ = false;
        hasDeadline_ = false;
        shutdown_ = false;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;

    bool signaled_ = false;
    bool shutdown_ = false;

    bool hasDeadline_ = false;
    TimePoint deadline_;
};

}  // namespace finenet
