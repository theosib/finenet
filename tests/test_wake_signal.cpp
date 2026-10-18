#include <gtest/gtest.h>
#include "finenet/wake_signal.hpp"
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>

using namespace finenet;

TEST(WakeSignalTest, InitialState) {
    WakeSignal signal;
    EXPECT_FALSE(signal.isShutdown());
    EXPECT_FALSE(signal.hasDeadline());
}

TEST(WakeSignalTest, SignalWakesWaiter) {
    WakeSignal signal;
    std::atomic<bool> woke{false};

    std::thread waiter([&]() {
        signal.wait();
        woke = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(woke);

    signal.signal();
    waiter.join();
    EXPECT_TRUE(woke);
}

TEST(WakeSignalTest, ShutdownWakesWaiterAndReturnsFalse) {
    WakeSignal signal;
    std::atomic<bool> result{true};

    std::thread waiter([&]() {
        result = signal.wait();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    signal.requestShutdown();
    waiter.join();

    EXPECT_FALSE(result);
    EXPECT_TRUE(signal.isShutdown());
}

TEST(WakeSignalTest, DeadlineWakesWaiter) {
    WakeSignal signal;
    std::atomic<bool> woke{false};

    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
    signal.setDeadline(deadline);
    EXPECT_TRUE(signal.hasDeadline());

    std::thread waiter([&]() {
        signal.wait();
        woke = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(woke);

    waiter.join();
    EXPECT_TRUE(woke);
}

TEST(WakeSignalTest, ClearDeadline) {
    WakeSignal signal;

    signal.setDeadline(std::chrono::steady_clock::now() + std::chrono::hours(1));
    EXPECT_TRUE(signal.hasDeadline());

    signal.clearDeadline();
    EXPECT_FALSE(signal.hasDeadline());
}

TEST(WakeSignalTest, WaitForWithTimeout) {
    WakeSignal signal;

    auto start = std::chrono::steady_clock::now();
    bool result = signal.waitFor(std::chrono::milliseconds(50));
    auto end = std::chrono::steady_clock::now();

    EXPECT_TRUE(result);  // Not shutdown
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    EXPECT_GE(elapsed.count(), 40);
}

TEST(WakeSignalTest, Reset) {
    WakeSignal signal;

    signal.signal();
    signal.setDeadline(std::chrono::steady_clock::now() + std::chrono::hours(1));
    signal.requestShutdown();

    EXPECT_TRUE(signal.isShutdown());
    EXPECT_TRUE(signal.hasDeadline());

    signal.reset();

    EXPECT_FALSE(signal.isShutdown());
    EXPECT_FALSE(signal.hasDeadline());
}

TEST(WakeSignalTest, MultipleSignalsWakeMultipleWaiters) {
    WakeSignal signal;
    std::atomic<int> wokenCount{0};
    constexpr int numWaiters = 4;

    std::vector<std::thread> waiters;
    for (int i = 0; i < numWaiters; ++i) {
        waiters.emplace_back([&]() {
            signal.wait();
            ++wokenCount;
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(wokenCount.load(), 0);

    for (int i = 0; i < numWaiters; ++i) {
        signal.signal();
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
    }

    for (auto& t : waiters) {
        t.join();
    }

    EXPECT_EQ(wokenCount.load(), numWaiters);
}
