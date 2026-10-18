#include <gtest/gtest.h>
#include "finenet/queue.hpp"
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <string>

using namespace finenet;

TEST(QueueTest, EmptyOnConstruction) {
    Queue<int> q;
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
    EXPECT_EQ(q.capacity(), 0u);
    EXPECT_FALSE(q.isFull());
    EXPECT_FALSE(q.isShutdown());
    EXPECT_FALSE(q.hasAlarm());
    EXPECT_FALSE(q.isAttached());
}

TEST(QueueTest, PushAndTryPop) {
    Queue<int> q;

    EXPECT_TRUE(q.push(42));
    EXPECT_FALSE(q.empty());
    EXPECT_EQ(q.size(), 1u);

    auto val = q.tryPop();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 42);

    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
}

TEST(QueueTest, TryPopEmptyReturnsNullopt) {
    Queue<int> q;
    auto val = q.tryPop();
    EXPECT_FALSE(val.has_value());
}

TEST(QueueTest, FIFOOrder) {
    Queue<int> q;
    EXPECT_TRUE(q.push(1));
    EXPECT_TRUE(q.push(2));
    EXPECT_TRUE(q.push(3));

    EXPECT_EQ(*q.tryPop(), 1);
    EXPECT_EQ(*q.tryPop(), 2);
    EXPECT_EQ(*q.tryPop(), 3);
    EXPECT_FALSE(q.tryPop().has_value());
}

TEST(QueueTest, MoveSemantics) {
    Queue<std::string> q;
    std::string s = "hello";
    EXPECT_TRUE(q.push(std::move(s)));

    auto val = q.tryPop();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "hello");
}

TEST(QueueTest, PushBatchVector) {
    Queue<int> q;
    std::vector<int> items = {10, 20, 30, 40};
    EXPECT_TRUE(q.pushBatch(std::move(items)));

    EXPECT_EQ(q.size(), 4u);
    EXPECT_EQ(*q.tryPop(), 10);
    EXPECT_EQ(*q.tryPop(), 20);
    EXPECT_EQ(*q.tryPop(), 30);
    EXPECT_EQ(*q.tryPop(), 40);
}

TEST(QueueTest, PushBatchIterators) {
    Queue<int> q;
    std::vector<int> items = {5, 6, 7};
    EXPECT_TRUE(q.pushBatch(items.begin(), items.end()));

    EXPECT_EQ(q.size(), 3u);
    EXPECT_EQ(*q.tryPop(), 5);
    EXPECT_EQ(*q.tryPop(), 6);
    EXPECT_EQ(*q.tryPop(), 7);
}

TEST(QueueTest, DrainAll) {
    Queue<int> q;
    q.push(1);
    q.push(2);
    q.push(3);

    auto items = q.drainAll();
    EXPECT_EQ(items.size(), 3u);
    EXPECT_EQ(items[0], 1);
    EXPECT_EQ(items[1], 2);
    EXPECT_EQ(items[2], 3);
    EXPECT_TRUE(q.empty());
}

TEST(QueueTest, DrainAllEmpty) {
    Queue<int> q;
    auto items = q.drainAll();
    EXPECT_TRUE(items.empty());
}

TEST(QueueTest, DrainUpTo) {
    Queue<int> q;
    q.push(1);
    q.push(2);
    q.push(3);
    q.push(4);
    q.push(5);

    auto batch1 = q.drainUpTo(3);
    EXPECT_EQ(batch1.size(), 3u);
    EXPECT_EQ(batch1[0], 1);
    EXPECT_EQ(batch1[1], 2);
    EXPECT_EQ(batch1[2], 3);

    auto batch2 = q.drainUpTo(10);
    EXPECT_EQ(batch2.size(), 2u);
    EXPECT_EQ(batch2[0], 4);
    EXPECT_EQ(batch2[1], 5);

    EXPECT_TRUE(q.empty());
}

TEST(QueueTest, Clear) {
    Queue<int> q;
    q.push(1);
    q.push(2);
    q.setAlarm(std::chrono::steady_clock::now() + std::chrono::hours(1));

    q.clear();
    EXPECT_TRUE(q.empty());
    EXPECT_FALSE(q.hasAlarm());
}

// --- Capacity & Backpressure tests ---

TEST(QueueTest, BoundedCapacityNonBlockingBackpressure) {
    Queue<int> q(3);
    EXPECT_EQ(q.capacity(), 3u);
    EXPECT_FALSE(q.isFull());

    EXPECT_TRUE(q.push(10));
    EXPECT_TRUE(q.push(20));
    EXPECT_FALSE(q.isFull());
    EXPECT_TRUE(q.push(30));
    EXPECT_TRUE(q.isFull());
    EXPECT_EQ(q.size(), 3u);

    // Next push must fail without enqueuing
    EXPECT_FALSE(q.push(40));
    int mv = 50;
    EXPECT_FALSE(q.push(std::move(mv)));
    EXPECT_EQ(q.size(), 3u);

    // Popping frees one slot
    EXPECT_EQ(*q.tryPop(), 10);
    EXPECT_FALSE(q.isFull());
    EXPECT_TRUE(q.push(40));
    EXPECT_TRUE(q.isFull());

    EXPECT_EQ(*q.tryPop(), 20);
    EXPECT_EQ(*q.tryPop(), 30);
    EXPECT_EQ(*q.tryPop(), 40);
    EXPECT_TRUE(q.empty());
}

TEST(QueueTest, PushBatchRespectsCapacity) {
    Queue<int> q(3);
    EXPECT_TRUE(q.pushBatch(std::vector<int>{1, 2}));
    EXPECT_EQ(q.size(), 2u);

    // Batch of 2 would exceed capacity 3 -> rejected without enqueuing
    EXPECT_FALSE(q.pushBatch(std::vector<int>{3, 4}));
    EXPECT_EQ(q.size(), 2u);

    // Batch of 1 fits
    EXPECT_TRUE(q.pushBatch(std::vector<int>{3}));
    EXPECT_TRUE(q.isFull());

    // Any further batch when full is rejected
    EXPECT_FALSE(q.pushBatch(std::vector<int>{4}));
}

TEST(QueueTest, PushBlockingWaitsForConsumerPop) {
    Queue<int> q(2);
    EXPECT_TRUE(q.push(1));
    EXPECT_TRUE(q.push(2));
    EXPECT_TRUE(q.isFull());

    std::atomic<bool> thirdPushed{false};
    std::thread producer([&]() {
        bool ok = q.pushBlocking(3);
        EXPECT_TRUE(ok);
        thirdPushed = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_FALSE(thirdPushed.load());

    // Pop one item to unblock producer
    EXPECT_EQ(*q.tryPop(), 1);
    producer.join();
    EXPECT_TRUE(thirdPushed.load());
    EXPECT_EQ(*q.tryPop(), 2);
    EXPECT_EQ(*q.tryPop(), 3);
}

TEST(QueueTest, PushBlockingUnblocksOnShutdown) {
    Queue<int> q(1);
    EXPECT_TRUE(q.push(1));

    std::atomic<bool> pushResult{true};
    std::thread producer([&]() {
        pushResult = q.pushBlocking(2);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    q.shutdown();
    producer.join();

    EXPECT_FALSE(pushResult.load());
}

TEST(QueueTest, PushBlockingUnblocksOnSetCapacity) {
    Queue<int> q(1);
    EXPECT_TRUE(q.push(1));

    std::atomic<bool> pushResult{false};
    std::thread producer([&]() {
        pushResult = q.pushBlocking(2);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    EXPECT_FALSE(pushResult.load());

    q.setCapacity(2);
    producer.join();
    EXPECT_TRUE(pushResult.load());
    EXPECT_EQ(q.size(), 2u);
}

// --- Alarm tests ---

TEST(QueueTest, AlarmSetAndClear) {
    Queue<int> q;
    EXPECT_FALSE(q.hasAlarm());

    q.setAlarm(std::chrono::steady_clock::now() + std::chrono::hours(1));
    EXPECT_TRUE(q.hasAlarm());

    q.resetAlarm();
    EXPECT_FALSE(q.hasAlarm());
}

TEST(QueueTest, AlarmWakesWaitForWork) {
    Queue<int> q;
    std::atomic<bool> woke{false};

    q.setAlarm(std::chrono::steady_clock::now() + std::chrono::milliseconds(30));

    std::thread waiter([&]() {
        q.waitForWork();
        woke = true;
    });

    waiter.join();
    EXPECT_TRUE(woke);
}

TEST(QueueTest, EarlierAlarmUpdatesDeadlineAndLaterAlarmDoesNotOverwrite) {
    Queue<int> q;
    auto now = std::chrono::steady_clock::now();

    // Schedule a far-future alarm (500ms), then an earlier alarm (40ms), then a later one (400ms)
    q.setAlarm(now + std::chrono::milliseconds(500));
    q.setAlarm(now + std::chrono::milliseconds(40));
    q.setAlarm(now + std::chrono::milliseconds(400)); // Later — must NOT overwrite 40ms

    EXPECT_TRUE(q.hasAlarm());

    auto start = std::chrono::steady_clock::now();
    q.waitForWork();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    // Should wake near 40ms, well below 200ms
    EXPECT_LT(elapsed.count(), 200);
}

TEST(QueueTest, EarlierAlarmWakesAlreadyWaitingThreadEarly) {
    Queue<int> q;
    auto start = std::chrono::steady_clock::now();
    q.setAlarm(start + std::chrono::seconds(5));

    std::atomic<bool> woke{false};
    std::thread waiter([&]() {
        q.waitForWork();
        woke = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(woke.load());

    // Now set an earlier alarm 25ms from now; waiter should wake up promptly
    q.setAlarm(std::chrono::steady_clock::now() + std::chrono::milliseconds(25));
    waiter.join();

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    EXPECT_TRUE(woke.load());
    EXPECT_LT(elapsed.count(), 500);
}

// --- Shutdown tests ---

TEST(QueueTest, ShutdownWakesWaiter) {
    Queue<int> q;
    std::atomic<bool> result{true};

    std::thread waiter([&]() {
        result = q.waitForWork();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    q.shutdown();
    waiter.join();

    EXPECT_FALSE(result); // waitForWork returns false on shutdown
    EXPECT_TRUE(q.isShutdown());
}

TEST(QueueTest, PushAfterShutdownDrops) {
    Queue<int> q;
    q.shutdown();

    EXPECT_FALSE(q.push(42));
    EXPECT_TRUE(q.empty());
}

TEST(QueueTest, TryPopWorksAfterShutdown) {
    Queue<int> q;
    q.push(1);
    q.push(2);

    q.shutdown();

    // Can still drain remaining items
    EXPECT_EQ(*q.tryPop(), 1);
    EXPECT_EQ(*q.tryPop(), 2);
    EXPECT_FALSE(q.tryPop().has_value());
}

TEST(QueueTest, ResetShutdown) {
    Queue<int> q;
    q.shutdown();
    EXPECT_TRUE(q.isShutdown());

    q.resetShutdown();
    EXPECT_FALSE(q.isShutdown());

    // Should accept pushes again
    EXPECT_TRUE(q.push(42));
    EXPECT_EQ(*q.tryPop(), 42);
}

// --- WaitForWork tests ---

TEST(QueueTest, WaitForWorkReturnsTrueOnPush) {
    Queue<int> q;
    std::atomic<bool> result{false};

    std::thread waiter([&]() {
        result = q.waitForWork();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.push(42);

    waiter.join();
    EXPECT_TRUE(result);
    EXPECT_EQ(*q.tryPop(), 42);
}

TEST(QueueTest, WaitForWorkWithTimeout) {
    Queue<int> q;

    auto start = std::chrono::steady_clock::now();
    bool result = q.waitForWork(std::chrono::milliseconds(50));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    EXPECT_TRUE(result); // Not shutdown
    EXPECT_GE(elapsed.count(), 40);
}

TEST(QueueTest, WaitForWorkWithTimeoutReturnsEarlyOnPush) {
    Queue<int> q;
    std::atomic<bool> result{false};

    std::thread waiter([&]() {
        result = q.waitForWork(std::chrono::milliseconds(5000));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.push(99);

    waiter.join();
    EXPECT_TRUE(result);
    EXPECT_EQ(*q.tryPop(), 99);
}

// --- WakeSignal attachment tests ---

TEST(QueueTest, AttachDetach) {
    Queue<int> q;
    WakeSignal ws;

    EXPECT_FALSE(q.isAttached());

    q.attach(&ws);
    EXPECT_TRUE(q.isAttached());

    q.detach();
    EXPECT_FALSE(q.isAttached());
}

TEST(QueueTest, PushSignalsAttachedWakeSignal) {
    Queue<int> q;
    WakeSignal ws;
    q.attach(&ws);

    std::atomic<bool> woke{false};
    std::thread waiter([&]() {
        ws.wait();
        woke = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(woke);

    q.push(42);
    waiter.join();
    EXPECT_TRUE(woke);
}

TEST(QueueTest, ShutdownSignalsAttachedWakeSignal) {
    Queue<int> q;
    WakeSignal ws;
    q.attach(&ws);

    std::atomic<bool> woke{false};
    std::thread waiter([&]() {
        ws.wait();
        woke = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    q.shutdown();
    waiter.join();
    EXPECT_TRUE(woke);
}

TEST(QueueTest, AttachToNonEmptyQueueSignalsImmediately) {
    Queue<int> q;
    WakeSignal ws;

    q.push(42);
    q.attach(&ws);

    // WakeSignal should be signaled immediately since queue is non-empty
    bool result = ws.waitFor(std::chrono::milliseconds(100));
    EXPECT_TRUE(result);
}

// --- Concurrent push from multiple threads ---

TEST(QueueTest, ConcurrentPushes) {
    Queue<int> q;
    constexpr int numThreads = 4;
    constexpr int itemsPerThread = 100;

    std::vector<std::thread> producers;
    for (int t = 0; t < numThreads; ++t) {
        producers.emplace_back([&q, t]() {
            for (int i = 0; i < itemsPerThread; ++i) {
                q.push(t * itemsPerThread + i);
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    EXPECT_EQ(q.size(), static_cast<size_t>(numThreads * itemsPerThread));

    auto all = q.drainAll();
    EXPECT_EQ(all.size(), static_cast<size_t>(numThreads * itemsPerThread));
}

TEST(QueueTest, MultiQueueWithWakeSignal) {
    Queue<int> q1;
    Queue<std::string> q2;
    WakeSignal ws;

    q1.attach(&ws);
    q2.attach(&ws);

    std::atomic<int> intCount{0};
    std::atomic<int> strCount{0};

    std::thread consumer([&]() {
        for (int i = 0; i < 3; ++i) {
            ws.wait();
            while (auto val = q1.tryPop()) {
                ++intCount;
            }
            while (auto val = q2.tryPop()) {
                ++strCount;
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q1.push(1);

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q2.push("hello");

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q1.push(2);

    consumer.join();
    EXPECT_EQ(intCount.load(), 2);
    EXPECT_EQ(strCount.load(), 1);
}
