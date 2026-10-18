#include <gtest/gtest.h>
#include "finenet/keyed_queue.hpp"
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <string>

using namespace finenet;

TEST(KeyedQueueTest, EmptyOnConstruction) {
    KeyedQueue<int, std::string> q;
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
    EXPECT_FALSE(q.isShutdown());
    EXPECT_FALSE(q.hasAlarm());
    EXPECT_FALSE(q.isAttached());
}

TEST(KeyedQueueTest, PushAndTryPop) {
    KeyedQueue<int, std::string> q;

    bool isNew = q.push(1, "hello");
    EXPECT_TRUE(isNew);
    EXPECT_EQ(q.size(), 1u);
    EXPECT_TRUE(q.contains(1));

    auto val = q.tryPop();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val->first, 1);
    EXPECT_EQ(val->second, "hello");
    EXPECT_TRUE(q.empty());
}

TEST(KeyedQueueTest, TryPopEmptyReturnsNullopt) {
    KeyedQueue<int, std::string> q;
    EXPECT_FALSE(q.tryPop().has_value());
}

TEST(KeyedQueueTest, FIFOOrder) {
    KeyedQueue<int, std::string> q;
    q.push(1, "a");
    q.push(2, "b");
    q.push(3, "c");

    auto v1 = q.tryPop();
    auto v2 = q.tryPop();
    auto v3 = q.tryPop();

    EXPECT_EQ(v1->first, 1);
    EXPECT_EQ(v2->first, 2);
    EXPECT_EQ(v3->first, 3);
}

TEST(KeyedQueueTest, DeduplicationDefaultMerge) {
    KeyedQueue<int, std::string> q;

    bool first = q.push(1, "old");
    EXPECT_TRUE(first);

    bool second = q.push(1, "new");
    EXPECT_FALSE(second); // Merged, not newly added

    EXPECT_EQ(q.size(), 1u);

    auto val = q.tryPop();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val->second, "new"); // Default merge: replace with incoming
}

TEST(KeyedQueueTest, CustomMergeFunction) {
    // Merge by summing integers
    KeyedQueue<std::string, int> q([](const int& existing, const int& incoming) {
        return existing + incoming;
    });

    q.push("score", 10);
    q.push("score", 5);
    q.push("score", 3);

    auto val = q.tryPop();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val->first, "score");
    EXPECT_EQ(val->second, 18); // 10 + 5 + 3
}

TEST(KeyedQueueTest, DeduplicationPreservesOrder) {
    KeyedQueue<int, std::string> q;

    q.push(1, "a");
    q.push(2, "b");
    q.push(3, "c");
    q.push(2, "b_updated"); // Merge — position stays at original insertion

    EXPECT_EQ(q.size(), 3u);

    // Order should be 1, 2, 3 (original insertion order)
    EXPECT_EQ(q.tryPop()->first, 1);
    EXPECT_EQ(q.tryPop()->first, 2);
    EXPECT_EQ(q.tryPop()->first, 3);
}

TEST(KeyedQueueTest, MoveSemantics) {
    KeyedQueue<int, std::string> q;

    std::string key_data = "value";
    q.push(42, std::move(key_data));

    auto val = q.tryPop();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val->second, "value");
}

TEST(KeyedQueueTest, GetData) {
    KeyedQueue<int, std::string> q;

    q.push(1, "hello");
    q.push(2, "world");

    auto d1 = q.getData(1);
    ASSERT_TRUE(d1.has_value());
    EXPECT_EQ(*d1, "hello");

    auto d3 = q.getData(3);
    EXPECT_FALSE(d3.has_value());
}

TEST(KeyedQueueTest, Contains) {
    KeyedQueue<int, std::string> q;

    q.push(1, "a");
    EXPECT_TRUE(q.contains(1));
    EXPECT_FALSE(q.contains(2));

    q.tryPop();
    EXPECT_FALSE(q.contains(1));
}

// --- Batch operations ---

TEST(KeyedQueueTest, PushBatch) {
    KeyedQueue<int, std::string> q;

    std::vector<std::pair<int, std::string>> items = {
        {1, "a"}, {2, "b"}, {3, "c"}
    };

    size_t newCount = q.pushBatch(std::move(items));
    EXPECT_EQ(newCount, 3u);
    EXPECT_EQ(q.size(), 3u);
}

TEST(KeyedQueueTest, PushBatchWithMerge) {
    KeyedQueue<int, int> q([](const int& existing, const int& incoming) {
        return existing + incoming;
    });

    q.push(1, 10);

    std::vector<std::pair<int, int>> items = {
        {1, 5}, {2, 20}, {3, 30}
    };

    size_t newCount = q.pushBatch(std::move(items));
    EXPECT_EQ(newCount, 2u); // Keys 2 and 3 are new; key 1 merged
    EXPECT_EQ(q.size(), 3u);

    auto v1 = q.tryPop();
    EXPECT_EQ(v1->first, 1);
    EXPECT_EQ(v1->second, 15); // 10 + 5
}

TEST(KeyedQueueTest, DrainAll) {
    KeyedQueue<int, std::string> q;
    q.push(1, "a");
    q.push(2, "b");
    q.push(3, "c");

    auto items = q.drainAll();
    EXPECT_EQ(items.size(), 3u);
    EXPECT_EQ(items[0].first, 1);
    EXPECT_EQ(items[1].first, 2);
    EXPECT_EQ(items[2].first, 3);
    EXPECT_TRUE(q.empty());
}

TEST(KeyedQueueTest, DrainUpTo) {
    KeyedQueue<int, std::string> q;
    q.push(1, "a");
    q.push(2, "b");
    q.push(3, "c");
    q.push(4, "d");

    auto batch = q.drainUpTo(2);
    EXPECT_EQ(batch.size(), 2u);
    EXPECT_EQ(batch[0].first, 1);
    EXPECT_EQ(batch[1].first, 2);
    EXPECT_EQ(q.size(), 2u);
}

TEST(KeyedQueueTest, PopWait) {
    KeyedQueue<int, std::string> q;
    std::atomic<bool> got{false};

    std::thread consumer([&]() {
        auto val = q.popWait();
        EXPECT_TRUE(val.has_value());
        EXPECT_EQ(val->first, 42);
        EXPECT_EQ(val->second, "answer");
        got = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(got);

    q.push(42, "answer");
    consumer.join();
    EXPECT_TRUE(got);
}

// --- Alarm tests ---

TEST(KeyedQueueTest, AlarmSetAndClear) {
    KeyedQueue<int, std::string> q;
    EXPECT_FALSE(q.hasAlarm());

    q.setAlarm(std::chrono::steady_clock::now() + std::chrono::hours(1));
    EXPECT_TRUE(q.hasAlarm());

    q.clearAlarm();
    EXPECT_FALSE(q.hasAlarm());
}

TEST(KeyedQueueTest, AlarmWakesWaitForWork) {
    KeyedQueue<int, std::string> q;
    std::atomic<bool> woke{false};

    q.setAlarm(std::chrono::steady_clock::now() + std::chrono::milliseconds(30));

    std::thread waiter([&]() {
        q.waitForWork();
        woke = true;
    });

    waiter.join();
    EXPECT_TRUE(woke);
}

// --- Shutdown tests ---

TEST(KeyedQueueTest, ShutdownWakesWaiter) {
    KeyedQueue<int, std::string> q;
    std::atomic<bool> result{true};

    std::thread waiter([&]() {
        result = q.waitForWork();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.shutdown();
    waiter.join();

    EXPECT_FALSE(result);
    EXPECT_TRUE(q.isShutdown());
}

TEST(KeyedQueueTest, PushAfterShutdownReturnsFalse) {
    KeyedQueue<int, std::string> q;
    q.shutdown();

    bool result = q.push(1, "dropped");
    EXPECT_FALSE(result);
    EXPECT_TRUE(q.empty());
}

TEST(KeyedQueueTest, PopWaitReturnsNulloptOnShutdown) {
    KeyedQueue<int, std::string> q;

    std::thread consumer([&]() {
        auto val = q.popWait();
        EXPECT_FALSE(val.has_value());
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.shutdown();
    consumer.join();
}

TEST(KeyedQueueTest, ResetShutdown) {
    KeyedQueue<int, std::string> q;
    q.shutdown();
    EXPECT_TRUE(q.isShutdown());

    q.resetShutdown();
    EXPECT_FALSE(q.isShutdown());
}

TEST(KeyedQueueTest, Clear) {
    KeyedQueue<int, std::string> q;
    q.push(1, "a");
    q.push(2, "b");

    q.clear();
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
    EXPECT_FALSE(q.contains(1));
}

// --- WakeSignal attachment ---

TEST(KeyedQueueTest, AttachDetach) {
    KeyedQueue<int, std::string> q;
    WakeSignal ws;

    EXPECT_FALSE(q.isAttached());

    q.attach(&ws);
    EXPECT_TRUE(q.isAttached());

    q.detach();
    EXPECT_FALSE(q.isAttached());
}

TEST(KeyedQueueTest, PushSignalsAttachedWakeSignal) {
    KeyedQueue<int, std::string> q;
    WakeSignal ws;
    q.attach(&ws);

    std::atomic<bool> woke{false};
    std::thread waiter([&]() {
        ws.wait();
        woke = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(woke);

    q.push(1, "data");
    waiter.join();
    EXPECT_TRUE(woke);
}

TEST(KeyedQueueTest, ShutdownSignalsAttachedWakeSignal) {
    KeyedQueue<int, std::string> q;
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

// --- Concurrent push ---

TEST(KeyedQueueTest, ConcurrentPushes) {
    KeyedQueue<int, int> q;
    constexpr int numThreads = 4;
    constexpr int itemsPerThread = 50;

    std::vector<std::thread> producers;
    for (int t = 0; t < numThreads; ++t) {
        producers.emplace_back([&q, t]() {
            for (int i = 0; i < itemsPerThread; ++i) {
                q.push(t * itemsPerThread + i, i);
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    // All keys are unique across threads
    EXPECT_EQ(q.size(), static_cast<size_t>(numThreads * itemsPerThread));
}

TEST(KeyedQueueTest, ConcurrentPushesWithMerge) {
    // All threads push to the same key — merge should accumulate
    KeyedQueue<int, int> q([](const int& existing, const int& incoming) {
        return existing + incoming;
    });

    constexpr int numThreads = 4;
    constexpr int pushesPerThread = 100;

    std::vector<std::thread> producers;
    for (int t = 0; t < numThreads; ++t) {
        producers.emplace_back([&q]() {
            for (int i = 0; i < pushesPerThread; ++i) {
                q.push(1, 1); // All push to key=1, value=1
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    EXPECT_EQ(q.size(), 1u);
    auto val = q.tryPop();
    ASSERT_TRUE(val.has_value());
    // First push creates with value 1, subsequent 399 pushes add 1 each
    EXPECT_EQ(val->second, numThreads * pushesPerThread);
}
