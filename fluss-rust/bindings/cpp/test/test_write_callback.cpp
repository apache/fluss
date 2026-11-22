/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

#include "test_utils.h"
#include "write_callback.hpp"

namespace {

class Completion {
   public:
    void Reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        results_.clear();
        thread_ = std::thread::id{};
    }

    void Record(fluss::Result result) {
        std::lock_guard<std::mutex> lock(mutex_);
        results_.push_back(std::move(result));
        thread_ = std::this_thread::get_id();
        ready_.notify_all();
    }

    bool Await(size_t count = 1) {
        std::unique_lock<std::mutex> lock(mutex_);
        return ready_.wait_for(lock, std::chrono::seconds(10),
                               [&] { return results_.size() >= count; });
    }

    std::vector<fluss::Result> Results() {
        std::lock_guard<std::mutex> lock(mutex_);
        return results_;
    }

    std::thread::id Thread() {
        std::lock_guard<std::mutex> lock(mutex_);
        return thread_;
    }

   private:
    std::mutex mutex_;
    std::condition_variable ready_;
    std::vector<fluss::Result> results_;
    std::thread::id thread_;
};

// A plain function pointer has no capture. This state lives for the process.
Completion function_completion;

void RecordFunctionCallback(const fluss::WriteCompletion& completion) {
    function_completion.Record(completion.result);
}

struct Lifetime {
    std::promise<void> released;
    ~Lifetime() { released.set_value(); }
};

}  // namespace

class WriteCallbackTest : public ::testing::Test {
   protected:
    void CreateTable(bool primary_key = false, bool disable_delete = false) {
        auto& env = *fluss_test::FlussTestEnvironment::Instance();
        auto builder = fluss::Schema::NewBuilder()
                           .AddColumn("id", fluss::DataType::Int())
                           .AddColumn("value", fluss::DataType::String());
        if (primary_key) {
            builder.SetPrimaryKeys({"id"});
        }
        auto descriptor_builder = fluss::TableDescriptor::NewBuilder()
                                      .SetSchema(builder.Build())
                                      .SetBucketCount(3)
                                      .SetBucketKeys({"id"})
                                      .SetProperty("table.replication.factor", "1");
        if (disable_delete) {
            descriptor_builder.SetProperty("table.delete.behavior", "disable");
        }
        auto descriptor = descriptor_builder.Build();
        table_path_ = fluss::TablePath(
            "fluss", std::string("cpp_callback_") +
                         ::testing::UnitTest::GetInstance()->current_test_info()->name());
        fluss_test::CreateTable(env.GetAdmin(), table_path_, descriptor);
        auto result = env.GetConnection().GetTable(table_path_, table_);
        ASSERT_OK(result);
    }

    fluss::GenericRow Row(int32_t id = 1) {
        fluss::GenericRow row(2);
        row.SetInt32(0, id);
        row.SetString(1, "callback");
        return row;
    }

    fluss::TablePath table_path_;
    fluss::Table table_;
};

TEST_F(WriteCallbackTest, AppendAcceptsFunctionPointer) {
    function_completion.Reset();
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));

    auto submitted = writer.Append(Row(), &RecordFunctionCallback);
    ASSERT_OK(submitted);
    ASSERT_TRUE(function_completion.Await());
    auto results = function_completion.Results();
    ASSERT_EQ(results.size(), 1u);
    EXPECT_OK(results.front());
    EXPECT_NE(function_completion.Thread(), std::this_thread::get_id());

    // The old acknowledgment and fire-and-forget overloads still work.
    fluss::WriteResult pending;
    ASSERT_OK(writer.Append(Row(2), pending));
    ASSERT_OK(pending.Wait());
    ASSERT_OK(writer.Append(Row(3)));
    ASSERT_OK(writer.Flush());
}

TEST_F(WriteCallbackTest, FlushWaitsForPendingCallbacks) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));
    auto started = std::make_shared<Completion>();
    auto finished = std::make_shared<Completion>();
    auto gate = std::make_shared<std::promise<void>>();
    auto resume = gate->get_future().share();
    auto lifetime = std::make_shared<Lifetime>();
    auto released = lifetime->released.get_future();
    std::weak_ptr<Lifetime> weak_lifetime = lifetime;
    {
        auto row = Row();
        fluss::WriteCallback callback = [started, finished, resume, owned = std::move(lifetime)](
                                            const fluss::WriteCompletion& notification) {
            const auto& result = notification.result;
            started->Record(result);
            // Bounded even when a preceding assertion fails.
            resume.wait_for(std::chrono::seconds(20));
            finished->Record(result);
        };
        ASSERT_OK(writer.Append(row, std::move(callback)));
    }
    ASSERT_TRUE(started->Await());
    EXPECT_FALSE(weak_lifetime.expired());
    // Flush now waits for pending callbacks, not just for server ACK.
    // The callback is blocked on the gate, so Flush must not return yet.
    std::promise<void> flush_started;
    auto flush = std::async(std::launch::async, [&] {
        flush_started.set_value();
        return writer.Flush();
    });
    flush_started.get_future().wait();
    EXPECT_EQ(flush.wait_for(std::chrono::milliseconds(25)), std::future_status::timeout);
    gate->set_value();
    ASSERT_OK(flush.get());
    // After Flush returns, the callback has finished and captures are released.
    EXPECT_FALSE(finished->Results().empty());
    EXPECT_EQ(released.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_TRUE(weak_lifetime.expired());
    EXPECT_OK(finished->Results().front());
}

TEST_F(WriteCallbackTest, AppendArrowBatchNotifiesOnce) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));
    auto completion = std::make_shared<Completion>();
    {
        arrow::Int32Builder ids;
        arrow::StringBuilder values;
        ASSERT_TRUE(ids.AppendValues({1, 2, 3, 4, 5, 6}).ok());
        ASSERT_TRUE(values.AppendValues({"a", "b", "c", "d", "e", "f"}).ok());
        auto batch =
            arrow::RecordBatch::Make(arrow::schema({arrow::field("id", arrow::int32()),
                                                    arrow::field("value", arrow::utf8())}),
                                     6, {ids.Finish().ValueOrDie(), values.Finish().ValueOrDie()});
        ASSERT_OK(writer.AppendArrowBatch(batch,
                                          [completion](const fluss::WriteCompletion& notification) {
                                              const auto& result = notification.result;
                                              completion->Record(result);
                                          }));
    }
    ASSERT_TRUE(completion->Await());
    ASSERT_EQ(completion->Results().size(), 1u);
    EXPECT_OK(completion->Results().front());
    ASSERT_OK(writer.Flush());
}

TEST_F(WriteCallbackTest, UpsertAndDeleteNotifyCompletion) {
    CreateTable(true);
    fluss::UpsertWriter writer;
    ASSERT_OK(table_.NewUpsert().CreateWriter(writer));
    auto completion = std::make_shared<Completion>();
    fluss::WriteCallback callback = [completion](const fluss::WriteCompletion& notification) {
        const auto& result = notification.result;
        completion->Record(result);
    };
    ASSERT_OK(writer.Upsert(Row(), callback));
    ASSERT_TRUE(completion->Await());

    fluss::Lookuper lookuper;
    ASSERT_OK(table_.NewLookup().CreateLookuper(lookuper));
    fluss::GenericRow key(2);
    key.SetInt32(0, 1);
    fluss::LookupResult found;
    ASSERT_OK(lookuper.Lookup(key, found));
    ASSERT_TRUE(found.Found());

    ASSERT_OK(writer.Delete(key, callback));
    ASSERT_TRUE(completion->Await(2));
    auto results = completion->Results();
    ASSERT_EQ(results.size(), 2u);
    EXPECT_OK(results[0]);
    EXPECT_OK(results[1]);
    fluss::LookupResult deleted;
    ASSERT_OK(lookuper.Lookup(key, deleted));
    EXPECT_FALSE(deleted.Found());
}

TEST_F(WriteCallbackTest, ServerRejectionIsReportedThroughCallback) {
    CreateTable(true, true);
    fluss::UpsertWriter writer;
    ASSERT_OK(table_.NewUpsert().CreateWriter(writer));
    fluss::WriteResult pending;
    ASSERT_OK(writer.Upsert(Row(), pending));
    ASSERT_OK(pending.Wait());

    auto completion = std::make_shared<Completion>();
    auto submitted = writer.Delete(Row(), [completion](const fluss::WriteCompletion& notification) {
        const auto& result = notification.result;
        completion->Record(result);
    });
    // The write is accepted locally; only the callback reports server rejection.
    ASSERT_OK(submitted);
    ASSERT_TRUE(completion->Await());
    auto results = completion->Results();
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results.front().error_code, fluss::ErrorCode::DELETION_DISABLED_EXCEPTION);
    EXPECT_NE(results.front().error_message.find("disabled"), std::string::npos);
}

TEST_F(WriteCallbackTest, MultipleOutstandingWritesEachNotifyOnce) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));
    auto completion = std::make_shared<Completion>();
    for (int32_t id = 0; id < 64; ++id) {
        ASSERT_OK(writer.Append(Row(id), [completion](const fluss::WriteCompletion& notification) {
            const auto& result = notification.result;
            completion->Record(result);
        }));
    }
    ASSERT_TRUE(completion->Await(64));
    ASSERT_OK(writer.Flush());
    auto results = completion->Results();
    ASSERT_EQ(results.size(), 64u);
    for (const auto& result : results) {
        EXPECT_OK(result);
    }
}

TEST_F(WriteCallbackTest, RejectedSubmissionDoesNotInvokeCallback) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));
    auto completion = std::make_shared<Completion>();
    auto lifetime = std::make_shared<Lifetime>();
    auto released = lifetime->released.get_future();
    fluss::GenericRow invalid(1);
    invalid.SetInt32(0, 1);  // Table requires two columns.
    auto result = writer.Append(invalid, [completion, owned = std::move(lifetime)](
                                             const fluss::WriteCompletion& notification) {
        const auto& completed = notification.result;
        completion->Record(completed);
    });
    EXPECT_FALSE(result.Ok());
    EXPECT_EQ(released.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_TRUE(completion->Results().empty());

    result =
        writer.AppendArrowBatch(nullptr, [completion](const fluss::WriteCompletion& notification) {
            const auto& completed = notification.result;
            completion->Record(completed);
        });
    EXPECT_FALSE(result.Ok());
    EXPECT_TRUE(completion->Results().empty());
    // Both failed submissions must not register a callback.
    ASSERT_OK(writer.Append(Row(), [completion](const fluss::WriteCompletion& notification) {
        const auto& completed = notification.result;
        completion->Record(completed);
    }));
    ASSERT_TRUE(completion->Await());
    ASSERT_OK(writer.Flush());
}

TEST_F(WriteCallbackTest, SameBucketCallbacksFireInSubmissionOrder) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));
    auto completion = std::make_shared<Completion>();
    auto order_mutex = std::make_shared<std::mutex>();
    auto order = std::make_shared<std::vector<int32_t>>();
    // A shared id keeps every record on one bucket, so completion order must
    // match submission order. The single callback worker must not reorder them.
    constexpr int32_t kWrites = 128;
    for (int32_t index = 0; index < kWrites; ++index) {
        ASSERT_OK(writer.Append(Row(7), [completion, order_mutex, order,
                                         index](const fluss::WriteCompletion& notification) {
            const auto& result = notification.result;
            {
                std::lock_guard<std::mutex> lock(*order_mutex);
                order->push_back(index);
            }
            completion->Record(result);
        }));
    }
    ASSERT_TRUE(completion->Await(kWrites));
    ASSERT_OK(writer.Flush());
    auto results = completion->Results();
    ASSERT_EQ(results.size(), static_cast<size_t>(kWrites));
    for (const auto& result : results) {
        EXPECT_OK(result);
    }
    std::lock_guard<std::mutex> lock(*order_mutex);
    ASSERT_EQ(order->size(), static_cast<size_t>(kWrites));
    for (int32_t index = 0; index < kWrites; ++index) {
        EXPECT_EQ((*order)[index], index);
    }
}

TEST_F(WriteCallbackTest, BatchedCallbacksSurviveExceptionsAndCoexistWithWait) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));
    auto completion = std::make_shared<Completion>();
    constexpr int count = 1024;
    for (int i = 0; i < count; ++i) {
        // The same bucket key encourages shared internal batches.
        ASSERT_OK(
            writer.Append(Row(1), [completion, i](const fluss::WriteCompletion& notification) {
                const auto& result = notification.result;
                completion->Record(result);
                if (i % 64 == 0) {
                    throw std::runtime_error("isolated batch callback exception");
                }
            }));
    }
    fluss::WriteResult pending;
    ASSERT_OK(writer.Append(Row(1), pending));
    ASSERT_OK(pending.Wait());
    ASSERT_OK(writer.Flush());
    ASSERT_TRUE(completion->Await(count));
    auto results = completion->Results();
    ASSERT_EQ(results.size(), static_cast<size_t>(count));
    for (const auto& result : results) {
        EXPECT_OK(result);
    }
}

TEST_F(WriteCallbackTest, BatchedServerFailureNotifiesEveryAcceptedDelete) {
    CreateTable(true, true);
    fluss::UpsertWriter writer;
    ASSERT_OK(table_.NewUpsert().CreateWriter(writer));
    fluss::WriteResult initial;
    ASSERT_OK(writer.Upsert(Row(), initial));
    ASSERT_OK(initial.Wait());
    auto completion = std::make_shared<Completion>();
    constexpr int count = 257;
    for (int i = 0; i < count; ++i) {
        ASSERT_OK(writer.Delete(Row(), [completion](const fluss::WriteCompletion& notification) {
            const auto& result = notification.result;
            completion->Record(result);
        }));
    }
    ASSERT_TRUE(completion->Await(count));
    auto results = completion->Results();
    ASSERT_EQ(results.size(), static_cast<size_t>(count));
    for (const auto& result : results) {
        EXPECT_EQ(result.error_code, fluss::ErrorCode::DELETION_DISABLED_EXCEPTION);
    }
}

TEST_F(WriteCallbackTest, EmptyArrowBatchCallbackIsStillAsynchronous) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));
    arrow::Int32Builder ids;
    arrow::StringBuilder values;
    auto batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("id", arrow::int32()), arrow::field("value", arrow::utf8())}),
        0, {ids.Finish().ValueOrDie(), values.Finish().ValueOrDie()});
    auto completion = std::make_shared<Completion>();
    ASSERT_OK(
        writer.AppendArrowBatch(batch, [completion](const fluss::WriteCompletion& notification) {
            const auto& result = notification.result;
            completion->Record(result);
        }));
    ASSERT_TRUE(completion->Await());
    ASSERT_EQ(completion->Results().size(), 1u);
    EXPECT_OK(completion->Results().front());
    EXPECT_NE(completion->Thread(), std::this_thread::get_id());
}

TEST_F(WriteCallbackTest, EmptyCallbacksAreRejectedBeforeSubmission) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));
    auto result = writer.Append(Row(), nullptr);
    EXPECT_FALSE(result.Ok());
    EXPECT_EQ(result.error_message, "Write callback must not be empty");
    result = writer.AppendArrowBatch(nullptr, fluss::WriteCallback{});
    EXPECT_FALSE(result.Ok());
    EXPECT_EQ(result.error_message, "Write callback must not be empty");

    fluss::UpsertWriter upsert;
    result = upsert.Upsert(Row(), nullptr);
    EXPECT_FALSE(result.Ok());
    EXPECT_EQ(result.error_message, "Write callback must not be empty");
    result = upsert.Delete(Row(), nullptr);
    EXPECT_FALSE(result.Ok());
    EXPECT_EQ(result.error_message, "Write callback must not be empty");
    ASSERT_OK(writer.Flush());
}

TEST_F(WriteCallbackTest, UnavailableWritersDoNotInvokeCallbacks) {
    auto completion = std::make_shared<Completion>();
    auto callback = [completion](const fluss::WriteCompletion& notification) {
        const auto& result = notification.result;
        completion->Record(result);
    };
    fluss::AppendWriter append;
    fluss::UpsertWriter upsert;
    EXPECT_FALSE(append.Append(Row(), callback).Ok());
    EXPECT_FALSE(append.AppendArrowBatch(nullptr, callback).Ok());
    EXPECT_FALSE(upsert.Upsert(Row(), callback).Ok());
    EXPECT_FALSE(upsert.Delete(Row(), callback).Ok());
    EXPECT_TRUE(completion->Results().empty());
}

TEST(WriteCallbackBridgeTest, ForwardsErrorAndReleasesCaptures) {
    auto completion = std::make_shared<Completion>();
    auto lifetime = std::make_shared<Lifetime>();
    auto released = lifetime->released.get_future();
    fluss::ffi::WriteCallback callback(
        [completion, owned = std::move(lifetime)](const fluss::WriteCompletion& notification) {
            const auto& result = notification.result;
            completion->Record(result);
        });
    callback.Complete(fluss::ErrorCode::DELETION_DISABLED_EXCEPTION, "Deletion is disabled");
    EXPECT_EQ(released.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    auto results = completion->Results();
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results.front().error_code, fluss::ErrorCode::DELETION_DISABLED_EXCEPTION);
    EXPECT_EQ(results.front().error_message, "Deletion is disabled");
}

TEST(WriteCallbackBridgeTest, ContainsCallbackExceptions) {
    auto lifetime = std::make_shared<Lifetime>();
    auto released = lifetime->released.get_future();
    fluss::ffi::WriteCallback callback(
        [owned = std::move(lifetime)](const fluss::WriteCompletion&) {
            throw std::runtime_error("callback failure");
        });
    EXPECT_NO_THROW(callback.Complete(0, ""));
    EXPECT_EQ(released.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    fluss::ffi::WriteCallback unknown([](const fluss::WriteCompletion&) { throw 42; });
    EXPECT_NO_THROW(unknown.Complete(0, ""));
}

TEST(WriteCallbackBridgeTest, CapacityRejectsOverflowWithoutDroppingReservations) {
    constexpr size_t max_pending_operations = 3;
    fluss::ffi::WriteCallbackCapacity capacity(max_pending_operations, 0);
    for (size_t i = 0; i < max_pending_operations; ++i) {
        ASSERT_OK(capacity.Acquire());
    }
    EXPECT_FALSE(capacity.Acquire().Ok());
    capacity.Release();
    ASSERT_OK(capacity.Acquire());
    EXPECT_FALSE(capacity.Acquire().Ok());
    for (size_t i = 0; i < max_pending_operations; ++i) {
        capacity.Release();
    }
    capacity.AwaitAll();
}

TEST(WriteCallbackBridgeTest, CapacityLastsThroughCallbackAndCaptureCleanup) {
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(1, 0);
    // This deleter runs after the user callback returns, but before its slot is returned.
    auto capture = std::shared_ptr<int>(new int(0), [capacity](int* value) {
        EXPECT_FALSE(capacity->Acquire().Ok());
        delete value;
    });
    fluss::ffi::WriteCallback callback(
        [capacity, owned = std::move(capture)](const fluss::WriteCompletion&) {
            EXPECT_FALSE(capacity->Acquire().Ok());
            throw std::runtime_error("callback failure");
        });
    ASSERT_OK(callback.Reserve(capacity));
    EXPECT_FALSE(capacity->Acquire().Ok());
    callback.Complete(0, "");
    // Complete must release the slot even while its Rust-owned wrapper is still alive.
    ASSERT_OK(capacity->Acquire());
    capacity->Release();
}

TEST(WriteCallbackBridgeTest, UnsubmittedCallbackReturnsCapacityOnException) {
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(1, 0);
    int calls = 0;
    try {
        fluss::ffi::WriteCallback callback([&](const fluss::WriteCompletion&) { ++calls; });
        ASSERT_OK(callback.Reserve(capacity));
        throw std::bad_alloc();
    } catch (const std::bad_alloc&) {
    }
    EXPECT_EQ(calls, 0);
    ASSERT_OK(capacity->Acquire());
    capacity->Release();
}

TEST(WriteCallbackBridgeTest, ReservationOutlivesWriterOwnership) {
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(1, 0);
    std::weak_ptr<fluss::ffi::WriteCallbackCapacity> weak = capacity;
    fluss::ffi::WriteCallback callback([](const fluss::WriteCompletion&) {});
    ASSERT_OK(callback.Reserve(capacity));
    capacity.reset();
    EXPECT_FALSE(weak.expired());
    callback.Complete(0, "");
    EXPECT_TRUE(weak.expired());
}

TEST(WriteCallbackBridgeTest, CapacityTimeoutDoesNotDiscardAcceptedCallback) {
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(1, 25);
    int calls = 0;
    fluss::ffi::WriteCallback accepted([&](const fluss::WriteCompletion&) { ++calls; });
    ASSERT_OK(accepted.Reserve(capacity));
    auto start = std::chrono::steady_clock::now();
    auto result = capacity->Acquire();
    EXPECT_FALSE(result.Ok());
    EXPECT_NE(result.error_message.find("Timed out"), std::string::npos);
    EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(25));
    EXPECT_EQ(calls, 0);
    accepted.Complete(0, "");
    EXPECT_EQ(calls, 1);
    ASSERT_OK(capacity->Acquire());
    capacity->Release();
}

TEST(WriteCallbackBridgeTest, WaitingSubmitterResumesAfterCompletion) {
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(1, 5000);
    fluss::ffi::WriteCallback accepted([](const fluss::WriteCompletion&) {});
    ASSERT_OK(accepted.Reserve(capacity));
    std::promise<void> started;
    auto waiter = std::async(std::launch::async, [&] {
        started.set_value();
        auto result = capacity->Acquire();
        if (result.Ok()) {
            capacity->Release();
        }
        return result;
    });
    started.get_future().wait();
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(25)), std::future_status::timeout);
    accepted.Complete(0, "");
    EXPECT_OK(waiter.get());
}

TEST(WriteCallbackBridgeTest, CallbackRejectsFullOtherWriterAndRestoresThreadContext) {
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(1, 25);
    ASSERT_OK(capacity->Acquire());  // Full writer unrelated to the executing callback.
    fluss::ffi::WriteCallback callback([capacity](const fluss::WriteCompletion&) {
        auto result = capacity->Acquire();
        EXPECT_EQ(result.error_message, "Write callback capacity is full");
        throw 42;
    });
    callback.Complete(0, "");
    auto start = std::chrono::steady_clock::now();
    // CallbackScope must restore the thread context, including on exceptions.
    EXPECT_NE(capacity->Acquire().error_message.find("Timed out"), std::string::npos);
    EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(25));
    capacity->Release();
}

TEST(WriteCallbackBridgeTest, ConcurrentCapacityReservationsStayBounded) {
    constexpr size_t limit = 3;
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(limit, 5000);
    std::atomic<size_t> active{0};
    std::atomic<size_t> completed{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < 250; ++j) {
                fluss::ffi::WriteCallback callback([&](const fluss::WriteCompletion&) {
                    --active;
                    ++completed;
                });
                ASSERT_OK(callback.Reserve(capacity));
                EXPECT_LE(++active, limit);
                std::this_thread::yield();
                callback.Complete(0, "");
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(active.load(), 0u);
    EXPECT_EQ(completed.load(), 2000u);
}

TEST(WriteCallbackBridgeTest, FlushRejectsCallbackReentryBeforeTouchingEitherWriter) {
    fluss::AppendWriter append;
    fluss::UpsertWriter upsert;
    EXPECT_FALSE(fluss::ffi::WriteCallbackCapacity::InCallback());
    fluss::ffi::WriteCallback callback([&](const fluss::WriteCompletion&) {
        EXPECT_TRUE(fluss::ffi::WriteCallbackCapacity::InCallback());
        EXPECT_EQ(append.Flush().error_message, "Flush cannot be called from a write callback");
        EXPECT_EQ(upsert.Flush().error_message, "Flush cannot be called from a write callback");
        throw std::runtime_error("restore callback context after exception");
    });
    callback.Complete(0, "");
    EXPECT_FALSE(fluss::ffi::WriteCallbackCapacity::InCallback());
    EXPECT_EQ(append.Flush().error_message, "AppendWriter not available");
    EXPECT_EQ(upsert.Flush().error_message, "UpsertWriter not available");
}

TEST_F(WriteCallbackTest, RejectsZeroCapacityWithoutCreatingWriter) {
    CreateTable(true);
    fluss::WriteCallbackOptions options;
    EXPECT_EQ(options.max_pending_operations, 262144u);
    options.max_pending_operations = 0;
    fluss::AppendWriter append;
    fluss::UpsertWriter upsert;
    auto appended = table_.NewAppend().CreateWriter(append, options);
    auto upserted = table_.NewUpsert().CreateWriter(upsert, options);
    EXPECT_EQ(appended.error_message, "max_pending_operations must be greater than zero");
    EXPECT_EQ(upserted.error_message, "max_pending_operations must be greater than zero");
    EXPECT_FALSE(append.Available());
    EXPECT_FALSE(upsert.Available());
}

TEST_F(WriteCallbackTest, ConfiguredCapacityBlocksUntilCallbackFinishes) {
    CreateTable();
    fluss::WriteCallbackOptions options;
    options.max_pending_operations = 1;
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer, options));
    auto gate = std::make_shared<std::promise<void>>();
    auto resume = gate->get_future().share();
    auto started = std::make_shared<std::promise<void>>();
    auto ready = started->get_future();
    ASSERT_OK(writer.Append(Row(), [started, resume](const fluss::WriteCompletion&) {
        started->set_value();
        resume.wait_for(std::chrono::seconds(10));
    }));
    ASSERT_EQ(ready.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    std::promise<void> submitting;
    auto submitted = std::async(std::launch::async, [&] {
        submitting.set_value();
        return writer.Append(Row(2), [](const fluss::WriteCompletion&) {});
    });
    submitting.get_future().wait();
    EXPECT_EQ(submitted.wait_for(std::chrono::milliseconds(25)), std::future_status::timeout);
    gate->set_value();
    ASSERT_OK(submitted.get());
    ASSERT_OK(writer.Flush());
}
