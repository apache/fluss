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

void RecordFunctionCallback(fluss::Result result) { function_completion.Record(std::move(result)); }

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
        fluss::TablePath path("fluss",
                              std::string("cpp_callback_") +
                                  ::testing::UnitTest::GetInstance()->current_test_info()->name());
        fluss_test::CreateTable(env.GetAdmin(), path, descriptor);
        auto result = env.GetConnection().GetTable(path, table_);
        ASSERT_OK(result);
    }

    fluss::GenericRow Row(int32_t id = 1) {
        fluss::GenericRow row(2);
        row.SetInt32(0, id);
        row.SetString(1, "callback");
        return row;
    }

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

TEST_F(WriteCallbackTest, CallbackOwnsCapturesAndDoesNotDelayFlush) {
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
        fluss::WriteCallback callback = [started, finished, resume,
                                         owned = std::move(lifetime)](fluss::Result result) {
            started->Record(result);
            // Bounded even when a preceding assertion fails.
            resume.wait_for(std::chrono::seconds(20));
            finished->Record(std::move(result));
        };
        ASSERT_OK(writer.Append(row, std::move(callback)));
    }
    ASSERT_TRUE(started->Await());
    EXPECT_FALSE(weak_lifetime.expired());
    ASSERT_OK(writer.Flush());
    // Flush must not wait for a user callback that is waiting for us.
    EXPECT_TRUE(finished->Results().empty());
    writer = fluss::AppendWriter{};
    EXPECT_FALSE(weak_lifetime.expired());
    gate->set_value();
    ASSERT_TRUE(finished->Await());
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
        ASSERT_OK(writer.AppendArrowBatch(
            batch, [completion](fluss::Result result) { completion->Record(std::move(result)); }));
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
    fluss::WriteCallback callback = [completion](fluss::Result result) {
        completion->Record(std::move(result));
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
    auto submitted = writer.Delete(
        Row(), [completion](fluss::Result result) { completion->Record(std::move(result)); });
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
        ASSERT_OK(writer.Append(Row(id), [completion](fluss::Result result) {
            completion->Record(std::move(result));
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
    ASSERT_OK(table_.NewAppend().CreateWriter(
        writer, fluss::WriteCallbackOptions{1, std::chrono::milliseconds(0)}));
    auto completion = std::make_shared<Completion>();
    auto lifetime = std::make_shared<Lifetime>();
    auto released = lifetime->released.get_future();
    fluss::GenericRow invalid(1);
    invalid.SetInt32(0, 1);  // Table requires two columns.
    auto result =
        writer.Append(invalid, [completion, owned = std::move(lifetime)](fluss::Result completed) {
            completion->Record(std::move(completed));
        });
    EXPECT_FALSE(result.Ok());
    EXPECT_EQ(released.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_TRUE(completion->Results().empty());

    result = writer.AppendArrowBatch(nullptr, [completion](fluss::Result completed) {
        completion->Record(std::move(completed));
    });
    EXPECT_FALSE(result.Ok());
    EXPECT_TRUE(completion->Results().empty());
    // Both failed submissions must return the only slot.
    ASSERT_OK(writer.Append(Row(), [completion](fluss::Result completed) {
        completion->Record(std::move(completed));
    }));
    ASSERT_TRUE(completion->Await());
    ASSERT_OK(writer.Flush());
}

TEST_F(WriteCallbackTest, ArrowBatchCapacitySurvivesMovesAndDoesNotAffectWaitOrFlush) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(
        writer, fluss::WriteCallbackOptions{1, std::chrono::milliseconds(250)}));
    arrow::Int32Builder ids;
    arrow::StringBuilder values;
    ASSERT_TRUE(ids.AppendValues({1, 2, 3}).ok());
    ASSERT_TRUE(values.AppendValues({"a", "b", "c"}).ok());
    auto batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("id", arrow::int32()), arrow::field("value", arrow::utf8())}),
        3, {ids.Finish().ValueOrDie(), values.Finish().ValueOrDie()});
    auto started = std::make_shared<Completion>();
    auto gate = std::make_shared<std::promise<void>>();
    auto resume = gate->get_future().share();
    ASSERT_OK(writer.AppendArrowBatch(batch, [started, resume](fluss::Result result) {
        started->Record(std::move(result));
        resume.wait_for(std::chrono::seconds(20));
    }));
    ASSERT_TRUE(started->Await());  // The three-row batch fits in one slot.

    fluss::AppendWriter moved(std::move(writer));
    writer = std::move(moved);
    EXPECT_FALSE(moved.Available());
    auto rejected = std::make_shared<Completion>();
    auto callback = [rejected](fluss::Result result) { rejected->Record(std::move(result)); };
    auto row_result = writer.Append(Row(4), callback);
    auto batch_result = writer.AppendArrowBatch(batch, callback);
    EXPECT_NE(row_result.error_message.find("Timed out"), std::string::npos);
    EXPECT_NE(batch_result.error_message.find("Timed out"), std::string::npos);
    // Independent writers do not share the capacity limit.
    fluss::AppendWriter independent;
    ASSERT_OK(table_.NewAppend().CreateWriter(independent));
    auto other = std::make_shared<Completion>();
    ASSERT_OK(independent.Append(
        Row(5), [other](fluss::Result result) { other->Record(std::move(result)); }));
    ASSERT_TRUE(other->Await());

    fluss::WriteResult pending;
    ASSERT_OK(writer.Append(Row(6), pending));
    ASSERT_OK(pending.Wait());
    ASSERT_OK(writer.Append(Row(7)));
    ASSERT_OK(writer.Flush());  // ACK completion does not return callback capacity.
    EXPECT_TRUE(rejected->Results().empty());
    EXPECT_NE(writer.Append(Row(8), callback).error_message.find("Timed out"), std::string::npos);
    gate->set_value();
    // Admission waits for the previous callback to return, not merely to signal started.
    ASSERT_OK(writer.Append(Row(9), callback));
    ASSERT_TRUE(rejected->Await());
    ASSERT_OK(writer.Flush());
    EXPECT_EQ(rejected->Results().size(), 1u);
}

TEST_F(WriteCallbackTest, UpsertAndDeleteShareCapacityAndReturnItOnSubmissionErrors) {
    CreateTable(true);
    fluss::UpsertWriter writer;
    ASSERT_OK(table_.NewUpsert().CreateWriter(
        writer, fluss::WriteCallbackOptions{1, std::chrono::milliseconds(250)}));
    auto completion = std::make_shared<Completion>();
    auto callback = [completion](fluss::Result result) { completion->Record(std::move(result)); };
    fluss::GenericRow invalid(2);
    invalid.SetString(0, "not an integer primary key");
    invalid.SetString(1, "value");
    EXPECT_FALSE(writer.Upsert(invalid, callback).Ok());
    EXPECT_FALSE(writer.Delete(invalid, callback).Ok());
    EXPECT_TRUE(completion->Results().empty());

    auto gate = std::make_shared<std::promise<void>>();
    auto resume = gate->get_future().share();
    ASSERT_OK(writer.Upsert(Row(), [completion, resume](fluss::Result result) {
        completion->Record(std::move(result));
        resume.wait_for(std::chrono::seconds(20));
    }));
    ASSERT_TRUE(completion->Await());
    fluss::UpsertWriter moved(std::move(writer));
    writer = std::move(moved);
    EXPECT_FALSE(moved.Available());
    EXPECT_NE(writer.Upsert(Row(2), callback).error_message.find("Timed out"), std::string::npos);
    EXPECT_NE(writer.Delete(Row(), callback).error_message.find("Timed out"), std::string::npos);
    ASSERT_OK(writer.Flush());
    gate->set_value();
    ASSERT_OK(writer.Delete(Row(), callback));
    ASSERT_TRUE(completion->Await(2));
    ASSERT_OK(writer.Flush());
    EXPECT_EQ(completion->Results().size(), 2u);
}

TEST_F(WriteCallbackTest, CallbackDoesNotWaitForCapacityOnAnotherWriter) {
    CreateTable();
    auto full_writer = std::make_shared<fluss::AppendWriter>();
    ASSERT_OK(table_.NewAppend().CreateWriter(
        *full_writer, fluss::WriteCallbackOptions{1, std::chrono::seconds(5)}));
    auto started = std::make_shared<Completion>();
    auto gate = std::make_shared<std::promise<void>>();
    auto resume = gate->get_future().share();
    ASSERT_OK(full_writer->Append(Row(), [started, resume](fluss::Result result) {
        started->Record(std::move(result));
        resume.wait_for(std::chrono::seconds(20));
    }));
    ASSERT_TRUE(started->Await());
    fluss::AppendWriter other;
    ASSERT_OK(table_.NewAppend().CreateWriter(other));
    auto attempted = std::make_shared<Completion>();
    auto unexpected = std::make_shared<Completion>();
    auto row = std::make_shared<fluss::GenericRow>(Row(2));
    ASSERT_OK(other.Append(Row(3), [full_writer, row, attempted, unexpected](fluss::Result) {
        // No submitting thread accesses full_writer concurrently with this callback.
        auto result = full_writer->Append(*row, [unexpected](fluss::Result completed) {
            unexpected->Record(std::move(completed));
        });
        attempted->Record(std::move(result));
    }));
    ASSERT_TRUE(attempted->Await());
    EXPECT_EQ(attempted->Results().front().error_message, "Write callback capacity is full");
    EXPECT_TRUE(unexpected->Results().empty());
    gate->set_value();
    ASSERT_OK(other.Flush());
}

TEST_F(WriteCallbackTest, BatchedCallbacksSurviveExceptionsAndCoexistWithWait) {
    CreateTable();
    fluss::AppendWriter writer;
    ASSERT_OK(table_.NewAppend().CreateWriter(writer));
    auto completion = std::make_shared<Completion>();
    constexpr int count = 1024;
    for (int i = 0; i < count; ++i) {
        // The same bucket key encourages shared internal batches.
        ASSERT_OK(writer.Append(Row(1), [completion, i](fluss::Result result) {
            completion->Record(std::move(result));
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
        ASSERT_OK(writer.Delete(
            Row(), [completion](fluss::Result result) { completion->Record(std::move(result)); }));
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
    ASSERT_OK(writer.AppendArrowBatch(
        batch, [completion](fluss::Result result) { completion->Record(std::move(result)); }));
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
    auto callback = [completion](fluss::Result result) { completion->Record(std::move(result)); };
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
        [completion, owned = std::move(lifetime)](fluss::Result result) {
            completion->Record(std::move(result));
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
    fluss::ffi::WriteCallback callback([owned = std::move(lifetime)](fluss::Result) {
        throw std::runtime_error("callback failure");
    });
    EXPECT_NO_THROW(callback.Complete(0, ""));
    EXPECT_EQ(released.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    fluss::ffi::WriteCallback unknown([](fluss::Result) { throw 42; });
    EXPECT_NO_THROW(unknown.Complete(0, ""));
}

TEST(WriteCallbackBridgeTest, ValidatesCallbackOptions) {
    fluss::WriteCallbackOptions options;
    EXPECT_EQ(options.max_pending_operations, 65536u);
    EXPECT_EQ(options.enqueue_timeout, std::chrono::seconds(30));
    EXPECT_OK(fluss::ffi::WriteCallbackCapacity::Validate(options));
    options.max_pending_operations = 0;
    EXPECT_FALSE(fluss::ffi::WriteCallbackCapacity::Validate(options).Ok());
    fluss::Table table;
    fluss::AppendWriter append;
    fluss::UpsertWriter upsert;
    EXPECT_EQ(table.NewAppend().CreateWriter(append, options).error_message,
              "max_pending_operations must be positive");
    EXPECT_EQ(table.NewUpsert().CreateWriter(upsert, options).error_message,
              "max_pending_operations must be positive");
    options.max_pending_operations = 1;
    options.enqueue_timeout = std::chrono::milliseconds(-1);
    EXPECT_FALSE(fluss::ffi::WriteCallbackCapacity::Validate(options).Ok());
    EXPECT_EQ(table.NewAppend().CreateWriter(append, options).error_message,
              "enqueue_timeout must be nonnegative");
    EXPECT_EQ(table.NewUpsert().CreateWriter(upsert, options).error_message,
              "enqueue_timeout must be nonnegative");
    options.enqueue_timeout = std::chrono::milliseconds(0);
    EXPECT_OK(fluss::ffi::WriteCallbackCapacity::Validate(options));
}

TEST(WriteCallbackBridgeTest, CapacityLastsThroughCallbackAndCaptureCleanup) {
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(
        fluss::WriteCallbackOptions{1, std::chrono::milliseconds(0)});
    // This deleter runs after the user callback returns, but before its slot is returned.
    auto capture = std::shared_ptr<int>(new int(0), [capacity](int* value) {
        EXPECT_FALSE(capacity->Acquire().Ok());
        delete value;
    });
    fluss::ffi::WriteCallback callback([capacity, owned = std::move(capture)](fluss::Result) {
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
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(
        fluss::WriteCallbackOptions{1, std::chrono::milliseconds(0)});
    int calls = 0;
    try {
        fluss::ffi::WriteCallback callback([&](fluss::Result) { ++calls; });
        ASSERT_OK(callback.Reserve(capacity));
        throw std::bad_alloc();
    } catch (const std::bad_alloc&) {
    }
    EXPECT_EQ(calls, 0);
    ASSERT_OK(capacity->Acquire());
    capacity->Release();
}

TEST(WriteCallbackBridgeTest, ReservationOutlivesWriterOwnership) {
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(
        fluss::WriteCallbackOptions{1, std::chrono::milliseconds(0)});
    std::weak_ptr<fluss::ffi::WriteCallbackCapacity> weak = capacity;
    fluss::ffi::WriteCallback callback([](fluss::Result) {});
    ASSERT_OK(callback.Reserve(capacity));
    capacity.reset();
    EXPECT_FALSE(weak.expired());
    callback.Complete(0, "");
    EXPECT_TRUE(weak.expired());
}

TEST(WriteCallbackBridgeTest, CapacityTimeoutDoesNotDiscardAcceptedCallback) {
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(
        fluss::WriteCallbackOptions{1, std::chrono::milliseconds(25)});
    int calls = 0;
    fluss::ffi::WriteCallback accepted([&](fluss::Result) { ++calls; });
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
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(
        fluss::WriteCallbackOptions{1, std::chrono::seconds(5)});
    fluss::ffi::WriteCallback accepted([](fluss::Result) {});
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
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(
        fluss::WriteCallbackOptions{1, std::chrono::milliseconds(25)});
    ASSERT_OK(capacity->Acquire());  // Full writer unrelated to the executing callback.
    fluss::ffi::WriteCallback callback([capacity](fluss::Result) {
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
    auto capacity = std::make_shared<fluss::ffi::WriteCallbackCapacity>(
        fluss::WriteCallbackOptions{limit, std::chrono::seconds(5)});
    std::atomic<size_t> active{0};
    std::atomic<size_t> completed{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < 250; ++j) {
                fluss::ffi::WriteCallback callback([&](fluss::Result) {
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
