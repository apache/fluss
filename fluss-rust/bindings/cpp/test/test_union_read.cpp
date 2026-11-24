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

#include "test_utils.h"

namespace {

using namespace fluss;

TEST(UnionReadTest, FrozenSnapshotAndResumedFullChanges) {
    auto* env = fluss_test::FlussTestEnvironment::Instance();
    TablePath path("fluss", "cpp_union_read_full");
    auto desc = TableDescriptor::NewBuilder()
                    .SetSchema(Schema::NewBuilder()
                                   .AddColumn("id", DataType::Int())
                                   .AddColumn("price", DataType::Int())
                                   .SetPrimaryKeys({"id"})
                                   .Build())
                    .SetBucketCount(1)
                    .SetProperty("table.changelog.image", "FULL")
                    .Build();
    fluss_test::CreateTable(env->GetAdmin(), path, desc);
    Table table;
    ASSERT_OK(env->GetConnection().GetTable(path, table));
    UpsertWriter writer;
    ASSERT_OK(table.NewUpsert().CreateWriter(writer));
    auto write = [&](int price) {
        GenericRow row(2);
        row.SetInt32(0, 1);
        row.SetInt32(1, price);
        WriteResult ack;
        ASSERT_OK(writer.Upsert(row, ack));
        ASSERT_OK(ack.Wait());
    };
    write(130);
    std::vector<uint8_t> context;
    ASSERT_OK(table.CaptureUnionReadContext(context));
    write(90);
    auto scan = table.NewScan();
    scan.ProjectByName({"price"}).Filter(Col("price").GreaterThan(100));
    UnionReadReader reader;
    UnionReadEvent event;
    ASSERT_OK(table.NewScan().ProjectByIndex({}).CreateBoundedUnionReader(context, reader));
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Snapshot);
    ASSERT_EQ(event.snapshot->num_columns(), 0);
    ASSERT_EQ(event.snapshot->num_rows(), 1);

    ASSERT_OK(scan.CreateBoundedUnionReader(context, reader));
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Snapshot);
    auto initial = event.snapshot;
    ASSERT_EQ(initial->num_columns(), 1);
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Finished);
    ASSERT_EQ(std::static_pointer_cast<arrow::Int32Array>(initial->column(0))->Value(0), 130);

    ASSERT_OK(scan.CreateStreamingUnionReader(context, reader));
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Snapshot);
    ASSERT_EQ(event.snapshot->num_columns(), 1);
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::SnapshotFinished);
    ASSERT_TRUE(event.checkpoint.has_value());
    auto checkpoint = *event.checkpoint;
    ASSERT_EQ(checkpoint.next_offsets.size(), 1u);
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Changes);
    ASSERT_EQ(event.changes.size(), 1u);
    auto first = event.changes[0];
    ASSERT_EQ(first.change_types, std::vector<ChangeType>{ChangeType::UpdateBefore});
    ASSERT_EQ(event.next_offsets.size(), 1u);
    ASSERT_GT(event.next_offsets[0].offset, checkpoint.next_offsets[0].offset);
    checkpoint.next_offsets = event.next_offsets;
    auto other = table.NewScan();
    EXPECT_FALSE(other.ResumeStreamingUnionReader(context, checkpoint, reader).Ok());
    ASSERT_OK(scan.ResumeStreamingUnionReader(context, checkpoint, reader));
    ASSERT_OK(reader.Next(1, event));
    ASSERT_EQ(event.kind, UnionReadKind::TimedOut);
    write(120);
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Changes);
    ASSERT_EQ(event.changes.size(), 1u);
    ASSERT_EQ(event.changes[0].change_types, std::vector<ChangeType>{ChangeType::UpdateAfter});
    ASSERT_EQ(
        std::static_pointer_cast<arrow::Int32Array>(event.changes[0].batch->column(0))->Value(0),
        120);
    reader = UnionReadReader{};
    EXPECT_EQ(std::static_pointer_cast<arrow::Int32Array>(first.batch->column(0))->Value(0), 130);
    EXPECT_EQ(initial->num_rows(), 1);
}

TEST(UnionReadTest, CheckpointAdvancesPastFilteredAppends) {
    auto* env = fluss_test::FlussTestEnvironment::Instance();
    TablePath path("fluss", "cpp_union_read_append");
    auto desc = TableDescriptor::NewBuilder()
                    .SetSchema(Schema::NewBuilder().AddColumn("value", DataType::Int()).Build())
                    .SetBucketCount(1)
                    .Build();
    fluss_test::CreateTable(env->GetAdmin(), path, desc);
    Table table;
    ASSERT_OK(env->GetConnection().GetTable(path, table));
    AppendWriter writer;
    ASSERT_OK(table.NewAppend().CreateWriter(writer));
    auto append = [&](int value) {
        GenericRow row(1);
        row.SetInt32(0, value);
        WriteResult ack;
        ASSERT_OK(writer.Append(row, ack));
        ASSERT_OK(ack.Wait());
    };
    std::vector<uint8_t> context;
    ASSERT_OK(table.CaptureUnionReadContext(context));
    auto scan = table.NewScan();
    scan.Filter(Col("value").GreaterThan(0));
    UnionReadReader reader;
    UnionReadEvent event;
    ASSERT_OK(scan.CreateStreamingUnionReader(context, reader));
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::SnapshotFinished);
    ASSERT_TRUE(event.checkpoint.has_value());
    auto checkpoint = *event.checkpoint;
    ASSERT_EQ(checkpoint.next_offsets.size(), 1u);
    auto start = checkpoint.next_offsets[0].offset;
    append(-1);
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Changes);
    ASSERT_TRUE(event.changes.empty());
    ASSERT_EQ(event.next_offsets.size(), 1u);
    ASSERT_EQ(event.next_offsets[0].offset, start + 1);
    checkpoint.next_offsets = event.next_offsets;
    ASSERT_OK(scan.ResumeStreamingUnionReader(context, checkpoint, reader));
    append(42);
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Changes);
    ASSERT_EQ(event.changes.size(), 1u);
    EXPECT_TRUE(event.changes[0].change_types.empty());
    EXPECT_EQ(
        std::static_pointer_cast<arrow::Int32Array>(event.changes[0].batch->column(0))->Value(0),
        42);
}

}  // namespace
