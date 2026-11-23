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

#ifdef FLUSS_ENABLE_UNION_READ
#include <arrow/api.h>

#include "test_utils.h"

namespace {

using namespace fluss;

TEST(UnionReadTest, FrozenSnapshotAndResumedWalDeletes) {
    auto* env = fluss_test::FlussTestEnvironment::Instance();
    TablePath path("fluss", "cpp_union_read_wal");
    auto desc = TableDescriptor::NewBuilder()
                    .SetSchema(Schema::NewBuilder()
                                   .AddColumn("id", DataType::Int())
                                   .AddColumn("price", DataType::Int())
                                   .SetPrimaryKeys({"id"})
                                   .Build())
                    .SetBucketCount(1)
                    .SetProperty("table.changelog.image", "WAL")
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
    ASSERT_EQ(event.snapshot->schema()->field(1)->name(), "id");
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::SnapshotFinished);
    ASSERT_EQ(event.next_offsets.size(), 1u);
    auto checkpoint = event.next_offsets;
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Changes);
    ASSERT_EQ(event.changes.Count(), 1u);
    auto first = *event.changes.begin();
    ASSERT_EQ(first.change_type, ChangeType::Delete);
    ASSERT_EQ(event.next_offsets.size(), 1u);
    ASSERT_GT(event.next_offsets[0].offset, checkpoint[0].offset);
    checkpoint = event.next_offsets;
    ASSERT_OK(scan.ResumeStreamingUnionReader(context, checkpoint, reader));
    ASSERT_OK(reader.Next(1, event));
    ASSERT_EQ(event.kind, UnionReadKind::TimedOut);
    write(70);
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Changes);
    ASSERT_EQ(event.changes.Count(), 1u);
    ASSERT_EQ((*event.changes.begin()).change_type, ChangeType::Delete);
    ASSERT_EQ((*event.changes.begin()).offset, checkpoint[0].offset);
    reader = UnionReadReader{};
    EXPECT_EQ(first.row.GetInt32("id"), 1);
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
    ASSERT_EQ(event.next_offsets.size(), 1u);
    auto start = event.next_offsets[0].offset;
    append(-1);
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Changes);
    ASSERT_EQ(event.changes.Count(), 0u);
    ASSERT_EQ(event.next_offsets.size(), 1u);
    ASSERT_EQ(event.next_offsets[0].offset, start + 1);
    ASSERT_OK(scan.ResumeStreamingUnionReader(context, event.next_offsets, reader));
    append(42);
    ASSERT_OK(reader.Next(10000, event));
    ASSERT_EQ(event.kind, UnionReadKind::Changes);
    ASSERT_EQ(event.changes.Count(), 1u);
    EXPECT_EQ((*event.changes.begin()).row.GetInt32("value"), 42);
}

}  // namespace
#endif
