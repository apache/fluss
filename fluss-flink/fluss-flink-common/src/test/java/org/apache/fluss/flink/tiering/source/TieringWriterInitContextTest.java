/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TieringWriterInitContext}. */
class TieringWriterInitContextTest {

    private static final long TABLE_ID = 1L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final int TABLE_BUCKET_COUNT = 8;

    @Test
    void testIoTmpDir() {
        TieringWriterInitContext defaultContext =
                newContext(new TableBucket(TABLE_ID, 0), null, null, null);
        TieringWriterInitContext context =
                newContext(
                        new TableBucket(TABLE_ID, 0),
                        null,
                        null,
                        new String[] {"/flink_tmp_0/fluss", "/flink_tmp_1/fluss"});

        assertThat(defaultContext.ioTmpDirs()).isNull();
        assertThat(context.ioTmpDirs()).containsExactly("/flink_tmp_0/fluss", "/flink_tmp_1/fluss");
    }

    @Test
    void testNonPartitionedFallsBackToTableLevelCount() {
        // A non-partitioned bucket carries no per-partition count; the table-level count applies.
        TieringWriterInitContext context = newContext(new TableBucket(TABLE_ID, 0), null, null);
        assertThat(context.bucketCount()).isEqualTo(TABLE_BUCKET_COUNT);
    }

    @Test
    void testPartitionedUsesPerPartitionCount() {
        // A partitioned bucket must use its own actual bucket count (bucket.num.actual).
        TieringWriterInitContext context =
                newContext(new TableBucket(TABLE_ID, 1L, 0), "2024-01", 4);
        assertThat(context.bucketCount()).isEqualTo(4);
    }

    @Test
    void testPartitionedWithoutCountFailsLoud() {
        // A partitioned bucket with no resolved per-partition count must fail loudly rather than
        // silently guessing (which would corrupt the lake table's bucket layout).
        assertThatThrownBy(() -> newContext(new TableBucket(TABLE_ID, 1L, 0), "2024-01", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("actual bucket count");
    }

    private static TieringWriterInitContext newContext(
            TableBucket tableBucket, String partition, Integer partitionBucketCount) {
        return newContext(tableBucket, partition, partitionBucketCount, null);
    }

    private static TieringWriterInitContext newContext(
            TableBucket tableBucket,
            String partition,
            Integer partitionBucketCount,
            String[] ioTmpDirs) {
        return new TieringWriterInitContext(
                TABLE_PATH,
                tableBucket,
                partition,
                createTableInfo(TABLE_BUCKET_COUNT),
                0,
                0L,
                partitionBucketCount,
                ioTmpDirs);
    }

    private static TableInfo createTableInfo(int numBuckets) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        return new TableInfo(
                TABLE_PATH,
                TABLE_ID,
                0,
                schema,
                Collections.emptyList(),
                Collections.emptyList(),
                numBuckets,
                new Configuration(),
                new Configuration(),
                DEFAULT_REMOTE_DATA_DIR,
                null,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}
