/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.BytesType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.types.DateType;
import com.alibaba.fluss.types.DoubleType;
import com.alibaba.fluss.types.FloatType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.SmallIntType;
import com.alibaba.fluss.types.StringType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;
import com.alibaba.fluss.types.TinyIntType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.utils.PartitionUtils.convertValueOfType;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for Fluss partitioned table supporting partition key of different types. */
class NewPartitionedTableITCase extends ClientToServerITCaseBase {
    Schema.Builder schemaBuilder =
            Schema.newBuilder()
                    .column("a", new StringType())
                    .column("char", new CharType())
                    .column("binary", new BinaryType())
                    .column("boolean", new BooleanType())
                    .column("bytes", new BytesType())
                    .column("tinyInt", new TinyIntType())
                    .column("smallInt", new SmallIntType())
                    .column("int", new IntType())
                    .column("bigInt", new BigIntType())
                    .column("date", new DateType())
                    .column("float", new FloatType())
                    .column("double", new DoubleType())
                    .column("time", new TimeType())
                    .column("timeStampNTZ", new TimestampType())
                    .column("timeStampLTZ", new LocalZonedTimestampType());

    Schema schema = schemaBuilder.build();
    DataTypeRoot[] allPartitionKeyTypes =
            new DataTypeRoot[] {
                DataTypeRoot.STRING,
                DataTypeRoot.CHAR,
                DataTypeRoot.BINARY,
                DataTypeRoot.BOOLEAN,
                DataTypeRoot.BYTES,
                DataTypeRoot.TINYINT,
                DataTypeRoot.SMALLINT,
                DataTypeRoot.INTEGER,
                DataTypeRoot.BIGINT,
                DataTypeRoot.DATE,
                DataTypeRoot.FLOAT,
                DataTypeRoot.DOUBLE,
                DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            };

    Object[] allPartitionKeyValues =
            new Object[] {
                BinaryString.fromString("a"),
                BinaryString.fromString("F"),
                new byte[] {0x10, 0x20, 0x30, 0x40, 0x50, (byte) 0b11111111},
                true,
                new byte[] {0x10, 0x20, 0x30, 0x40, 0x50, (byte) 0b11111111},
                (byte) 100,
                (short) -32760, // smallint
                299000, // Integer
                1748662955428L, // Bigint
                20235, // Date
                5.73f, // Float
                5.73, // Double
                5402199, // Time
                TimestampNtz.fromMillis(1748662955428L), // TIME_WITHOUT_TIME_ZONE
                TimestampLtz.fromEpochMillis(1748662955428L) // TIMESTAMP_WITH_LOCAL_TIME_ZONE
            };

    Schema.Column[] extraColumn =
            new Schema.Column[] {
                new Schema.Column("a", new StringType()),
                new Schema.Column("char", new CharType()),
                new Schema.Column("binary", new BinaryType()),
                new Schema.Column("boolean", new BooleanType()),
                new Schema.Column("bytes", new BytesType()),
                new Schema.Column("tinyInt", new TinyIntType()),
                new Schema.Column("smallInt", new SmallIntType()),
                new Schema.Column("int", new IntType()),
                new Schema.Column("bigInt", new BigIntType()),
                new Schema.Column("date", new DateType()),
                new Schema.Column("float", new FloatType()),
                new Schema.Column("double", new DoubleType()),
                new Schema.Column("time", new TimeType()),
                new Schema.Column("timeStampNTZ", new TimestampType()),
                new Schema.Column("timeStampLTZ", new LocalZonedTimestampType())
            };

    List<String> result =
            Arrays.asList(
                    "a",
                    "F",
                    "1020304050ff",
                    "true",
                    "1020304050ff",
                    "100",
                    "-32760",
                    "299000",
                    "1748662955428",
                    "2025-05-27",
                    "5_73",
                    "5_73",
                    "01-30-02_199",
                    "2025-05-31-03-42-35_428",
                    "2025-05-31-03-42-35_428");

    @Test
    void testMultipleTypedPartitionedTable() throws Exception {

        for (int i = 0; i < allPartitionKeyTypes.length; i++) {
            String partitionKey = extraColumn[i].getName();
            TablePath tablePath =
                    TablePath.of("test_part_db_" + i, "test_static_partitioned_pk_table_" + i);
            createPartitionedTable(tablePath, partitionKey);
            String partitionValue =
                    convertValueOfType(allPartitionKeyValues[i], allPartitionKeyTypes[i]);

            admin.createPartition(tablePath, newPartitionSpec(partitionKey, partitionValue), true)
                    .get();

            Map<String, Long> partitionIdByNames =
                    FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1);

            List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
            List<String> expectedPartitions = new ArrayList<>(partitionIdByNames.keySet());
            assertThat(
                            partitionInfos.stream()
                                    .map(PartitionInfo::getPartitionName)
                                    .collect(Collectors.toList()))
                    .containsExactlyInAnyOrderElementsOf(expectedPartitions);

            Table table = conn.getTable(tablePath);
            AppendWriter appendWriter = table.newAppend().createWriter();
            Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
            for (String partition : partitionIdByNames.keySet()) {
                for (int j = 0; j < allPartitionKeyValues.length; j++) {
                    InternalRow row = row(allPartitionKeyValues);
                    appendWriter.append(row);
                    expectPartitionAppendRows
                            .computeIfAbsent(
                                    partitionIdByNames.get(partition), k -> new ArrayList<>())
                            .add(row);
                }
            }
            appendWriter.flush();

            assertThat(admin.listPartitionInfos(tablePath).get().get(0).getPartitionName())
                    .isEqualTo(result.get(i));
        }
    }

    private void createPartitionedTable(TablePath tablePath, String partitionKey) throws Exception {
        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder().schema(schema).partitionedBy(partitionKey).build();
        createTable(tablePath, partitionTableDescriptor, false);
    }
}
