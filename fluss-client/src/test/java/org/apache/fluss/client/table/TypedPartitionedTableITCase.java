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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for Fluss partitioned table supporting partition key of different types. */
class TypedPartitionedTableITCase extends ClientToServerITCaseBase {
    private static final Schema.Builder schemaBuilder =
            Schema.newBuilder()
                    .column("a", new StringType())
                    .column("char", new CharType())
                    .column("binary", new BinaryType(6))
                    .column("boolean", new BooleanType())
                    .column("bytes", new BytesType())
                    .column("tinyInt", new TinyIntType())
                    .column("smallInt", new SmallIntType())
                    .column("int", new IntType())
                    .column("bigInt", new BigIntType())
                    .column("date", new DateType())
                    .column("float", new FloatType())
                    .column("double", new DoubleType())
                    .column("time", new TimeType(3))
                    .column("timeStampNTZ", new TimestampType())
                    .column("timeStampLTZ", new LocalZonedTimestampType());

    private static final Schema schema = schemaBuilder.build();
    private static final DataTypeRoot[] allPartitionKeyTypes =
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

    private static final Object[] allPartitionKeyValues =
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

    private static final Schema.Column[] extraColumn =
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

    private static final List<String> result =
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
    public void testPartitionedTable() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "person");

        TableDescriptor partitionedTable =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .column("dt", DataTypes.DATE())
                                        .build())
                        .distributedBy(3, "name")
                        .partitionedBy("id", "dt")
                        .build();
        admin.createTable(tablePath, partitionedTable, true).get();
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();

        assertThat(tableInfo).isNotNull();
        assertThat(tableInfo.getTablePath()).isEqualTo(tablePath);

        List<String> partitionKeys = tableInfo.getPartitionKeys();
        assertThat(partitionKeys).hasSize(2);
        assertThat(partitionKeys).containsExactly("id", "dt");
    }

    @Test
    public void testMultipleTypedPartitionedTable() throws Exception {
        for (int i = 0; i < allPartitionKeyTypes.length; i++) {
            String partitionKey = extraColumn[i].getName();
            TablePath tablePath =
                    TablePath.of("fluss", "test_static_partitioned_pk_table_" + partitionKey);
            createPartitionedTable(tablePath, partitionKey);

            // append a record to the table will dynamically create the corresponding partition
            Table table = conn.getTable(tablePath);
            AppendWriter appendWriter = table.newAppend().createWriter();
            InternalRow row = row(allPartitionKeyValues);
            appendWriter.append(row);
            appendWriter.flush();

            List<PartitionInfo> actualPartitions = admin.listPartitionInfos(tablePath).get();
            assertThat(actualPartitions).hasSize(1);
            PartitionInfo actualPartition = actualPartitions.get(0);
            assertThat(actualPartition.getPartitionName()).isEqualTo(result.get(i));

            Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
            expectPartitionAppendRows.put(actualPartition.getPartitionId(), Arrays.asList(row));
            // assert result
            verifyPartitionLogs(table, schema.getRowType(), expectPartitionAppendRows);
            admin.dropTable(tablePath, true).get();
        }
    }

    private void createPartitionedTable(TablePath tablePath, String partitionKey) throws Exception {
        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder().schema(schema).partitionedBy(partitionKey).build();
        createTable(tablePath, partitionTableDescriptor, true);
    }
}
