/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.types.Tuple2;

import java.util.Arrays;
import java.util.List;

/** utils to create test data. */
public final class TestData {
    public static final short DEFAULT_SCHEMA_ID = 1;
    public static final long BASE_OFFSET = 0L;
    public static final byte DEFAULT_MAGIC = (byte) 0;
    // ---------------------------- data1 and related table info begin ---------------------------
    public static final List<Object[]> DATA1 =
            Arrays.asList(
                    new Object[] {1, "a"},
                    new Object[] {2, "b"},
                    new Object[] {3, "c"},
                    new Object[] {4, "d"},
                    new Object[] {5, "e"},
                    new Object[] {6, "f"},
                    new Object[] {7, "g"},
                    new Object[] {8, "h"},
                    new Object[] {9, "i"},
                    new Object[] {10, "j"});
    public static final List<Object[]> ANOTHER_DATA1 =
            Arrays.asList(
                    new Object[] {1, "a1"},
                    new Object[] {2, "b1"},
                    new Object[] {3, "c1"},
                    new Object[] {4, "d1"},
                    new Object[] {5, "e1"},
                    new Object[] {6, "f1"},
                    new Object[] {7, "g1"},
                    new Object[] {8, "h1"},
                    new Object[] {9, "i1"},
                    new Object[] {10, "j1"});

    public static final RowType DATA1_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("a", DataTypes.INT()), new DataField("b", DataTypes.STRING()));

    // for log table
    public static final long DATA1_TABLE_ID = 150001L;
    public static final Schema DATA1_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .build();
    public static final TablePath DATA1_TABLE_PATH =
            TablePath.of("test_db_1", "test_non_pk_table_1");

    public static final TableDescriptor DATA1_TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(3).build();
    public static final PhysicalTablePath DATA1_PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(DATA1_TABLE_PATH);
    public static final TableInfo DATA1_TABLE_INFO =
            TableInfo.of(
                    DATA1_TABLE_PATH,
                    DATA1_TABLE_ID,
                    1,
                    DATA1_TABLE_DESCRIPTOR,
                    System.currentTimeMillis(),
                    System.currentTimeMillis());

    // for log table / partition table
    public static final TableDescriptor DATA1_PARTITIONED_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DATA1_SCHEMA)
                    .distributedBy(3)
                    .partitionedBy("b")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR)
                    .build();
    public static final PhysicalTablePath DATA1_PHYSICAL_TABLE_PATH_PA_2024 =
            PhysicalTablePath.of(DATA1_TABLE_PATH, "2024");
    public static final TableInfo DATA1_PARTITIONED_TABLE_INFO =
            TableInfo.of(
                    DATA1_TABLE_PATH,
                    DATA1_TABLE_ID,
                    1,
                    DATA1_PARTITIONED_TABLE_DESCRIPTOR,
                    System.currentTimeMillis(),
                    System.currentTimeMillis());

    // for pk table
    public static final RowType DATA1_KEY_TYPE = DataTypes.ROW(new DataField("a", DataTypes.INT()));
    public static final Schema DATA1_SCHEMA_PK =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .primaryKey("a")
                    .build();
    public static final TablePath DATA1_TABLE_PATH_PK =
            TablePath.of("test_db_1", "test_pk_table_1");
    public static final long DATA1_TABLE_ID_PK = 150003L;

    public static final TableDescriptor DATA1_TABLE_DESCRIPTOR_PK =
            TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(3, "a").build();
    public static final PhysicalTablePath DATA1_PHYSICAL_TABLE_PATH_PK =
            PhysicalTablePath.of(DATA1_TABLE_PATH_PK);
    public static final TableInfo DATA1_TABLE_INFO_PK =
            TableInfo.of(
                    DATA1_TABLE_PATH_PK,
                    DATA1_TABLE_ID_PK,
                    1,
                    DATA1_TABLE_DESCRIPTOR_PK,
                    System.currentTimeMillis(),
                    System.currentTimeMillis());

    public static final PhysicalTablePath DATA1_PHYSICAL_TABLE_PATH_PK_PA_2024 =
            PhysicalTablePath.of(DATA1_TABLE_PATH_PK, "2024");

    public static final List<Tuple2<Object[], Object[]>> DATA_1_WITH_KEY_AND_VALUE =
            Arrays.asList(
                    Tuple2.of(new Object[] {1}, new Object[] {1, "a"}),
                    Tuple2.of(new Object[] {2}, new Object[] {2, "b"}),
                    Tuple2.of(new Object[] {3}, new Object[] {3, "c"}),
                    Tuple2.of(new Object[] {1}, new Object[] {1, "a1"}),
                    Tuple2.of(new Object[] {2}, new Object[] {2, "b1"}),
                    Tuple2.of(new Object[] {3}, null));

    public static final List<Tuple2<RowKind, Object[]>> EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK =
            Arrays.asList(
                    Tuple2.of(RowKind.INSERT, new Object[] {1, "a"}),
                    Tuple2.of(RowKind.INSERT, new Object[] {2, "b"}),
                    Tuple2.of(RowKind.INSERT, new Object[] {3, "c"}),
                    Tuple2.of(RowKind.UPDATE_BEFORE, new Object[] {1, "a"}),
                    Tuple2.of(RowKind.UPDATE_AFTER, new Object[] {1, "a1"}),
                    Tuple2.of(RowKind.UPDATE_BEFORE, new Object[] {2, "b"}),
                    Tuple2.of(RowKind.UPDATE_AFTER, new Object[] {2, "b1"}),
                    Tuple2.of(RowKind.DELETE, new Object[] {3, "c"}));

    // ---------------------------- data1 table info end ------------------------------

    // ------------------- data2 and related table info begin ----------------------
    public static final List<Object[]> DATA2 =
            Arrays.asList(
                    new Object[] {1, "a", "hello"},
                    new Object[] {2, "b", "hi"},
                    new Object[] {3, "c", "nihao"},
                    new Object[] {4, "d", "hello world"},
                    new Object[] {5, "e", "hi world"},
                    new Object[] {6, "f", "nihao world"},
                    new Object[] {7, "g", "hello world2"},
                    new Object[] {8, "h", "hi world2"},
                    new Object[] {9, "i", "nihao world2"},
                    new Object[] {10, "j", "hello world3"});

    public static final RowType DATA2_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("a", DataTypes.INT()),
                    new DataField("b", DataTypes.STRING()),
                    new DataField("c", DataTypes.STRING()));
    public static final Schema DATA2_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.STRING())
                    .withComment("b is second column")
                    .column("c", DataTypes.STRING())
                    .withComment("c is adding column")
                    .primaryKey("a")
                    .build();
    public static final TablePath DATA2_TABLE_PATH = TablePath.of("test_db_2", "test_table_2");
    public static final PhysicalTablePath DATA2_PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(DATA2_TABLE_PATH);
    public static final long DATA2_TABLE_ID = 150002L;
    public static final TableDescriptor DATA2_TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(DATA2_SCHEMA).distributedBy(3, "a").build();
    public static final TableInfo DATA2_TABLE_INFO =
            TableInfo.of(
                    DATA2_TABLE_PATH,
                    DATA2_TABLE_ID,
                    1,
                    DATA2_TABLE_DESCRIPTOR,
                    System.currentTimeMillis(),
                    System.currentTimeMillis());
    // -------------------------------- data2 info end ------------------------------------

    // ------------------- data3 and related table info begin ----------------------
    public static final Schema DATA3_SCHEMA_PK =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .withComment("a is first column")
                    .column("b", DataTypes.BIGINT())
                    .withComment("b is second column")
                    .primaryKey("a")
                    .build();
    public static final TablePath DATA3_TABLE_PATH_PK =
            TablePath.of("test_db_3", "test_pk_table_3");
    // ---------------------------- data3 table info end ------------------------------

}
