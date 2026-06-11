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
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TooManyPartitionsException;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.apache.fluss.utils.FlussPaths.KV_TABLET_DIR_PREFIX;
import static org.apache.fluss.utils.FlussPaths.LOG_TABLET_DIR_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * IT case for Fluss partitioned table.
 *
 * <p>If you want to add ITCase for auto partitioned table in client, please adding to class {@link
 * AutoPartitionedTableITCase}.
 */
class PartitionedTableITCase extends ClientToServerITCaseBase {

    private static final int IMPLICIT_TEST_RETENTION = 10_000;

    @Test
    void testImplicitPartitionedPrimaryKeyTableEndToEndWithDirectoryLayout() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_implicit_partitioned_pk_e2e_table_1");
        Schema schema = createImplicitPartitionedTable(tablePath, true);
        Table table = conn.getTable(tablePath);
        TimestampNtz eventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        InternalRow writtenRow = row(schema.getRowType(), "us", 1, eventTime, 10, "e2e-value-10");

        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        upsertWriter.upsert(writtenRow).get();
        upsertWriter.flush();

        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getRowType().getFieldNames())
                .containsExactly("region", "id", "event_time", "seq", "payload");
        assertThat(tableInfo.getRowType().getFieldNames()).doesNotContain("event_day");
        assertThat(tableInfo.getPrimaryKeys()).containsExactly("region", "id", "event_time", "seq");
        assertThat(tableInfo.getPartitionKeys()).containsExactly("region", "event_day");
        assertThat(tableInfo.getPhysicalPartitionKeys()).containsExactly("region");
        assertThat(tableInfo.getVirtualPartitionKeys()).containsExactly("event_day");
        assertThat(tableInfo.getPartitionSourceColumns()).containsExactly("event_time");
        assertThat(tableInfo.getPartitionExpressions()).hasSize(1);
        assertThat(tableInfo.getPartitionExpressions().get(0).getVirtualPartitionSpecKey())
                .hasValue("event_day");

        List<PartitionInfo> partitionInfos = waitForPartitionInfos(tablePath, 1);
        PartitionInfo partitionInfo = partitionInfos.get(0);
        assertThat(partitionInfo.getPartitionName()).isEqualTo("us$20240315");
        assertThat(partitionInfo.getPartitionSpec().getSpecMap())
                .containsEntry("region", "us")
                .containsEntry("event_day", "20240315");
        assertThat(FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1))
                .containsEntry("us$20240315", partitionInfo.getPartitionId());

        Lookuper lookuper = table.newLookup().createLookuper();
        assertThatRow(lookupRow(lookuper, row("us", 1, eventTime, 10)))
                .withSchema(schema.getRowType())
                .isEqualTo(writtenRow);

        assertPartitionReplicaDirectories(
                tablePath, tableInfo.getTableId(), partitionInfo.getPartitionId(), "us$20240315");
    }

    @Test
    void testImplicitPartitionedPrimaryKeyTableWriteLookupAndDelete() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_implicit_partitioned_pk_table_1");
        Schema schema =
                Schema.newBuilder()
                        .column("region", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("event_time", DataTypes.TIMESTAMP())
                        .column("seq", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("region", "id", "event_time", "seq")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.column("region"),
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))))
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION,
                                IMPLICIT_TEST_RETENTION)
                        .distributedBy(2, "id", "event_time")
                        .build();
        createTable(tablePath, descriptor, false);
        admin.createPartition(
                        tablePath,
                        newPartitionSpec(
                                Arrays.asList("region", "event_day"),
                                Arrays.asList("us", "20240315")),
                        false)
                .get();
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1);
        assertThat(partitionIdByNames).containsKey("us$20240315");
        assertThat(
                        admin.listPartitionInfos(
                                        tablePath, newPartitionSpec("event_day", "20240315"))
                                .get())
                .hasSize(1);

        TimestampNtz eventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        InternalRow firstRow = row(schema.getRowType(), "us", 1, eventTime, 10, "value-10");
        InternalRow secondRow = row(schema.getRowType(), "us", 1, eventTime, 11, "value-11");

        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        upsertWriter.upsert(firstRow).get();
        upsertWriter.upsert(secondRow).get();
        upsertWriter.flush();

        Lookuper lookuper = table.newLookup().createLookuper();
        assertThatRow(lookupRow(lookuper, row("us", 1, eventTime, 10)))
                .withSchema(schema.getRowType())
                .isEqualTo(firstRow);

        Lookuper prefixLookuper =
                table.newLookup()
                        .lookupBy(Arrays.asList("region", "id", "event_time"))
                        .createLookuper();
        List<InternalRow> prefixRows =
                prefixLookuper.lookup(row("us", 1, eventTime)).get().getRowList();
        assertThat(prefixRows).hasSize(2);

        Lookuper reorderedPrefixLookuper =
                table.newLookup()
                        .lookupBy(Arrays.asList("id", "event_time", "region"))
                        .createLookuper();
        assertThat(reorderedPrefixLookuper.lookup(row(1, eventTime, "us")).get().getRowList())
                .hasSize(2);
        assertThat(reorderedPrefixLookuper.lookup(row(1, eventTime, "eu")).get().getRowList())
                .isEmpty();

        upsertWriter.delete(row("us", 1, eventTime, 10)).get();
        upsertWriter.flush();

        assertThat(lookupRow(lookuper, row("us", 1, eventTime, 10))).isNull();
        prefixRows = prefixLookuper.lookup(row("us", 1, eventTime)).get().getRowList();
        assertThat(prefixRows).hasSize(1);
        assertThatRow(prefixRows.get(0)).withSchema(schema.getRowType()).isEqualTo(secondRow);
    }

    @Test
    void testImplicitPartitionPrefixLookupWithRoutingOnlyTransformSource() throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partitioned_prefix_lookup_table_1");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("event_time", DataTypes.TIMESTAMP())
                        .column("seq", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .primaryKey("id", "event_time", "seq")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))))
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION,
                                IMPLICIT_TEST_RETENTION)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 0)
                        .distributedBy(2, "id")
                        .build();
        createTable(tablePath, descriptor, false);
        admin.createPartition(tablePath, newPartitionSpec("event_day", "20240315"), false).get();
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1);

        TimestampNtz firstEventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        TimestampNtz secondEventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 11, 30));
        InternalRow firstRow = row(schema.getRowType(), 1, firstEventTime, 10, "value-10");
        InternalRow secondRow = row(schema.getRowType(), 1, secondEventTime, 11, "value-11");
        InternalRow otherIdRow = row(schema.getRowType(), 2, firstEventTime, 12, "value-12");

        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        upsertWriter.upsert(firstRow).get();
        upsertWriter.upsert(secondRow).get();
        upsertWriter.upsert(otherIdRow).get();
        upsertWriter.flush();

        Lookuper prefixLookuper =
                table.newLookup().lookupBy(Arrays.asList("id", "event_time")).createLookuper();
        List<InternalRow> prefixRows =
                prefixLookuper.lookup(row(1, firstEventTime)).get().getRowList();
        assertThat(prefixRows).hasSize(2);

        assertThat(prefixLookuper.lookup(row(2, firstEventTime)).get().getRowList()).hasSize(1);
    }

    @Test
    void testImplicitPartitionedPrimaryKeyTableDynamicCreatePartition() throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partitioned_pk_dynamic_table_1");
        Schema schema = createImplicitPartitionedTable(tablePath, true);
        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();

        TimestampNtz eventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        InternalRow row = row(schema.getRowType(), "us", 1, eventTime, 10, "value-10");
        upsertWriter.upsert(row).get();
        upsertWriter.flush();

        List<PartitionInfo> partitionInfoList =
                waitValue(
                        () -> {
                            List<PartitionInfo> partitionInfos =
                                    admin.listPartitionInfos(tablePath).get();
                            if (partitionInfos.size() == 1) {
                                return Optional.of(partitionInfos);
                            } else {
                                return Optional.empty();
                            }
                        },
                        Duration.ofMinutes(1),
                        "Fail to wait for the implicit partition created.");
        assertThat(partitionInfoList.get(0).getPartitionName()).isEqualTo("us$20240315");

        Lookuper lookuper = table.newLookup().createLookuper();
        assertThatRow(lookupRow(lookuper, row("us", 1, eventTime, 10)))
                .withSchema(schema.getRowType())
                .isEqualTo(row);
    }

    @Test
    void testImplicitPartitionedLogTableAppend() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_implicit_partitioned_log_table_1");
        Schema schema = createImplicitPartitionedTable(tablePath, false);
        admin.createPartition(
                        tablePath,
                        newPartitionSpec(
                                Arrays.asList("region", "event_day"),
                                Arrays.asList("us", "20240315")),
                        false)
                .get();
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1);
        assertThat(partitionIdByNames).containsKey("us$20240315");

        TimestampNtz firstEventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        TimestampNtz secondEventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 23, 59));
        List<InternalRow> expectedRows =
                Arrays.asList(
                        row(schema.getRowType(), "us", 1, firstEventTime, 10, "value-10"),
                        row(schema.getRowType(), "us", 2, secondEventTime, 20, "value-20"));

        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        for (InternalRow row : expectedRows) {
            appendWriter.append(row).get();
        }
        appendWriter.flush();

        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        expectPartitionAppendRows.put(partitionIdByNames.get("us$20240315"), expectedRows);
        verifyPartitionLogs(table, schema.getRowType(), expectPartitionAppendRows);
    }

    @Test
    void testImplicitPartitionedLogTableDynamicCreatePartitions() throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partitioned_log_dynamic_table_1");
        Schema schema = createImplicitPartitionedTable(tablePath, false);
        TimestampNtz firstEventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        TimestampNtz secondEventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 16, 0, 30));
        InternalRow firstRow = row(schema.getRowType(), "us", 1, firstEventTime, 10, "value-10");
        InternalRow secondRow = row(schema.getRowType(), "us", 2, secondEventTime, 20, "value-20");

        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        appendWriter.append(firstRow).get();
        appendWriter.append(secondRow).get();
        appendWriter.flush();

        List<PartitionInfo> partitionInfos = waitForPartitionInfos(tablePath, 2);
        assertThat(partitionInfos)
                .extracting(PartitionInfo::getPartitionName)
                .containsExactlyInAnyOrder("us$20240315", "us$20240316");

        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            if (partitionInfo.getPartitionName().equals("us$20240315")) {
                expectPartitionAppendRows.put(
                        partitionInfo.getPartitionId(), Arrays.asList(firstRow));
            } else {
                expectPartitionAppendRows.put(
                        partitionInfo.getPartitionId(), Arrays.asList(secondRow));
            }
        }
        verifyPartitionLogs(table, schema.getRowType(), expectPartitionAppendRows);
    }

    @Test
    void testImplicitPartitionPersistsLifecycleDefaultsAndEnforcesRetentionBoundary()
            throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partition_lifecycle_defaults");
        Schema schema =
                Schema.newBuilder()
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .column("payload", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))))
                        .distributedBy(1)
                        .build();
        createTable(tablePath, descriptor, false);

        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED))
                .isTrue();
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION))
                .isEqualTo(7);
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_KEY))
                .isEqualTo("event_day");
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT))
                .isEqualTo(AutoPartitionTimeUnit.DAY);
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE))
                .isEqualTo("UTC");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate utcToday = LocalDate.now(ZoneOffset.UTC);
        String retainedBoundary = utcToday.minusDays(7).format(formatter);
        admin.createPartition(tablePath, newPartitionSpec("event_day", retainedBoundary), false)
                .get();

        String expiredPartition = utcToday.minusDays(8).format(formatter);
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath,
                                                newPartitionSpec("event_day", expiredPartition),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining("is out-of-date");
    }

    @Test
    void testImplicitPartitionLifecycleConstraintsRemainValidAfterAlter() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_implicit_partition_lifecycle_alter");
        Schema schema =
                Schema.newBuilder().column("event_time", DataTypes.TIMESTAMP().copy(false)).build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))))
                        .distributedBy(1)
                        .build();
        createTable(tablePath, descriptor, false);

        assertThatThrownBy(
                        () ->
                                admin.alterTable(
                                                tablePath,
                                                Collections.singletonList(
                                                        TableChange.set(
                                                                ConfigOptions
                                                                        .TABLE_AUTO_PARTITION_ENABLED
                                                                        .key(),
                                                                "false")),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("require 'table.auto-partition.enabled'=true");

        assertThatThrownBy(
                        () ->
                                admin.alterTable(
                                                tablePath,
                                                Collections.singletonList(
                                                        TableChange.set(
                                                                ConfigOptions
                                                                        .TABLE_AUTO_PARTITION_NUM_RETENTION
                                                                        .key(),
                                                                "-1")),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("require non-negative");

        admin.alterTable(
                        tablePath,
                        Arrays.asList(
                                TableChange.reset(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key()),
                                TableChange.reset(
                                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key())),
                        false)
                .get();

        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED))
                .isTrue();
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_KEY))
                .isEqualTo("event_day");
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT))
                .isEqualTo(AutoPartitionTimeUnit.DAY);
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE))
                .isEqualTo("UTC");
        assertThat(tableInfo.getProperties().get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION))
                .isEqualTo(7);
    }

    @Test
    void testMultipleKeyImplicitPartitionRejectsPreCreateOnCreateAndAlter() throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_multiple_key_implicit_partition_precreate");
        Schema schema =
                Schema.newBuilder()
                        .column("region", DataTypes.STRING().copy(false))
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.column("region"),
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))))
                        .distributedBy(1)
                        .build();

        TableDescriptor invalidDescriptor =
                TableDescriptor.builder(descriptor)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 1)
                        .build();
        assertThatThrownBy(() -> createTable(tablePath, invalidDescriptor, false))
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("multiple partition keys")
                .hasMessageContaining(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key())
                .hasMessageContaining("=0");

        admin.createTable(tablePath, descriptor, false).get();
        assertThat(
                        admin.getTableInfo(tablePath)
                                .get()
                                .getProperties()
                                .get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE))
                .isZero();

        assertThatThrownBy(
                        () ->
                                admin.alterTable(
                                                tablePath,
                                                Collections.singletonList(
                                                        TableChange.set(
                                                                ConfigOptions
                                                                        .TABLE_AUTO_PARTITION_NUM_PRECREATE
                                                                        .key(),
                                                                "1")),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("multiple partition keys")
                .hasMessageContaining(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key())
                .hasMessageContaining("=0");
        assertThat(
                        admin.getTableInfo(tablePath)
                                .get()
                                .getProperties()
                                .get(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE))
                .isZero();
    }

    @Test
    void testImplicitPartitionedLogTableWriteNewPartitionFailsWhenDynamicDisabled()
            throws Exception {
        clientConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, false);
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partitioned_log_disabled_dynamic_table_1");
        Schema schema = createImplicitPartitionedTable(tablePath, false);
        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        TimestampNtz eventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        InternalRow row = row(schema.getRowType(), "us", 1, eventTime, 10, "value-10");

        assertThatThrownBy(() -> appendWriter.append(row).get())
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(
                        "Table partition '%s' does not exist.",
                        PhysicalTablePath.of(tablePath, "us$20240315"));
    }

    @Test
    void testImplicitPartitionManagementUsesFinalSpecKeysOnly() throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partition_management_table_1");
        createImplicitPartitionedTable(tablePath, false);
        TableInfo tableInfo = admin.getTableInfo(tablePath).get();
        assertThat(tableInfo.getRowType().getFieldNames()).doesNotContain("event_day");
        DateTruncPartitionTransform transform =
                (DateTruncPartitionTransform)
                        tableInfo.getPartitionExpressions().get(0).getTransform();
        assertThat(transform.getTimeZone()).isEmpty();
        admin.createPartition(
                        tablePath,
                        newPartitionSpec(
                                Arrays.asList("region", "event_day"),
                                Arrays.asList("us", "20240315")),
                        false)
                .get();

        assertThat(
                        admin.listPartitionInfos(
                                        tablePath, newPartitionSpec("event_day", "20240315"))
                                .get())
                .hasSize(1);
        assertThatThrownBy(
                        () ->
                                admin.listPartitionInfos(
                                                tablePath,
                                                newPartitionSpec("event_time", "2024-03-15"))
                                        .get())
                .cause()
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("table don't contains this partitionKey: event_time");
        assertThatThrownBy(
                        () ->
                                admin.createPartition(
                                                tablePath,
                                                newPartitionSpec(
                                                        Arrays.asList("region", "event_time"),
                                                        Arrays.asList("us", "2024-03-15")),
                                                false)
                                        .get())
                .cause()
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining("partition key 'event_day'");
    }

    @Test
    void testImplicitPartitionExpressionsSurviveSchemaReload() throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partition_schema_reload_table_1");
        Schema schema = createImplicitPartitionedTable(tablePath, true);
        TableInfo beforeAlter = admin.getTableInfo(tablePath).get();

        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "extra",
                                        DataTypes.STRING(),
                                        "extra column",
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();

        TableInfo afterAlter = admin.getTableInfo(tablePath).get();
        assertThat(afterAlter.getPartitionExpressions())
                .isEqualTo(beforeAlter.getPartitionExpressions());
        assertThat(afterAlter.getPartitionKeys()).containsExactly("region", "event_day");
        assertThat(afterAlter.getRowType().getFieldNames()).contains("extra");

        admin.createPartition(
                        tablePath,
                        newPartitionSpec(
                                Arrays.asList("region", "event_day"),
                                Arrays.asList("us", "20240315")),
                        false)
                .get();
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1);
        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        TimestampNtz eventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        InternalRow row =
                row(afterAlter.getRowType(), "us", 1, eventTime, 10, "value-10", "extra-value");

        upsertWriter.upsert(row).get();
        upsertWriter.flush();

        Lookuper lookuper = table.newLookup().createLookuper();
        assertThatRow(lookupRow(lookuper, row("us", 1, eventTime, 10)))
                .withSchema(afterAlter.getRowType())
                .isEqualTo(row);
        assertThat(afterAlter.getSchema().getColumns()).hasSize(schema.getColumns().size() + 1);
    }

    @Test
    void testImplicitPartitionedPrimaryKeyTableLookupMissingPartitionAndOverwrite()
            throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partitioned_pk_overwrite_table_1");
        Schema schema = createImplicitPartitionedTable(tablePath, true);
        admin.createPartition(
                        tablePath,
                        newPartitionSpec(
                                Arrays.asList("region", "event_day"),
                                Arrays.asList("us", "20240315")),
                        false)
                .get();
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1);
        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        TimestampNtz eventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        InternalRow oldRow = row(schema.getRowType(), "us", 1, eventTime, 10, "old-value");
        InternalRow newRow = row(schema.getRowType(), "us", 1, eventTime, 10, "new-value");

        upsertWriter.upsert(oldRow).get();
        upsertWriter.upsert(newRow).get();
        upsertWriter.flush();

        Lookuper lookuper = table.newLookup().createLookuper();
        assertThatRow(lookupRow(lookuper, row("us", 1, eventTime, 10)))
                .withSchema(schema.getRowType())
                .isEqualTo(newRow);

        TimestampNtz missingPartitionEventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 16, 10, 30));
        assertThat(lookupRow(lookuper, row("us", 1, missingPartitionEventTime, 10))).isNull();
    }

    @Test
    void testImplicitPartitionedPrimaryKeyTableDistinguishesSourceValuesInSamePartition()
            throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partitioned_pk_same_partition_table_1");
        Schema schema = createImplicitPartitionedTable(tablePath, true);
        admin.createPartition(
                        tablePath,
                        newPartitionSpec(
                                Arrays.asList("region", "event_day"),
                                Arrays.asList("us", "20240315")),
                        false)
                .get();
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath, 1);
        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        TimestampNtz firstEventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 10, 30));
        TimestampNtz secondEventTime =
                TimestampNtz.fromLocalDateTime(LocalDateTime.of(2024, 3, 15, 11, 30));
        InternalRow firstRow = row(schema.getRowType(), "us", 1, firstEventTime, 10, "value-10");
        InternalRow secondRow = row(schema.getRowType(), "us", 1, secondEventTime, 10, "value-11");

        upsertWriter.upsert(firstRow).get();
        upsertWriter.upsert(secondRow).get();
        upsertWriter.flush();

        Lookuper lookuper = table.newLookup().createLookuper();
        assertThatRow(lookupRow(lookuper, row("us", 1, firstEventTime, 10)))
                .withSchema(schema.getRowType())
                .isEqualTo(firstRow);
        assertThatRow(lookupRow(lookuper, row("us", 1, secondEventTime, 10)))
                .withSchema(schema.getRowType())
                .isEqualTo(secondRow);

        upsertWriter.delete(row("us", 1, firstEventTime, 10)).get();
        upsertWriter.flush();

        assertThat(lookupRow(lookuper, row("us", 1, firstEventTime, 10))).isNull();
        assertThatRow(lookupRow(lookuper, row("us", 1, secondEventTime, 10)))
                .withSchema(schema.getRowType())
                .isEqualTo(secondRow);
    }

    @Test
    void testImplicitPartitionedPrimaryKeyPrefixLookupRequiresTransformSource() throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_implicit_partitioned_pk_prefix_table_1");
        createImplicitPartitionedTable(tablePath, true);
        Table table = conn.getTable(tablePath);

        assertThatThrownBy(
                        () ->
                                table.newLookup()
                                        .lookupBy(Arrays.asList("region", "id"))
                                        .createLookuper())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must contain all partition fields [region, event_time]");
    }

    @Test
    void testPartitionedPrimaryKeyTable() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_static_partitioned_pk_table_1");
        Schema schema = createPartitionedTable(tablePath, true);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.isEmpty()).isTrue();

        // add three partitions.
        for (int i = 0; i < 3; i++) {
            admin.createPartition(tablePath, newPartitionSpec("c", "c" + i), false).get();
        }
        partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.size()).isEqualTo(3);

        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        int recordsPerPartition = 5;
        // now, put some data to the partitions
        Map<Long, List<InternalRow>> expectPutRows = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            String partitionName = partitionInfo.getPartitionName();
            long partitionId = partitionInfo.getPartitionId();
            for (int j = 0; j < recordsPerPartition; j++) {
                InternalRow row = row(j, "a" + j, partitionName);
                upsertWriter.upsert(row);
                expectPutRows.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(row);
            }
        }

        upsertWriter.flush();

        Lookuper lookuper = table.newLookup().createLookuper();
        // now, let's lookup the written data by look up.
        for (PartitionInfo partitionInfo : partitionInfos) {
            String partitionName = partitionInfo.getPartitionName();
            for (int j = 0; j < recordsPerPartition; j++) {
                InternalRow actualRow = row(j, "a" + j, partitionName);
                InternalRow lookupRow =
                        lookuper.lookup(row(j, partitionName)).get().getSingletonRow();
                assertThatRow(lookupRow).withSchema(schema.getRowType()).isEqualTo(actualRow);
            }
        }

        // lookup the non-exist partition
        assertThat(lookuper.lookup(row(0, "non-exist-partition")).get().getSingletonRow())
                .isEqualTo(null);

        // then, let's scan and check the cdc log
        verifyPartitionLogs(table, schema.getRowType(), expectPutRows);
    }

    @Test
    void testPartitionedLogTable() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_static_partitioned_log_table_1");
        Schema schema = createPartitionedTable(tablePath, false);

        List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.isEmpty()).isTrue();

        // add three partitions.
        for (int i = 0; i < 3; i++) {
            admin.createPartition(tablePath, newPartitionSpec("c", "c" + i), false).get();
        }
        partitionInfos = admin.listPartitionInfos(tablePath).get();
        assertThat(partitionInfos.size()).isEqualTo(3);

        Table table = conn.getTable(tablePath);
        AppendWriter appendWriter = table.newAppend().createWriter();
        int recordsPerPartition = 5;
        Map<Long, List<InternalRow>> expectPartitionAppendRows = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            String partitionName = partitionInfo.getPartitionName();
            long partitionId = partitionInfo.getPartitionId();
            for (int j = 0; j < recordsPerPartition; j++) {
                InternalRow row = row(j, "a" + j, partitionName);
                appendWriter.append(row);
                expectPartitionAppendRows
                        .computeIfAbsent(partitionId, k -> new ArrayList<>())
                        .add(row);
            }
        }
        appendWriter.flush();

        // then, let's verify the logs
        verifyPartitionLogs(table, schema.getRowType(), expectPartitionAppendRows);
    }

    @Test
    void testWriteToNonExistsPartitionWhenDisabledDynamicPartition() throws Exception {
        clientConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, false);
        createPartitionedTable(DATA1_TABLE_PATH_PK, true);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);

        // test write to not exist partition when enable dynamic create partition
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        GenericRow row = row(1, "a", "notExistPartition");
        assertThatThrownBy(() -> upsertWriter.upsert(row).get())
                .cause()
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessageContaining(
                        "Table partition '%s' does not exist.",
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK, "notExistPartition"));
    }

    @Test
    void testWriteToNonExistsPartitionWhenEnabledDynamicPartition() throws Exception {
        Schema schema = createPartitionedTable(DATA1_TABLE_PATH_PK, false);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);
        AppendWriter appendWriter = table.newAppend().createWriter();
        int partitionSize = 5;

        // first try to add records and wait partition created.
        Map<String, List<InternalRow>> expectPartitionNameAndAppendRows = new HashMap<>();
        for (int i = 0; i < partitionSize; i++) {
            String partitionName = String.valueOf(i);
            InternalRow row = row(i, "a" + i, partitionName);
            appendWriter.append(row);
            expectPartitionNameAndAppendRows
                    .computeIfAbsent(partitionName, k -> new ArrayList<>())
                    .add(row);
        }
        appendWriter.flush();

        List<PartitionInfo> partitionInfoList =
                waitValue(
                        () -> {
                            List<PartitionInfo> partitionInfos =
                                    admin.listPartitionInfos(DATA1_TABLE_PATH_PK).get();
                            if (partitionInfos.size() == partitionSize) {
                                return Optional.of(partitionInfos);
                            } else {
                                return Optional.empty();
                            }
                        },
                        Duration.ofMinutes(1),
                        "Fail to wait for the partition created.");
        Map<Long, List<InternalRow>> expectPartitionIdAndAppendRows = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfoList) {
            expectPartitionIdAndAppendRows.put(
                    partitionInfo.getPartitionId(),
                    expectPartitionNameAndAppendRows.get(partitionInfo.getPartitionName()));
        }

        // then, let's verify the logs
        verifyPartitionLogs(table, schema.getRowType(), expectPartitionIdAndAppendRows);
    }

    @Test
    void testCreatePartitionExceedMaxPartitionNumber() throws Exception {
        // test make partition number exceed max partition number 10 (set in
        // ClientToServerITCaseBase).
        createPartitionedTable(DATA1_TABLE_PATH_PK, true);
        Table table = conn.getTable(DATA1_TABLE_PATH_PK);

        // test write to not exist partition when enable dynamic create partition
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        for (int i = 0; i < 10; i++) {
            String partitionName = String.valueOf(i);
            InternalRow row = row(i, "a" + i, partitionName);
            upsertWriter.upsert(row).get();
        }

        // add one row will not throw TooManyPartitionsException immediately.
        upsertWriter.upsert(row(10, "a" + 10, "10"));

        // add another rows will throw TooManyPartitionsException final.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThatThrownBy(() -> upsertWriter.upsert(row(10, "a" + 10, "10")).get())
                                .rootCause()
                                .isInstanceOf(TooManyPartitionsException.class)
                                .hasMessageContaining(
                                        "Exceed the maximum number of partitions for table "
                                                + "test_db_1.test_pk_table_1, only allow 10 partitions."));
    }

    private List<PartitionInfo> waitForPartitionInfos(TablePath tablePath, int expectedSize)
            throws Exception {
        return waitValue(
                () -> {
                    List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
                    if (partitionInfos.size() == expectedSize) {
                        return Optional.of(partitionInfos);
                    } else {
                        return Optional.empty();
                    }
                },
                Duration.ofMinutes(1),
                "Fail to wait for the implicit partition created.");
    }

    private void assertPartitionReplicaDirectories(
            TablePath tablePath, long tableId, long partitionId, String partitionName) {
        String expectedPartitionDir = partitionName + "-p" + partitionId;
        retry(
                Duration.ofMinutes(1),
                () -> {
                    List<PhysicalTablePath> physicalTablePaths = new ArrayList<>();
                    List<Path> logTabletDirs = new ArrayList<>();
                    List<Path> kvTabletDirs = new ArrayList<>();
                    FLUSS_CLUSTER_EXTENSION
                            .getTabletServers()
                            .forEach(
                                    tabletServer ->
                                            tabletServer
                                                    .getReplicaManager()
                                                    .onlineReplicas()
                                                    .filter(
                                                            replica ->
                                                                    replica.getTableBucket()
                                                                                    .getTableId()
                                                                            == tableId)
                                                    .filter(
                                                            replica ->
                                                                    Long.valueOf(partitionId)
                                                                            .equals(
                                                                                    replica.getTableBucket()
                                                                                            .getPartitionId()))
                                                    .forEach(
                                                            replica -> {
                                                                physicalTablePaths.add(
                                                                        replica
                                                                                .getPhysicalTablePath());
                                                                logTabletDirs.add(
                                                                        replica.getLogTablet()
                                                                                .getLogDir()
                                                                                .toPath());
                                                                if (replica.getKvTablet() != null) {
                                                                    kvTabletDirs.add(
                                                                            replica.getKvTablet()
                                                                                    .getKvTabletDir()
                                                                                    .toPath());
                                                                }
                                                            }));
                    assertThat(physicalTablePaths)
                            .isNotEmpty()
                            .containsOnly(PhysicalTablePath.of(tablePath, partitionName));
                    assertThat(logTabletDirs)
                            .isNotEmpty()
                            .allSatisfy(
                                    logTabletDir ->
                                            assertTabletDir(
                                                    logTabletDir,
                                                    expectedPartitionDir,
                                                    tablePath,
                                                    tableId,
                                                    LOG_TABLET_DIR_PREFIX));
                    assertThat(kvTabletDirs)
                            .isNotEmpty()
                            .allSatisfy(
                                    kvTabletDir ->
                                            assertTabletDir(
                                                    kvTabletDir,
                                                    expectedPartitionDir,
                                                    tablePath,
                                                    tableId,
                                                    KV_TABLET_DIR_PREFIX));
                });
    }

    private void assertTabletDir(
            Path tabletDir,
            String expectedPartitionDir,
            TablePath tablePath,
            long tableId,
            String tabletDirPrefix) {
        assertThat(tabletDir).exists().isDirectory();
        assertThat(tabletDir.getFileName().toString()).startsWith(tabletDirPrefix);
        assertThat(tabletDir.getParent().getFileName().toString()).isEqualTo(expectedPartitionDir);
        assertThat(tabletDir.getParent().getParent().getFileName().toString())
                .isEqualTo(tablePath.getTableName() + "-" + tableId);
        assertThat(tabletDir.getParent().getParent().getParent().getFileName().toString())
                .isEqualTo(tablePath.getDatabaseName());
    }

    private Schema createPartitionedTable(TablePath tablePath, boolean isPrimaryTable)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .withComment("a is first column")
                        .column("b", DataTypes.STRING())
                        .withComment("b is second column")
                        .column("c", DataTypes.STRING())
                        .withComment("c is third column");

        if (isPrimaryTable) {
            schemaBuilder.primaryKey("a", "c");
        }

        Schema schema = schemaBuilder.build();

        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder().schema(schema).partitionedBy("c").build();
        createTable(tablePath, partitionTableDescriptor, false);
        return schema;
    }

    private Schema createImplicitPartitionedTable(TablePath tablePath, boolean isPrimaryTable)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("region", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .column("seq", DataTypes.INT())
                        .column("payload", DataTypes.STRING());

        if (isPrimaryTable) {
            schemaBuilder.primaryKey("region", "id", "event_time", "seq");
        }

        Schema schema = schemaBuilder.build();
        TableDescriptor.Builder tableDescriptorBuilder =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedByKeys(
                                PartitionKey.column("region"),
                                PartitionKey.expression(
                                        PartitionExpression.of(
                                                "event_day",
                                                DateTruncPartitionTransform.of(
                                                        "event_time", AutoPartitionTimeUnit.DAY))));
        tableDescriptorBuilder.property(
                ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, IMPLICIT_TEST_RETENTION);
        if (isPrimaryTable) {
            tableDescriptorBuilder.distributedBy(2, "id", "event_time");
        } else {
            tableDescriptorBuilder.distributedBy(1);
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        createTable(tablePath, tableDescriptor, false);
        return schema;
    }
}
