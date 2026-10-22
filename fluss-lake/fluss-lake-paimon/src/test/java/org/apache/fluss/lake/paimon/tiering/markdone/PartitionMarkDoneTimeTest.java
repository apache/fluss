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

package org.apache.fluss.lake.paimon.tiering.markdone;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests Paimon partition time rules with a fixed clock. */
class PartitionMarkDoneTimeTest {

    @TempDir private File warehouse;

    @Test
    void testPaimonTimeRuleOnAutoPartitionedTable() throws Exception {
        ManualClock clock =
                new ManualClock(
                        LocalDateTime.of(2024, 6, 15, 12, 0)
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli());
        Map<String, String> options = new HashMap<>();
        options.put("partition.timestamp-pattern", "$day $hour");
        options.put("partition.timestamp-formatter", "yyyyMMdd HH");
        options.put("partition.time-interval", "1 h");
        String oppositeZone =
                ZoneId.systemDefault().getRules().getOffset(clock.instant()).getTotalSeconds() >= 0
                        ? "GMT-12:00"
                        : "GMT+14:00";
        try (PaimonPartitionMarkDone markDone = createMarkDone(options, oppositeZone, clock)) {
            assertThat(
                            markDone.markIdlePartitionsDone(trackedHours(), Collections.emptySet())
                                    .getTrackedPartitionLastUpdateTimes())
                    .containsOnlyKeys("20240615$15");
        }
    }

    private PartitionMarkDoneState trackedHours() {
        Map<String, Long> partitions = new HashMap<>();
        partitions.put("20240615$09", 0L);
        partitions.put("20240615$15", 0L);
        return new PartitionMarkDoneState(true, partitions);
    }

    private PaimonPartitionMarkDone createMarkDone(
            Map<String, String> timeOptions, String autoTimeZone, ManualClock clock)
            throws Exception {
        Map<String, String> options = new HashMap<>(timeOptions);
        options.put("partition.idle-time-to-done", "1 ms");
        TablePath path = TablePath.of("test", "mark_done");
        TableDescriptor.Builder descriptor =
                TableDescriptor.builder()
                        .schema(
                                org.apache.fluss.metadata.Schema.newBuilder()
                                        .column("day", DataTypes.STRING())
                                        .column("hour", DataTypes.STRING())
                                        .build())
                        .partitionedBy("day", "hour")
                        .distributedBy(1);
        descriptor
                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "day")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.DAY)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, autoTimeZone);
        TableInfo tableInfo =
                TableInfo.of(path, 0, 1, descriptor.build(), DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
        try (Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                Options.fromMap(
                                        Collections.singletonMap(
                                                "warehouse", warehouse.toString()))))) {
            catalog.createDatabase(path.getDatabaseName(), true);
            catalog.createTable(
                    toPaimon(path),
                    Schema.newBuilder()
                            .column("day", org.apache.paimon.types.DataTypes.STRING())
                            .column("hour", org.apache.paimon.types.DataTypes.STRING())
                            .partitionKeys(Arrays.asList("day", "hour"))
                            .options(options)
                            .build(),
                    false);
            return new PaimonPartitionMarkDone(
                    (FileStoreTable) catalog.getTable(toPaimon(path)), tableInfo, clock);
        }
    }
}
