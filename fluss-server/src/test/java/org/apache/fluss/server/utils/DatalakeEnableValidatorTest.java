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
 *
 */

package org.apache.fluss.server.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.lake.lakestorage.LakeSnapshotInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.server.utils.LakeTableValidator.checkSnapshotConsistency;
import static org.apache.fluss.server.utils.LakeTableValidator.checkTtlRisk;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LakeTableValidator}. */
class DatalakeEnableValidatorTest {

    @Test
    void testSnapshotConsistency() throws Exception {
        TablePath tablePath = TablePath.of("db", "t");
        long tableId = 1L;

        // both Fluss side and lake side have no snapshot - should pass
        checkSnapshotConsistency(tablePath, tableId, Optional.empty(), Optional.empty());

        // no snapshot but has lake table - should fail
        LakeTableSnapshot lakeTableSnapshot = new LakeTableSnapshot(1L, Collections.emptyMap());
        LakeTable lakeTable = new LakeTable(lakeTableSnapshot);
        assertThatThrownBy(
                        () ->
                                checkSnapshotConsistency(
                                        tablePath,
                                        tableId,
                                        Optional.of(lakeTable),
                                        Optional.empty()))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("lake table has no snapshot information");

        // table id mismatched - should fail
        LakeSnapshotInfo snapshotWithWrongTableId =
                new LakeSnapshotInfo(
                        1L,
                        System.currentTimeMillis(),
                        "/remote/data/lake/db/t-2/metadata/uuid.offsets");
        assertThatThrownBy(
                        () ->
                                checkSnapshotConsistency(
                                        tablePath,
                                        tableId,
                                        Optional.empty(),
                                        Optional.of(snapshotWithWrongTableId)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("belongs to a different Fluss table");

        // snapshot id mismatched - should fail
        LakeSnapshotInfo snapshotWithWrongSnapshotId =
                new LakeSnapshotInfo(
                        2L,
                        System.currentTimeMillis(),
                        "/remote/data/lake/db/t-1/metadata/uuid.offsets");
        assertThatThrownBy(
                        () ->
                                checkSnapshotConsistency(
                                        tablePath,
                                        tableId,
                                        Optional.of(lakeTable),
                                        Optional.of(snapshotWithWrongSnapshotId)))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("different latest snapshot ids");
    }

    @Test
    void testTtlRisk() {
        // within TTL - should pass
        TableInfo tableInfo = createTableInfoWithTtl(Duration.ofHours(1));
        LakeSnapshotInfo snapshotInfo = new LakeSnapshotInfo(1L, System.currentTimeMillis(), null);
        checkTtlRisk(tableInfo, Optional.of(snapshotInfo), SystemClock.getInstance());

        // older than TTL - should fail
        Duration ttl = Duration.ofMillis(10);
        TableInfo tableInfoWithShortTtl = createTableInfoWithTtl(ttl);
        long oldTimestamp = System.currentTimeMillis() - ttl.toMillis() * 10;
        LakeSnapshotInfo oldSnapshot = new LakeSnapshotInfo(1L, oldTimestamp, null);
        assertThatThrownBy(
                        () ->
                                checkTtlRisk(
                                        tableInfoWithShortTtl,
                                        Optional.of(oldSnapshot),
                                        SystemClock.getInstance()))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("older than table.log.ttl");
    }

    private static TableInfo createTableInfoWithTtl(Duration ttl) {
        Schema schema = Schema.newBuilder().column("a", DataTypes.INT()).build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_LOG_TTL, ttl)
                        .build();

        TablePath tablePath = TablePath.of("db", "t");
        return TableInfo.of(
                tablePath,
                1L,
                1,
                tableDescriptor,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}
