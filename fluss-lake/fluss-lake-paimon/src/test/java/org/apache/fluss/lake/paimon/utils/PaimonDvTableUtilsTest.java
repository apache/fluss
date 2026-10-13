/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonDvTableUtils.findLatestSnapshotExactlyHoldingL0Files;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactCommitter;
import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.writeAndCommitData;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonDvTableUtils}. */
class PaimonDvTableUtilsTest {

    private static final String DEFAULT_DB = "test_db";

    @TempDir private Path tempDir;
    @TempDir private File compactionTempDir;
    private Catalog paimonCatalog;

    @BeforeEach
    void setUp() throws Exception {
        // Create a local filesystem catalog for testing
        Map<String, String> options = new HashMap<>();
        options.put("warehouse", tempDir.resolve("warehouse").toString());
        options.put("metastore", "filesystem");
        paimonCatalog =
                CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(options)));
        paimonCatalog.createDatabase(DEFAULT_DB, false);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (paimonCatalog != null) {
            paimonCatalog.close();
        }
    }

    @Test
    void testFindLatestSnapshotExactlyHoldingL0Files() throws Exception {
        int bucket0 = 0;
        int bucket1 = 1;

        // Given: create a DV table with 2 buckets
        Identifier tableIdentifier = Identifier.create(DEFAULT_DB, "test_find_base_snapshot");
        FileStoreTable fileStoreTable = createDvTable(tableIdentifier, 2);

        PaimonTestUtils.CompactHelper compactHelper =
                new PaimonTestUtils.CompactHelper(fileStoreTable, compactionTempDir);

        // Step 1: APPEND snapshot 1 - write data to bucket0 and bucket1
        // Snapshot 1 state:
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L0: [rows 0-2]              │
        //   │ bucket1 │ L0: [rows 0-2]              │
        //   └─────────┴─────────────────────────────┘
        Map<Integer, List<GenericRow>> appendRows = new HashMap<>();
        appendRows.put(bucket0, generateRows(0, 3));
        appendRows.put(bucket1, generateRows(0, 3));
        long snapshot1 = writeAndCommitData(fileStoreTable, appendRows);

        // Step 2: COMPACT snapshot 2 - compact bucket0
        // Snapshot 2 state (after compacting bucket0):
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [compacted from s1 L0]  │ ← L0 flushed to L1
        //   │ bucket1 │ L0: [rows 0-2]              │ ← unchanged
        //   └─────────┴─────────────────────────────┘
        CompactCommitter compactCommitter1Bucket0 = compactHelper.compactBucket(bucket0);
        compactCommitter1Bucket0.commit();

        // Prepare compact for bucket1 (not committed yet)
        CompactCommitter compactCommitter1Bucket1 = compactHelper.compactBucket(bucket1);

        // Verify: snapshot2 flushed bucket0's L0 files, which were added in snapshot1
        // So snapshot1 is the latest snapshot holding those L0 files
        long snapshot2 = fileStoreTable.snapshotManager().latestSnapshot().id();
        Snapshot snapshot2Obj = fileStoreTable.snapshotManager().snapshot(snapshot2);
        Snapshot latestHoldL0Snapshot =
                findLatestSnapshotExactlyHoldingL0Files(fileStoreTable, snapshot2Obj);
        assertThat(latestHoldL0Snapshot).isNotNull();
        assertThat(latestHoldL0Snapshot.id()).isEqualTo(snapshot1);

        // Step 3: APPEND snapshot 3 - write more data to bucket0
        // Snapshot 3 state:
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [from s2]               │
        //   │         │ L0: [rows 3-5] ← new        │
        //   │ bucket1 │ L0: [rows 0-2] ← unchanged  │
        //   └─────────┴─────────────────────────────┘
        long snapshot3 =
                writeAndCommitData(
                        fileStoreTable, Collections.singletonMap(bucket0, generateRows(3, 6)));
        fileStoreTable.snapshotManager().snapshot(snapshot3);

        // Step 4: COMPACT snapshot 4 - compact bucket0 again (flushes snapshot3's L0)
        // Snapshot 4 state (after compacting bucket0):
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [merged s2 L1 + s3 L0]  │ ← L0 flushed to L1
        //   │ bucket1 │ L0: [rows 0-2]              │ ← unchanged
        //   └─────────┴─────────────────────────────┘
        CompactCommitter compactCommitter3Bucket0 = compactHelper.compactBucket(bucket0);
        compactCommitter3Bucket0.commit();

        // Verify: snapshot4 flushed bucket0's L0 files, which were added in snapshot3
        // So snapshot3 is the latest snapshot holding those L0 files
        long snapshot4 = fileStoreTable.snapshotManager().latestSnapshot().id();
        Snapshot snapshot4Obj = fileStoreTable.snapshotManager().snapshot(snapshot4);
        latestHoldL0Snapshot =
                findLatestSnapshotExactlyHoldingL0Files(fileStoreTable, snapshot4Obj);
        assertThat(latestHoldL0Snapshot).isNotNull();
        assertThat(latestHoldL0Snapshot.id()).isEqualTo(snapshot3);

        // Step 5: APPEND snapshot 5 - write more data to bucket1
        // Snapshot 5 state:
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [from s4]               │ ← unchanged
        //   │ bucket1 │ L0: [rows 0-2 from s1]      │ ← from snapshot1
        //   │         │ L0: [rows 3-5] ← new        │ ← added in snapshot5
        //   └─────────┴─────────────────────────────┘
        long snapshot5 =
                writeAndCommitData(
                        fileStoreTable, Collections.singletonMap(bucket1, generateRows(3, 6)));

        // Step 6: COMPACT snapshot 6 - commit compactCommitter1_b1 (compact bucket1)
        // Note: compactCommitter1_b1 was prepared based on snapshot1, so it will only compact
        // bucket1's L0 files from snapshot1 (rows 0-2), not the new L0 files added in snapshot5.
        // Snapshot 6 state (after compacting bucket1):
        //   ┌─────────┬─────────────────────────────┐
        //   │ Bucket  │ Files                       │
        //   ├─────────┼─────────────────────────────┤
        //   │ bucket0 │ L1: [from s4]               │ ← unchanged
        //   │ bucket1 │ L1: [compacted from s1 L0]  │ ← s1's L0 (rows 0-2) flushed to L1
        //   │         │ L0: [rows 3-5 from s5]      │ ← s5's L0 still remains
        //   └─────────┴─────────────────────────────┘
        compactCommitter1Bucket1.commit();
        long snapshot6 = fileStoreTable.snapshotManager().latestSnapshot().id();
        Snapshot snapshot6Obj = fileStoreTable.snapshotManager().snapshot(snapshot6);
        latestHoldL0Snapshot =
                findLatestSnapshotExactlyHoldingL0Files(fileStoreTable, snapshot6Obj);
        // Verify: snapshot6 flushed bucket1's L0 files from snapshot1 (rows 0-2).
        // These L0 files were added in snapshot1, and they still exist(with all L0 files match) in
        // snapshot4, (snapshot5 doesn't exactly match flushed L0 files, with extra L0 files).
        // So snapshot4 is the latest snapshot exactly holding those L0 files.
        assertThat(latestHoldL0Snapshot).isNotNull();
        assertThat(latestHoldL0Snapshot.id()).isEqualTo(snapshot4);

        // test snapshot expiration case
        fileStoreTable
                .newExpireSnapshots()
                .config(ExpireConfig.builder().snapshotRetainMax(1).build())
                .expire();
        latestHoldL0Snapshot =
                findLatestSnapshotExactlyHoldingL0Files(fileStoreTable, snapshot6Obj);
        // the snapshot4 is expired, should get null now
        assertThat(latestHoldL0Snapshot).isNull();
    }

    /**
     * Create a DV table using Paimon catalog directly.
     *
     * @param tableIdentifier the table identifier
     * @param numBuckets the number of buckets
     * @return the FileStoreTable instance
     */
    private FileStoreTable createDvTable(Identifier tableIdentifier, int numBuckets)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .option("bucket", String.valueOf(numBuckets))
                        .option("deletion-vectors.enabled", "true")
                        .build();
        paimonCatalog.createTable(tableIdentifier, schema, false);
        return (FileStoreTable) paimonCatalog.getTable(tableIdentifier);
    }

    /**
     * Generate rows for testing with a simple schema (c1: INT, c2: STRING).
     *
     * @param from the starting value (inclusive)
     * @param to the ending value (exclusive)
     * @return a list of GenericRow instances
     */
    public static List<GenericRow> generateRows(int from, int to) {
        List<GenericRow> rows = new java.util.ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("value" + i)));
        }
        return rows;
    }
}
