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

package org.apache.fluss.lake.iceberg.testutils;

import org.apache.fluss.lake.iceberg.utils.IcebergPartitionSpecUtils;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Utils for iceberg testing. */
public class IcebergTestUtils {

    /**
     * Adjusts a clean Iceberg table into a legacy one by appending the three trailing Fluss system
     * columns ({@code __bucket}, {@code __offset}, {@code __timestamp}) and restoring the
     * Iceberg-specific legacy physical layout ({@code ASC(__offset)} sort order, plus an {@code
     * identity(__bucket)} partition for bucket-unaware tables). This simulates a table created
     * before FIP-27, when the lake table always carried these system columns, so tests can verify
     * legacy tables remain readable and writable.
     *
     * <p>The partition spec is preserved in a spec-aware way, matching how a real pre-FIP-27 table
     * looked:
     *
     * <ul>
     *   <li>a bucket-aware table keeps its {@code bucket(bucketKey)} transform on the user key; no
     *       {@code identity(__bucket)} is added (adding it would create a second, invalid bucket
     *       field that {@code IcebergPartitionSpecValidator} rejects);
     *   <li>a bucket-unaware table gains an {@code identity(__bucket)} partition, which is how such
     *       tables physically carried the bucket before FIP-27.
     * </ul>
     *
     * <p>Mirrors Paimon's {@code PaimonTestUtils.adjustToLegacyV1Table}, but additionally restores
     * the Iceberg-specific partition spec and sort order that a legacy table carries.
     */
    public static void adjustToLegacyV1Table(Catalog icebergCatalog, TableIdentifier tableId) {
        Table table = icebergCatalog.loadTable(tableId);

        // A bucket-aware table already partitions by bucket(bucketKey); a bucket-unaware table has
        // no bucket transform and needs identity(__bucket) added below.
        boolean bucketAware =
                table.spec().fields().stream()
                        .anyMatch(IcebergPartitionSpecUtils::isBucketTransform);

        // 1. Append the three trailing system columns. They must be REQUIRED to faithfully match a
        // real pre-FIP-27 legacy table (created via IcebergSchemaUtils, which adds them required).
        // addRequiredColumn is allowed here because the table is still empty right after creation.
        table.updateSchema()
                .allowIncompatibleChanges()
                .addRequiredColumn(BUCKET_COLUMN_NAME, Types.IntegerType.get())
                .addRequiredColumn(OFFSET_COLUMN_NAME, Types.LongType.get())
                .addRequiredColumn(TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone())
                .commit();

        // 2. For a bucket-unaware table, restore the legacy identity(__bucket) partition. A
        // bucket-aware table keeps its existing bucket(bucketKey) transform untouched.
        if (!bucketAware) {
            table.refresh();
            table.updateSpec().addField(BUCKET_COLUMN_NAME).commit();
        }

        // 3. Restore the legacy ASC(__offset) sort order.
        table.refresh();
        table.replaceSortOrder().asc(OFFSET_COLUMN_NAME).commit();
        table.refresh();
    }
}
