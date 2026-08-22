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

import org.apache.iceberg.Schema;
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
     * columns ({@code __bucket}, {@code __offset}, {@code __timestamp}), then restoring the legacy
     * physical layout of an {@code identity(__bucket)} partition spec and an {@code ASC(__offset)}
     * sort order. This simulates a table created before FIP-27, when the lake table always carried
     * these system columns, so tests can verify legacy tables remain readable and writable.
     *
     * <p>Mirrors Paimon's {@code PaimonTestUtils.adjustToLegacyV1Table}, but additionally restores
     * the Iceberg-specific partition spec and sort order that a legacy table carries.
     */
    public static void adjustToLegacyV1Table(Catalog icebergCatalog, TableIdentifier tableId) {
        Table table = icebergCatalog.loadTable(tableId);

        // 1. Append the three trailing system columns. They must be REQUIRED to faithfully match a
        // real pre-FIP-27 legacy table (created via IcebergSchemaUtils, which adds them required).
        // addRequiredColumn is allowed here because the table is still empty right after creation.
        table.updateSchema()
                .allowIncompatibleChanges()
                .addRequiredColumn(BUCKET_COLUMN_NAME, Types.IntegerType.get())
                .addRequiredColumn(OFFSET_COLUMN_NAME, Types.LongType.get())
                .addRequiredColumn(TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone())
                .commit();

        // 2. Restore the legacy identity(__bucket) partition spec.
        table.refresh();
        table.updateSpec().addField(BUCKET_COLUMN_NAME).commit();

        // 3. Restore the legacy ASC(__offset) sort order.
        table.refresh();
        Schema schema = table.schema();
        table.replaceSortOrder().asc(schema.findField(OFFSET_COLUMN_NAME).name()).commit();
        table.refresh();
    }
}
