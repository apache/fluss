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

package org.apache.fluss.lake.paimon.testutils;

import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;

import java.util.Arrays;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** The utils for paimon testing. */
public class PaimonTestUtils {

    /**
     * Adjusts the Paimon table to a legacy table by appending the three trailing Fluss system
     * columns ({@code __bucket}, {@code __offset}, {@code __timestamp}). This simulates a table
     * created before FIP-27, when the lake table always carried these system columns, so tests can
     * verify that legacy tables remain readable and writable.
     */
    public static void adjustToLegacyV1Table(TablePath tablePath, Catalog paimonCatalog)
            throws Exception {
        paimonCatalog.alterTable(
                toPaimon(tablePath),
                Arrays.asList(
                        SchemaChange.addColumn(BUCKET_COLUMN_NAME, DataTypes.INT()),
                        SchemaChange.addColumn(OFFSET_COLUMN_NAME, DataTypes.BIGINT()),
                        SchemaChange.addColumn(
                                TIMESTAMP_COLUMN_NAME, DataTypes.TIMESTAMP_LTZ_MILLIS())),
                false);
    }
}
