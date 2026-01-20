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

package org.apache.fluss.lake.paimon.testutils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TableRegistration;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** The utils for paimon testing. */
public class PaimonTestUtils {

    /** Adjust the table to legacy v1 table with system columns. */
    public static void adjustToLegacyV1Table(
            TablePath tablePath,
            long tableId,
            TableDescriptor tableDescriptor,
            Catalog paimonCatalog,
            ZooKeeperClient zkClient)
            throws Exception {
        // firstly, clear the lake storage version option
        Map<String, String> props = new HashMap<>(tableDescriptor.getProperties());
        props.remove(ConfigOptions.TABLE_DATALAKE_STORAGE_VERSION.key());
        props.keySet().removeIf(k -> k.startsWith("table.datalake.paimon."));
        TableDescriptor newTableDescriptor = tableDescriptor.withProperties(props);
        TableRegistration tableRegistration =
                TableRegistration.newTable(tableId, newTableDescriptor);
        zkClient.updateTable(tablePath, tableRegistration);

        // then, alter paimon table to add three system columns
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
