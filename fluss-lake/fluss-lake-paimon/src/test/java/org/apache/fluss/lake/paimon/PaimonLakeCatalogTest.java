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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link PaimonLakeCatalog}. */
class PaimonLakeCatalogTest {

    @TempDir private File tempWarehouseDir;

    private PaimonLakeCatalog flussPaimonCatalog;

    @BeforeEach
    public void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        flussPaimonCatalog = new PaimonLakeCatalog(configuration);
    }

    @Test
    void testAlterTableProperties() throws Exception {
        String database = "test_alter_table_properties_db";
        String tableName = "test_alter_table_properties_table";
        TablePath tablePath = TablePath.of(database, tableName);
        Identifier identifier = Identifier.create(database, tableName);
        createTable(database, tableName);
        Table table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);

        // value should be null for key
        assertThat(table.options().get("fluss.key")).isEqualTo(null);

        // set the value for key
        flussPaimonCatalog.alterTable(
                tablePath, Collections.singletonList(TableChange.set("key", "value")));

        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        // we have set the value for key
        assertThat(table.options().get("fluss.key")).isEqualTo("value");

        // reset the value for key
        flussPaimonCatalog.alterTable(
                tablePath, Collections.singletonList(TableChange.reset("key")));

        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        // we have reset the value for key
        assertThat(table.options().get("fluss.key")).isEqualTo(null);

        // test for bucket.num
        assertThat(table.options().get("fluss.bucket.num")).isEqualTo(null);
        flussPaimonCatalog.alterTable(
                tablePath, Collections.singletonList(TableChange.bucketNum(3)));
        table = flussPaimonCatalog.getPaimonCatalog().getTable(identifier);
        assertThat(table.options().get("fluss.bucket.num")).isEqualTo("3");
    }

    private void createTable(String database, String tableName) {
        Schema flussSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("address", DataTypes.STRING())
                        .build();

        TableDescriptor td =
                TableDescriptor.builder()
                        .schema(flussSchema)
                        .distributedBy(3) // no bucket key
                        .build();

        TablePath tablePath = TablePath.of(database, tableName);

        flussPaimonCatalog.createTable(tablePath, td);
    }
}
