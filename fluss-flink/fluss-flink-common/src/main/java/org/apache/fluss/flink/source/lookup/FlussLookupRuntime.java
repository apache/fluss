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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookup;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.LookupType;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/** The shared Fluss client-side lookup runtime. */
final class FlussLookupRuntime implements LookupRuntime, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(FlussLookupRuntime.class);

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final TablePath tablePath;
    private final RowType flinkRowType;
    private final LookupNormalizer lookupNormalizer;
    private final boolean insertIfNotExists;

    private Connection connection;
    private Table table;
    private Lookuper lookuper;

    FlussLookupRuntime(
            Configuration flussConfig,
            TablePath tablePath,
            RowType flinkRowType,
            LookupNormalizer lookupNormalizer,
            boolean insertIfNotExists) {
        this.flussConfig = flussConfig;
        this.tablePath = tablePath;
        this.flinkRowType = flinkRowType;
        this.lookupNormalizer = lookupNormalizer;
        this.insertIfNotExists = insertIfNotExists;
    }

    @Override
    public void open() {
        LOG.info("Starting Fluss lookup runtime for table {}.", tablePath);
        connection = ConnectionFactory.createConnection(flussConfig);
        table = connection.getTable(tablePath);

        Lookup lookup = table.newLookup();
        if (lookupNormalizer.getLookupType() == LookupType.PREFIX_LOOKUP) {
            int[] lookupKeyIndexes = lookupNormalizer.getLookupKeyIndexes();
            RowType lookupKeyRowType = FlinkUtils.projectRowType(flinkRowType, lookupKeyIndexes);
            lookup = lookup.lookupBy(lookupKeyRowType.getFieldNames());
        } else if (insertIfNotExists) {
            lookup = lookup.enableInsertIfNotExists();
        }
        lookuper = lookup.createLookuper();
        LOG.info("Finished starting Fluss lookup runtime.");
    }

    @Override
    public CompletableFuture<LookupResult> lookup(RowData normalizedKeyRow) {
        return lookuper.lookup(new FlinkAsFlussRow(normalizedKeyRow));
    }

    TableInfo getTableInfo() {
        return table.getTableInfo();
    }

    Admin getAdmin() {
        return connection.getAdmin();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Fluss lookup runtime for table {}.", tablePath);
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
