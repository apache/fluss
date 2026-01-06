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

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.types.DataTypeRoot;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.concurrent.NotThreadSafe;

import java.time.Duration;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Manages auto-increment logic for tables, providing schema-specific updaters that handle
 * auto-increment column assignment during row writes.
 */
@NotThreadSafe
public class AutoIncrementManager {
    // No-op implementation that returns the input unchanged.
    public static final AutoIncrementUpdater NO_OP_UPDATER = rowValue -> rowValue;

    private final SchemaGetter schemaGetter;
    private final Cache<Integer, AutoIncrementUpdater> autoIncrementUpdaterCache;
    private final int autoIncrementColumnId;
    private final SequenceGenerator sequenceGenerator;

    public AutoIncrementManager(
            SchemaGetter schemaGetter,
            TablePath tablePath,
            TableConfig tableConf,
            ZooKeeperClient zkClient) {
        this.autoIncrementUpdaterCache =
                Caffeine.newBuilder()
                        .maximumSize(5)
                        .expireAfterAccess(Duration.ofMinutes(5))
                        .build();
        this.schemaGetter = schemaGetter;
        int schemaId = schemaGetter.getLatestSchemaInfo().getSchemaId();
        Schema schema = schemaGetter.getSchema(schemaId);
        int[] autoIncrementColumnIds = schema.getAutoIncrementColumnIds();

        checkState(
                autoIncrementColumnIds.length <= 1,
                "Only support one auto increment column for a table, but got %d.",
                autoIncrementColumnIds.length);

        if (autoIncrementColumnIds.length == 1) {
            autoIncrementColumnId = autoIncrementColumnIds[0];
            boolean requiresIntOverflowCheck =
                    schema.getRowType()
                            .getField(schema.getColumnName(autoIncrementColumnId))
                            .getType()
                            .is(DataTypeRoot.INTEGER);
            sequenceGenerator =
                    new BoundedSegmentSequenceGenerator(
                            tablePath,
                            schema.getColumnName(autoIncrementColumnId),
                            new ZkSequenceIDCounter(
                                    zkClient.getCuratorClient(),
                                    ZkData.AutoIncrementColumnZNode.path(
                                            tablePath, autoIncrementColumnId)),
                            tableConf,
                            requiresIntOverflowCheck ? Integer.MAX_VALUE : Long.MAX_VALUE);
        } else {
            autoIncrementColumnId = -1;
            sequenceGenerator = null;
        }
    }

    // Supports removing or reordering columns; does NOT support adding an auto-increment column to
    // an existing table.
    public AutoIncrementUpdater getUpdaterForSchema(KvFormat kvFormat, int latestSchemaId) {
        return autoIncrementUpdaterCache.get(
                latestSchemaId, k -> createAutoIncrementUpdater(kvFormat, k));
    }

    private AutoIncrementUpdater createAutoIncrementUpdater(KvFormat kvFormat, int schemaId) {
        Schema schema = schemaGetter.getSchema(schemaId);
        int[] autoIncrementColumnIds = schema.getAutoIncrementColumnIds();
        if (autoIncrementColumnId == -1) {
            checkState(
                    autoIncrementColumnIds.length == 0,
                    "Cannot add auto-increment column after table creation.");
        } else {
            checkState(
                    autoIncrementColumnIds.length == 1
                            && autoIncrementColumnIds[0] == autoIncrementColumnId,
                    "Auto-increment column cannot be changed after table creation.");
        }
        if (autoIncrementColumnIds.length == 1) {
            return new PerSchemaAutoIncrementUpdater(
                    kvFormat,
                    (short) schemaId,
                    schema,
                    autoIncrementColumnIds[0],
                    sequenceGenerator);
        } else {
            return NO_OP_UPDATER;
        }
    }
}
