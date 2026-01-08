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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.concurrent.NotThreadSafe;

import java.time.Duration;

import static org.apache.fluss.utils.Preconditions.checkState;

/** AutoIncProcessor is used to process auto increment column. */
@NotThreadSafe
public class AutoIncManager {
    // No-op implementation that returns the input unchanged.
    public static final AutoIncUpdater NO_OP_UPDATER = rowValue -> rowValue;

    private final SchemaGetter schemaGetter;
    private final Cache<Integer, AutoIncUpdater> autoIncUpdaters;
    private final int autoIncColumnId;
    private final SequenceGenerator sequenceGenerator;

    public AutoIncManager(
            SchemaGetter schemaGetter,
            TablePath tablePath,
            Configuration properties,
            ZooKeeperClient zkClient) {
        this.autoIncUpdaters =
                Caffeine.newBuilder()
                        .maximumSize(5)
                        .expireAfterAccess(Duration.ofMinutes(5))
                        .build();
        this.schemaGetter = schemaGetter;
        int schemaId = schemaGetter.getLatestSchemaInfo().getSchemaId();
        Schema schema = schemaGetter.getSchema(schemaId);
        int[] autoIncColumnIds = schema.getAutoIncColumnIds();

        checkState(
                autoIncColumnIds.length <= 1,
                "Only support one auto increment column for a table, but got %d.",
                autoIncColumnIds.length);

        if (autoIncColumnIds.length == 1) {
            autoIncColumnId = autoIncColumnIds[0];
            sequenceGenerator =
                    new SegmentSequenceGenerator(
                            tablePath,
                            autoIncColumnId,
                            schema.getColumnName(autoIncColumnId),
                            new ZkSequenceIDCounter(
                                    zkClient.getCuratorClient(),
                                    ZkData.AutoIncrementColumnZNode.path(
                                            tablePath, autoIncColumnId)),
                            properties);
        } else {
            autoIncColumnId = -1;
            sequenceGenerator = null;
        }
    }

    // Supports removing or reordering columns; does NOT support adding an auto-increment column to
    // an existing table.
    public AutoIncUpdater getUpdaterForSchema(KvFormat kvFormat, int latestSchemaId) {
        return autoIncUpdaters.get(latestSchemaId, k -> createAutoIncUpdater(kvFormat, k));
    }

    private AutoIncUpdater createAutoIncUpdater(KvFormat kvFormat, int schemaId) {
        Schema schema = schemaGetter.getSchema(schemaId);
        int[] autoIncColumnIds = schema.getAutoIncColumnIds();
        if (autoIncColumnId == -1) {
            checkState(
                    autoIncColumnIds.length == 0,
                    "Cannot add auto-increment column after table creation.");
        } else {
            checkState(
                    autoIncColumnIds.length == 1 && autoIncColumnIds[0] == autoIncColumnId,
                    "Auto-increment column cannot be changed after table creation.");
        }
        if (autoIncColumnIds.length == 1) {
            return new PerSchemaAutoIncUpdater(
                    kvFormat, (short) schemaId, schema, autoIncColumnIds[0], sequenceGenerator);
        } else {
            return NO_OP_UPDATER;
        }
    }
}
