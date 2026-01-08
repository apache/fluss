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
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkState;

/** AutoIncProcessor is used to process auto increment column. */
@NotThreadSafe
public class AutoIncProcessor {
    private static final AutoIncUpdater NO_OP_UPDATER = new AutoIncUpdater.NoOpUpdater();

    private final SchemaGetter schemaGetter;
    private final KvFormat kvFormat;
    private final Cache<Integer, AutoIncUpdater> autoIncUpdaters;
    private final Map<Integer, SequenceGenerator> sequenceGeneratorMap = new HashMap<>();

    public AutoIncProcessor(
            SchemaGetter schemaGetter,
            KvFormat kvFormat,
            TablePath tablePath,
            Configuration properties,
            ZooKeeperClient zkClient) {
        this.autoIncUpdaters =
                Caffeine.newBuilder()
                        .maximumSize(5)
                        .expireAfterAccess(Duration.ofMinutes(5))
                        .build();
        this.schemaGetter = schemaGetter;
        this.kvFormat = kvFormat;
        int schemaId = schemaGetter.getLatestSchemaInfo().getSchemaId();
        Schema schema = schemaGetter.getSchema(schemaId);
        int[] autoIncColumnIds = schema.getAutoIncColumnIds();

        if (autoIncColumnIds.length > 1) {
            throw new IllegalStateException(
                    "Only support one auto increment column for a table, but got "
                            + autoIncColumnIds.length);
        }

        for (int autoIncColumnId : autoIncColumnIds) {
            ZkSequenceIDCounter zkSequenceIDCounter =
                    new ZkSequenceIDCounter(
                            zkClient.getCuratorClient(),
                            ZkData.AutoIncrementColumnZNode.path(tablePath, autoIncColumnId));
            SequenceGenerator sequenceGenerator =
                    new SegmentSequenceGenerator(
                            tablePath,
                            autoIncColumnId,
                            schema.getColumnName(autoIncColumnId),
                            zkSequenceIDCounter,
                            properties);
            sequenceGeneratorMap.put(autoIncColumnId, sequenceGenerator);
        }
        autoIncUpdaters.put(schemaId, createAutoIncUpdater(schemaId));
    }

    // Supports removing or reordering columns; does NOT support adding an auto-increment column to
    // an existing table.
    public AutoIncUpdater configureSchema(int latestSchemaId) {
        return autoIncUpdaters.get(latestSchemaId, this::createAutoIncUpdater);
    }

    private AutoIncUpdater createAutoIncUpdater(int schemaId) {
        Schema schema = schemaGetter.getSchema(schemaId);
        int[] autoIncColumnIds = schema.getAutoIncColumnIds();
        checkState(
                autoIncColumnIds.length != sequenceGeneratorMap.size(),
                String.format(
                        "Auto-increment column count (%d) does not match sequence generator count (%d). Adding or dropping auto-increment columns is not supported.",
                        autoIncColumnIds.length, sequenceGeneratorMap.size()));

        if (autoIncColumnIds.length == 1) {
            int autoIncColumnId = autoIncColumnIds[0];
            return new AutoIncUpdater.DefaultAutoIncUpdater(
                    kvFormat,
                    (short) schemaId,
                    schema,
                    autoIncColumnId,
                    sequenceGeneratorMap.get(autoIncColumnId));
        } else {
            return NO_OP_UPDATER;
        }
    }
}
