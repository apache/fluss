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
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;

/** AutoIncProcessor is used to process auto increment column for each schema ID. */
@NotThreadSafe
public class AutoIncProcessor {
    private final SchemaGetter schemaGetter;
    private final KvFormat kvFormat;
    private AutoIncUpdater autoIncUpdater;
    private int schemaId;
    private final Map<Integer, SequenceGenerator> sequenceGeneratorMap = new HashMap<>();

    public AutoIncProcessor(
            SchemaGetter schemaGetter,
            KvFormat kvFormat,
            TablePath tablePath,
            Configuration properties,
            ZooKeeperClient zkClient) {
        this.schemaGetter = schemaGetter;
        this.kvFormat = kvFormat;
        schemaId = schemaGetter.getLatestSchemaInfo().getSchemaId();
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
                            ZkData.AutoIncrementColumnZNode.path(tablePath, autoIncColumnIds[0]));
            SequenceGenerator sequenceGenerator =
                    new SegmentSequenceGenerator(
                            tablePath,
                            autoIncColumnIds[0],
                            schema.getColumnName(autoIncColumnIds[0]),
                            zkSequenceIDCounter,
                            properties);
            sequenceGeneratorMap.put(autoIncColumnId, sequenceGenerator);
        }

        autoIncUpdater =
                new AutoIncUpdater(
                        kvFormat,
                        (short) schemaId,
                        schema,
                        autoIncColumnIds[0],
                        sequenceGeneratorMap.get(autoIncColumnIds[0]));
    }

    // Supports removing or reordering columns; does NOT support adding an auto-increment column to
    // an existing table.
    public void configureSchema(int latestSchemaId) {
        if (latestSchemaId != this.schemaId) {
            Schema schema = schemaGetter.getSchema(latestSchemaId);
            int[] autoIncColumnIds = schema.getAutoIncColumnIds();
            if (autoIncColumnIds.length > 1) {
                throw new IllegalStateException(
                        "Only support one auto increment column for a table, but got "
                                + autoIncColumnIds.length);
            } else if (autoIncColumnIds.length == 1) {
                if (sequenceGeneratorMap.containsKey(autoIncColumnIds[0])) {
                    this.autoIncUpdater =
                            new AutoIncUpdater(
                                    kvFormat,
                                    (short) latestSchemaId,
                                    schemaGetter.getSchema(latestSchemaId),
                                    autoIncColumnIds[0],
                                    sequenceGeneratorMap.get(autoIncColumnIds[0]));
                } else {
                    throw new IllegalStateException(
                            "Not supported add auto increment column for a table.");
                }
            } else {
                this.autoIncUpdater = null;
                this.sequenceGeneratorMap.clear();
            }
            this.schemaId = latestSchemaId;
        }
    }

    /**
     * Process auto increment for a given old value.
     *
     * @param oldValue the old value
     * @return the new value with auto incremented column value
     */
    public BinaryValue processAutoInc(BinaryValue oldValue) {
        if (autoIncUpdater != null) {
            return autoIncUpdater.updateAutoInc(oldValue);
        } else {
            return oldValue;
        }
    }
}
