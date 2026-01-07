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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.types.DataType;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;

/** AutoIncProcessor is used to process auto increment column. */
@NotThreadSafe
public class AutoIncProcessor {
    private static final AutoIncUpdater NO_OP_UPDATER = oldValue -> oldValue;

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
        this.schemaId = schemaGetter.getLatestSchemaInfo().getSchemaId();
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

        if (autoIncColumnIds.length > 0) {
            autoIncUpdater =
                    new DefaultAutoIncUpdater(
                            kvFormat,
                            (short) schemaId,
                            schema,
                            autoIncColumnIds[0],
                            sequenceGeneratorMap.get(autoIncColumnIds[0]));
        } else {
            autoIncUpdater = NO_OP_UPDATER;
        }
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
                int autoIncColumnId = autoIncColumnIds[0];
                if (sequenceGeneratorMap.containsKey(autoIncColumnId)) {
                    this.autoIncUpdater =
                            new DefaultAutoIncUpdater(
                                    kvFormat,
                                    (short) latestSchemaId,
                                    schemaGetter.getSchema(latestSchemaId),
                                    autoIncColumnId,
                                    sequenceGeneratorMap.get(autoIncColumnId));
                } else {
                    throw new IllegalStateException(
                            "Not supported add auto increment column for a table.");
                }
            } else {
                this.autoIncUpdater = NO_OP_UPDATER;
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
        return autoIncUpdater.updateAutoInc(oldValue);
    }

    public boolean isNoOpUpdate() {
        return autoIncUpdater.equals(NO_OP_UPDATER);
    }

    private interface AutoIncUpdater {
        BinaryValue updateAutoInc(BinaryValue oldValue);
    }

    /** Default auto increment column updater. */
    @NotThreadSafe
    private static class DefaultAutoIncUpdater implements AutoIncProcessor.AutoIncUpdater {
        private final InternalRow.FieldGetter[] flussFieldGetters;
        private final RowEncoder rowEncoder;
        private final DataType[] fieldDataTypes;
        private final int targetColumnIdx;
        private final SequenceGenerator idGenerator;
        private final short schemaId;

        public DefaultAutoIncUpdater(
                KvFormat kvFormat,
                short schemaId,
                Schema schema,
                int autoIncColumnId,
                SequenceGenerator sequenceGenerator) {
            DataType[] fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);

            // getter for the fields in row
            InternalRow.FieldGetter[] flussFieldGetters =
                    new InternalRow.FieldGetter[fieldDataTypes.length];
            for (int i = 0; i < fieldDataTypes.length; i++) {
                flussFieldGetters[i] = InternalRow.createFieldGetter(fieldDataTypes[i], i);
            }
            this.idGenerator = sequenceGenerator;
            this.schemaId = schemaId;
            this.targetColumnIdx = schema.getColumnIds().indexOf(autoIncColumnId);
            if (targetColumnIdx == -1) {
                throw new IllegalStateException(
                        String.format(
                                "Auto-increment column ID %d not found in schema columns: %s",
                                autoIncColumnId, schema.getColumnIds()));
            }
            this.rowEncoder = RowEncoder.create(kvFormat, fieldDataTypes);
            this.fieldDataTypes = fieldDataTypes;
            this.flussFieldGetters = flussFieldGetters;
        }

        public BinaryValue updateAutoInc(BinaryValue oldValue) {
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldDataTypes.length; i++) {
                if (targetColumnIdx == i) {
                    rowEncoder.encodeField(i, idGenerator.nextVal());
                } else {
                    // use the old row value
                    if (oldValue == null) {
                        rowEncoder.encodeField(i, null);
                    } else {
                        rowEncoder.encodeField(
                                i, flussFieldGetters[i].getFieldOrNull(oldValue.row));
                    }
                }
            }
            return new BinaryValue(schemaId, rowEncoder.finishRow());
        }
    }
}
