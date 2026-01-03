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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.zk.ZkSequenceIDCounter;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A updater to auto increment column . */
@NotThreadSafe
public class AutoIncProcessor {

    private final InternalRow.FieldGetter[] flussFieldGetters;

    private final RowEncoder rowEncoder;

    private final DataType[] fieldDataTypes;
    private final Map<Integer, SequenceGenerator> idGeneratorMap;
    private final short schemaId;

    public AutoIncProcessor(
            TablePath tablePath,
            Configuration properties,
            KvFormat kvFormat,
            short schemaId,
            Schema schema,
            ZooKeeperClient zkClient) {
        this.idGeneratorMap = new HashMap<>();
        int[] autoIncColumnIds = schema.getAutoIncColumnIds();
        SequenceGenerator[] sequenceGenerators = new SequenceGenerator[autoIncColumnIds.length];
        for (int i = 0; i < autoIncColumnIds.length; i++) {
            ZkSequenceIDCounter zkSequenceIDCounter =
                    new ZkSequenceIDCounter(
                            zkClient.getCuratorClient(),
                            ZkData.AutoIncrementColumnZNode.path(tablePath, autoIncColumnIds[i]));
            sequenceGenerators[i] =
                    new SegmentSequenceGenerator(
                            tablePath,
                            autoIncColumnIds[i],
                            schema.getColumnName(autoIncColumnIds[i]),
                            zkSequenceIDCounter,
                            properties);
        }

        List<Integer> columnIds = schema.getColumnIds();
        int[] targetColumnIdx = Arrays.stream(autoIncColumnIds).map(columnIds::indexOf).toArray();
        for (int i = 0; i < autoIncColumnIds.length; i++) {
            idGeneratorMap.put(targetColumnIdx[i], sequenceGenerators[i]);
        }
        this.fieldDataTypes = schema.getRowType().getChildren().toArray(new DataType[0]);

        // getter for the fields in row
        flussFieldGetters = new InternalRow.FieldGetter[fieldDataTypes.length];
        for (int i = 0; i < fieldDataTypes.length; i++) {
            flussFieldGetters[i] = InternalRow.createFieldGetter(fieldDataTypes[i], i);
        }
        this.rowEncoder = RowEncoder.create(kvFormat, fieldDataTypes);
        this.schemaId = schemaId;
    }

    @Nullable
    public BinaryValue processAutoInc(BinaryValue oldValue) {
        if (idGeneratorMap.isEmpty()) {
            return oldValue;
        } else {
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldDataTypes.length; i++) {
                if (idGeneratorMap.containsKey(i)) {
                    if (oldValue != null && oldValue.row.isNullAt(i)) {
                        rowEncoder.encodeField(i, idGeneratorMap.get(i).nextVal());
                    }
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
