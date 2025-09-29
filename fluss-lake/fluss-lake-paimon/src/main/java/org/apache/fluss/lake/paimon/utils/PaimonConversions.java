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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.lake.paimon.source.FlussRowAsPaimonRow;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Utils for conversion between Paimon and Fluss. */
public class PaimonConversions {

    public static RowKind toRowKind(ChangeType changeType) {
        switch (changeType) {
            case APPEND_ONLY:
            case INSERT:
                return RowKind.INSERT;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException("Unsupported change type: " + changeType);
        }
    }

    public static ChangeType toChangeType(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
                return ChangeType.INSERT;
            case UPDATE_BEFORE:
                return ChangeType.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return ChangeType.UPDATE_AFTER;
            case DELETE:
                return ChangeType.DELETE;
            default:
                throw new IllegalArgumentException("Unsupported rowKind: " + rowKind);
        }
    }

    public static Identifier toPaimon(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static BinaryRow toPaimonPartitionBinaryRow(
            List<String> partitionKeys, @Nullable String partitionName) {
        if (partitionName == null || partitionKeys.isEmpty()) {
            return BinaryRow.EMPTY_ROW;
        }

        //  Fluss's existing utility
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);

        BinaryRow partitionBinaryRow = new BinaryRow(partitionKeys.size());
        BinaryRowWriter writer = new BinaryRowWriter(partitionBinaryRow);

        List<String> partitionValues = resolvedPartitionSpec.getPartitionValues();
        for (int i = 0; i < partitionKeys.size(); i++) {
            // Todo Currently, partition column must be String datatype, so we can always use
            // `BinaryString.fromString` to convert to Paimon's data structure. Revisit here when
            // #489 is finished.
            writer.writeString(i, BinaryString.fromString(partitionValues.get(i)));
        }

        writer.complete();
        return partitionBinaryRow;
    }

    public static Object toPaimonLiteral(DataType dataType, Object flussLiteral) {
        RowType rowType = RowType.of(dataType);
        InternalRow flussRow = GenericRow.of(flussLiteral);
        FlussRowAsPaimonRow flussRowAsPaimonRow = new FlussRowAsPaimonRow(flussRow, rowType);
        return org.apache.paimon.data.InternalRow.createFieldGetter(dataType, 0)
                .getFieldOrNull(flussRowAsPaimonRow);
    }

    public static List<SchemaChange> toPaimonSchemaChanges(
            List<TableChange> tableChanges, Function<String, String> optionKeyTransformer) {
        List<SchemaChange> schemaChanges = new ArrayList<>(tableChanges.size());

        for (TableChange tableChange : tableChanges) {
            if (tableChange instanceof TableChange.SetOption) {
                TableChange.SetOption setOption = (TableChange.SetOption) tableChange;
                schemaChanges.add(
                        SchemaChange.setOption(
                                optionKeyTransformer.apply(setOption.getKey()),
                                setOption.getValue()));
            } else if (tableChange instanceof TableChange.ResetOption) {
                TableChange.ResetOption resetOption = (TableChange.ResetOption) tableChange;
                schemaChanges.add(
                        SchemaChange.removeOption(
                                optionKeyTransformer.apply(resetOption.getKey())));
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table change: " + tableChange.getClass());
            }
        }

        return schemaChanges;
    }
}
