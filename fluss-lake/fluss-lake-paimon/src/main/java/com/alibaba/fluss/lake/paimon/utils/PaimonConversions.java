/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.utils;

import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

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

    public static Identifier toPaimon(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static BinaryRow toPaimonBinaryRow(@Nullable String value) {
        if (value == null) {
            return BinaryRow.EMPTY_ROW;
        }
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeString(0, BinaryString.fromString(value));
        writer.complete();
        return binaryRow;
    }
}
