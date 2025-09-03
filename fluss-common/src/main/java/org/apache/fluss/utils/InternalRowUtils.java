/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/** Utils for {@link InternalRow} structures. */
public class InternalRowUtils {

    public static InternalRow copyInternalRow(InternalRow row, RowType rowType) {
        if (row instanceof IndexedRow) {
            return ((IndexedRow) row).copy();
        } else {
            GenericRow ret = new GenericRow(row.getFieldCount());

            InternalRow.FieldGetter[] fieldGetters = createFieldGetters(rowType.getChildren());
            for (int i = 0; i < row.getFieldCount(); ++i) {
                ret.setField(i, fieldGetters[i].getFieldOrNull(row));
            }

            return ret;
        }
    }

    public static Object copy(Object o, DataType type) {
        if (o instanceof BinaryString) {
            return ((BinaryString) o).copy();
        } else if (o instanceof InternalRow) {
            return copyInternalRow((InternalRow) o, (RowType) type);
        } else if (o instanceof Decimal) {
            return ((Decimal) o).copy();
        }
        return o;
    }

    public static long castToIntegral(Decimal dec) {
        BigDecimal bd = dec.toBigDecimal();
        // rounding down. This is consistent with float=>int,
        // and consistent with SQLServer, Spark.
        bd = bd.setScale(0, RoundingMode.DOWN);
        return bd.longValue();
    }

    public static InternalRow.FieldGetter[] createFieldGetters(List<DataType> fieldTypes) {
        InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldGetters[i] = createNullCheckingFieldGetter(fieldTypes.get(i), i);
        }
        return fieldGetters;
    }

    public static InternalRow.FieldGetter createNullCheckingFieldGetter(
            DataType dataType, int index) {
        InternalRow.FieldGetter getter = InternalRow.createFieldGetter(dataType, index);
        if (dataType.isNullable()) {
            return getter;
        } else {
            return row -> {
                if (row.isNullAt(index)) {
                    return null;
                }
                return getter.getFieldOrNull(row);
            };
        }
    }
}
