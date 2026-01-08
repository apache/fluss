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

package org.apache.fluss.types;

import java.util.List;

/** Visitor that checks whether two data types are equal. */
public class DataTypeEqualsWithFieldId extends DataTypeDefaultVisitor<Boolean> {
    private final DataType original;

    private DataTypeEqualsWithFieldId(DataType original) {
        this.original = original;
    }

    public static boolean equals(DataType original, DataType that) {
        return that.accept(new DataTypeEqualsWithFieldId(original));
    }

    @Override
    public Boolean visit(RowType that) {
        if (!original.equals(that)) {
            return false;
        }

        // compare field ids.
        List<DataField> originalFields = ((RowType) original).getFields();
        List<DataField> thatFields = that.getFields();
        for (int i = 0; i < that.getFieldCount(); i++) {
            DataField originalField = originalFields.get(i);
            DataField thatField = thatFields.get(i);
            if (originalField.getFieldId() != thatField.getFieldId()
                    || !equals(originalField.getType(), thatField.getType())) {
                return false;
            }
        }

        return true;
    }

    @Override
    protected Boolean defaultMethod(DataType that) {
        return original.equals(that);
    }
}
