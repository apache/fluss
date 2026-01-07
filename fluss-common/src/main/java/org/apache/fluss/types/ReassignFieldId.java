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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Visitor that recursively reassigns field IDs in nested data types using a provided counter. */
public class ReassignFieldId extends DataTypeDefaultVisitor<DataType> {

    private final AtomicInteger highestFieldId;

    public ReassignFieldId(AtomicInteger highestFieldId) {
        this.highestFieldId = highestFieldId;
    }

    public static DataType reassign(DataType input, AtomicInteger highestFieldId) {
        return input.accept(new ReassignFieldId(highestFieldId));
    }

    @Override
    public DataType visit(ArrayType arrayType) {
        return new ArrayType(arrayType.isNullable(), arrayType.getElementType().accept(this));
    }

    @Override
    public DataType visit(MapType mapType) {
        return new MapType(
                mapType.isNullable(),
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this));
    }

    @Override
    public DataType visit(RowType rowType) {
        List<DataField> originalDataFields = rowType.getFields();

        List<DataField> dataFields = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataField dataField = originalDataFields.get(i);
            int id = highestFieldId.incrementAndGet();
            DataType dataType = dataField.getType().accept(this);
            dataFields.add(
                    new DataField(
                            dataField.getName(),
                            dataType,
                            dataField.getDescription().orElse(null),
                            id));
        }

        return new RowType(rowType.isNullable(), dataFields);
    }

    @Override
    protected DataType defaultMethod(DataType dataType) {
        return dataType;
    }
}
