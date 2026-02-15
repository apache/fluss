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

import org.apache.fluss.annotation.PublicStable;

import java.util.Collections;
import java.util.List;

/**
 * Data type of a timestamp with nanosecond precision WITHOUT time zone. Iceberg V3 type that stores
 * timestamps as 64-bit nanoseconds from epoch. Values range from {@code 0000-01-01
 * 00:00:00.000000000} to {@code 9999-12-31 23:59:59.999999999}.
 *
 * @since 0.7
 */
@PublicStable
public final class TimestampNanoType extends DataType {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "TIMESTAMP_NS";

    public TimestampNanoType(boolean isNullable) {
        super(isNullable, DataTypeRoot.TIMESTAMP_NANO_WITHOUT_TIME_ZONE);
    }

    public TimestampNanoType() {
        this(true);
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new TimestampNanoType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }

    @Override
    public String asSummaryString() {
        return asSerializableString();
    }

    @Override
    public List<DataType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
