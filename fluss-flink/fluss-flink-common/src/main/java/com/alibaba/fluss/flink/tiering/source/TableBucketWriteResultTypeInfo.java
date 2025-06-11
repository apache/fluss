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

package com.alibaba.fluss.flink.tiering.source;

import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.util.function.SerializableSupplier;

/** A {@link TypeInformation} for {@link TableBucketWriteResult} . */
public class TableBucketWriteResultTypeInfo<WriteResult>
        extends TypeInformation<TableBucketWriteResult<WriteResult>> {

    private final SerializableSupplier<SimpleVersionedSerializer<WriteResult>>
            writeResultSerializerFactory;

    private TableBucketWriteResultTypeInfo(
            SerializableSupplier<SimpleVersionedSerializer<WriteResult>>
                    writeResultSerializerFactory) {
        this.writeResultSerializerFactory = writeResultSerializerFactory;
    }

    public static <WriteResult> TypeInformation<TableBucketWriteResult<WriteResult>> of(
            SerializableSupplier<SimpleVersionedSerializer<WriteResult>>
                    writeResultSerializerFactory) {
        return new TableBucketWriteResultTypeInfo<>(writeResultSerializerFactory);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Class<TableBucketWriteResult<WriteResult>> getTypeClass() {
        return (Class) TableBucketWriteResult.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<TableBucketWriteResult<WriteResult>> createSerializer(
            ExecutionConfig executionConfig) {
        // no copy, so that data from lake writer is directly going into lake committer while
        // chaining
        return new SimpleVersionedSerializerTypeSerializerProxy<
                TableBucketWriteResult<WriteResult>>(
                () -> new TableBucketWriteResultSerializer<>(writeResultSerializerFactory.get())) {
            @Override
            public TableBucketWriteResult<WriteResult> copy(
                    TableBucketWriteResult<WriteResult> from) {
                return from;
            }

            @Override
            public TableBucketWriteResult<WriteResult> copy(
                    TableBucketWriteResult<WriteResult> from,
                    TableBucketWriteResult<WriteResult> reuse) {
                return from;
            }
        };
    }

    @Override
    public String toString() {
        return "TableBucketWriteResultTypeInfo";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TableBucketWriteResultTypeInfo;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof TableBucketWriteResultTypeInfo;
    }
}
