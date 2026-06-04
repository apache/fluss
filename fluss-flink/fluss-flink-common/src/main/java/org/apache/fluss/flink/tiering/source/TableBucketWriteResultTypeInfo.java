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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.flink.adapter.TypeInformationAdapter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.util.function.SerializableSupplier;

/**
 * A {@link TypeInformation} for {@link org.apache.fluss.client.tiering.TableBucketWriteResult} .
 */
public class TableBucketWriteResultTypeInfo<WriteResult>
        extends TypeInformationAdapter<
                org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult>> {

    private final SerializableSupplier<SimpleVersionedSerializer<WriteResult>>
            writeResultSerializerFactory;

    private TableBucketWriteResultTypeInfo(
            SerializableSupplier<SimpleVersionedSerializer<WriteResult>>
                    writeResultSerializerFactory) {
        this.writeResultSerializerFactory = writeResultSerializerFactory;
    }

    public static <WriteResult>
            TypeInformation<org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult>> of(
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
    public Class<org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult>>
            getTypeClass() {
        return (Class) org.apache.fluss.client.tiering.TableBucketWriteResult.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    protected TypeSerializer<org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult>>
            createSerializer(TypeSerializerCreator typeSerializerCreator) {
        // no copy, so that data from lake writer is directly going into lake committer while
        // chaining
        return new SimpleVersionedSerializerTypeSerializerProxy<
                org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult>>(
                () -> new TableBucketWriteResultSerializer<>(writeResultSerializerFactory.get())) {
            @Override
            public org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult> copy(
                    org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult> from) {
                return from;
            }

            @Override
            public org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult> copy(
                    org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult> from,
                    org.apache.fluss.client.tiering.TableBucketWriteResult<WriteResult> reuse) {
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
