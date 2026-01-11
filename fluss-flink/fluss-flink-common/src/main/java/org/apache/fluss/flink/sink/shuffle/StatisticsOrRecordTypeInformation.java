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

package org.apache.fluss.flink.sink.shuffle;

import org.apache.fluss.annotation.Internal;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

/**
 * The type information for StatisticsOrRecord.
 *
 * @param <InputT>
 */
@Internal
public class StatisticsOrRecordTypeInformation<InputT>
        extends TypeInformation<StatisticsOrRecord<InputT>> {

    private final TypeInformation<InputT> rowTypeInformation;
    private final DataStatisticsSerializer globalStatisticsSerializer;

    public StatisticsOrRecordTypeInformation(TypeInformation<InputT> rowTypeInformation) {
        this.rowTypeInformation = rowTypeInformation;
        this.globalStatisticsSerializer = new DataStatisticsSerializer();
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

    @Override
    @SuppressWarnings("unchecked")
    public Class<StatisticsOrRecord<InputT>> getTypeClass() {
        return (Class<StatisticsOrRecord<InputT>>) (Class<?>) StatisticsOrRecord.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<StatisticsOrRecord<InputT>> createSerializer(SerializerConfig config) {
        TypeSerializer<InputT> recordSerializer = rowTypeInformation.createSerializer(config);
        return new StatisticsOrRecordSerializer<>(globalStatisticsSerializer, recordSerializer);
    }

    @Override
    @Deprecated
    public TypeSerializer<StatisticsOrRecord<InputT>> createSerializer(ExecutionConfig config) {
        TypeSerializer<InputT> recordSerializer = rowTypeInformation.createSerializer(config);
        return new StatisticsOrRecordSerializer<>(globalStatisticsSerializer, recordSerializer);
    }

    @Override
    public String toString() {
        return "StatisticsOrRecord";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            StatisticsOrRecordTypeInformation<InputT> that =
                    (StatisticsOrRecordTypeInformation<InputT>) o;
            return that.rowTypeInformation.equals(rowTypeInformation)
                    && that.globalStatisticsSerializer.equals(globalStatisticsSerializer);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowTypeInformation, globalStatisticsSerializer);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof StatisticsOrRecordTypeInformation;
    }
}
