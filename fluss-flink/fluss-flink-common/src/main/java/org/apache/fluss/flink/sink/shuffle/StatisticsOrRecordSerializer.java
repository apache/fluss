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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * The serializer for {@link StatisticsOrRecord}.
 *
 * @param <InputT>
 */
@Internal
public class StatisticsOrRecordSerializer<InputT>
        extends TypeSerializer<StatisticsOrRecord<InputT>> {
    private final TypeSerializer<DataStatistics> statisticsSerializer;
    private final TypeSerializer<InputT> recordSerializer;

    StatisticsOrRecordSerializer(
            TypeSerializer<DataStatistics> statisticsSerializer,
            TypeSerializer<InputT> recordSerializer) {
        this.statisticsSerializer = statisticsSerializer;
        this.recordSerializer = recordSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @SuppressWarnings("ReferenceEquality")
    @Override
    public TypeSerializer<StatisticsOrRecord<InputT>> duplicate() {
        TypeSerializer<DataStatistics> duplicateStatisticsSerializer =
                statisticsSerializer.duplicate();
        TypeSerializer<InputT> duplicateRowDataSerializer = recordSerializer.duplicate();
        if ((statisticsSerializer != duplicateStatisticsSerializer)
                || (recordSerializer != duplicateRowDataSerializer)) {
            return new StatisticsOrRecordSerializer<>(
                    duplicateStatisticsSerializer, duplicateRowDataSerializer);
        } else {
            return this;
        }
    }

    @Override
    public StatisticsOrRecord<InputT> createInstance() {
        // arbitrarily always create RowData value instance
        return StatisticsOrRecord.fromRecord(recordSerializer.createInstance());
    }

    @Override
    public StatisticsOrRecord<InputT> copy(StatisticsOrRecord<InputT> from) {
        if (from.isRecord()) {
            return StatisticsOrRecord.fromRecord(recordSerializer.copy(from.record()));
        } else {
            return StatisticsOrRecord.fromStatistics(statisticsSerializer.copy(from.statistics()));
        }
    }

    @Override
    public StatisticsOrRecord<InputT> copy(
            StatisticsOrRecord<InputT> from, StatisticsOrRecord<InputT> reuse) {
        StatisticsOrRecord<InputT> to;
        if (from.isRecord()) {
            to = StatisticsOrRecord.reuseRecord(reuse, recordSerializer);
            InputT record = recordSerializer.copy(from.record(), to.record());
            to.setRecord(record);
        } else {
            to = StatisticsOrRecord.reuseStatistics(reuse, statisticsSerializer);
            DataStatistics statistics =
                    statisticsSerializer.copy(from.statistics(), to.statistics());
            to.setStatistics(statistics);
        }

        return to;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(StatisticsOrRecord<InputT> statisticsOrRecord, DataOutputView target)
            throws IOException {
        if (statisticsOrRecord.isRecord()) {
            target.writeBoolean(true);
            recordSerializer.serialize(statisticsOrRecord.record(), target);
        } else {
            target.writeBoolean(false);
            statisticsSerializer.serialize(statisticsOrRecord.statistics(), target);
        }
    }

    @Override
    public StatisticsOrRecord<InputT> deserialize(DataInputView source) throws IOException {
        boolean isRecord = source.readBoolean();
        if (isRecord) {
            return StatisticsOrRecord.fromRecord(recordSerializer.deserialize(source));
        } else {
            return StatisticsOrRecord.fromStatistics(statisticsSerializer.deserialize(source));
        }
    }

    @Override
    public StatisticsOrRecord<InputT> deserialize(
            StatisticsOrRecord<InputT> reuse, DataInputView source) throws IOException {
        StatisticsOrRecord<InputT> to;
        boolean isRecord = source.readBoolean();
        if (isRecord) {
            to = StatisticsOrRecord.reuseRecord(reuse, recordSerializer);
            InputT record = recordSerializer.deserialize(to.record(), source);
            to.setRecord(record);
        } else {
            to = StatisticsOrRecord.reuseStatistics(reuse, statisticsSerializer);
            DataStatistics statistics = statisticsSerializer.deserialize(to.statistics(), source);
            to.setStatistics(statistics);
        }

        return to;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        boolean hasRecord = source.readBoolean();
        target.writeBoolean(hasRecord);
        if (hasRecord) {
            recordSerializer.copy(source, target);
        } else {
            statisticsSerializer.copy(source, target);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StatisticsOrRecordSerializer)) {
            return false;
        }

        StatisticsOrRecordSerializer<InputT> other = (StatisticsOrRecordSerializer<InputT>) obj;
        return Objects.equals(statisticsSerializer, other.statisticsSerializer)
                && Objects.equals(recordSerializer, other.recordSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statisticsSerializer, recordSerializer);
    }

    @Override
    public TypeSerializerSnapshot<StatisticsOrRecord<InputT>> snapshotConfiguration() {
        return new StatisticsOrRecordSerializerSnapshot<InputT>(this);
    }

    /**
     * Snapshot implementation for the {@link StatisticsOrRecordSerializer}.
     *
     * @param <InputT>
     */
    public static class StatisticsOrRecordSerializerSnapshot<InputT>
            extends CompositeTypeSerializerSnapshot<
                    StatisticsOrRecord<InputT>, StatisticsOrRecordSerializer<InputT>> {
        private static final int CURRENT_VERSION = 1;

        /** Constructor for read instantiation. */
        @SuppressWarnings({"unused", "checkstyle:RedundantModifier"})
        public StatisticsOrRecordSerializerSnapshot() {}

        @SuppressWarnings("checkstyle:RedundantModifier")
        public StatisticsOrRecordSerializerSnapshot(
                StatisticsOrRecordSerializer<InputT> serializer) {
            super(serializer);
        }

        @SuppressWarnings("checkstyle:RedundantModifier")
        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return CURRENT_VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                StatisticsOrRecordSerializer outerSerializer) {
            return new TypeSerializer<?>[] {
                outerSerializer.statisticsSerializer, outerSerializer.recordSerializer
            };
        }

        @SuppressWarnings("unchecked")
        @Override
        protected StatisticsOrRecordSerializer<InputT> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            TypeSerializer<DataStatistics> statisticsSerializer =
                    (TypeSerializer<DataStatistics>) nestedSerializers[0];
            TypeSerializer<InputT> recordSerializer = (TypeSerializer<InputT>) nestedSerializers[1];
            return new StatisticsOrRecordSerializer<>(statisticsSerializer, recordSerializer);
        }
    }
}
