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

package com.alibaba.fluss.lakehouse.paimon.sink.committable;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittableTypeInfo;
import org.apache.paimon.table.sink.CommitMessageSerializer;

/**
 * Type information of {@link MultiTableCommittable}. Extending {@link
 * MultiTableCommittableTypeInfo} but will use {@link PaimonMultiTableCommittableSerializer} as the
 * serializer.
 */
public class PaimonMultiTableCommittableTypeInfo extends MultiTableCommittableTypeInfo {

    @Override
    public TypeSerializer<MultiTableCommittable> createSerializer(ExecutionConfig config) {
        return new SimpleVersionedSerializerTypeSerializerProxy<MultiTableCommittable>(
                () -> new PaimonMultiTableCommittableSerializer(new CommitMessageSerializer())) {

            @Override
            public MultiTableCommittable copy(MultiTableCommittable from) {
                return from;
            }

            @Override
            public MultiTableCommittable copy(
                    MultiTableCommittable from, MultiTableCommittable reuse) {
                return from;
            }
        };
    }
}
