/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.sink.serializer;

import com.alibaba.fluss.types.RowType;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Contextual information that can be used during initialization of {@link
 * FlussSerializationSchema}.
 */
public class SerializerInitContextImpl implements FlussSerializationSchema.InitializationContext {

    private final RowType rowSchema;

    public SerializerInitContextImpl(RowType rowSchema) {
        this.rowSchema = checkNotNull(rowSchema, "rowSchema");
    }

    @Override
    public RowType getRowSchema() {
        return rowSchema;
    }
}
