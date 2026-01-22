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

package org.apache.fluss.flink.source.deserializer;

import org.apache.fluss.flink.utils.ChangelogRowConverter;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkRowType;

/**
 * A deserialization schema that converts {@link LogRecord} objects to Flink's {@link RowData}
 * format with additional changelog metadata columns.
 */
public class ChangelogDeserializationSchema implements FlussDeserializationSchema<RowData> {

    /**
     * Converter responsible for transforming Fluss row data into Flink's {@link RowData} format
     * with metadata columns. Initialized during {@link #open(InitializationContext)}.
     */
    private transient ChangelogRowConverter converter;

    /** Creates a new ChangelogDeserializationSchema. */
    public ChangelogDeserializationSchema() {}

    /** Initializes the deserialization schema. */
    @Override
    public void open(InitializationContext context) throws Exception {
        if (converter == null) {
            this.converter = new ChangelogRowConverter(context.getRowSchema());
        }
    }

    /**
     * Deserializes a {@link LogRecord} into a Flink {@link RowData} object with metadata columns.
     */
    @Override
    public RowData deserialize(LogRecord record) throws Exception {
        if (converter == null) {
            throw new IllegalStateException(
                    "Converter not initialized. The open() method must be called before deserializing records.");
        }
        return converter.toChangelogRowData(record);
    }

    /**
     * Returns the TypeInformation for the produced {@link RowData} type including metadata columns.
     */
    @Override
    public TypeInformation<RowData> getProducedType(RowType rowSchema) {
        // Build the output type with metadata columns
        org.apache.flink.table.types.logical.RowType outputType =
                ChangelogRowConverter.buildChangelogRowType(toFlinkRowType(rowSchema));
        return InternalTypeInfo.of(outputType);
    }
}
