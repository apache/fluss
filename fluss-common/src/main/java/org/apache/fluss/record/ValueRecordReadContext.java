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

package org.apache.fluss.record;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.types.DataType;

/** A default implementation of {@link ValueRecordBatch.ReadContext} . */
public class ValueRecordReadContext implements ValueRecordBatch.ReadContext {

    private final RowDecoder rowDecoder;

    public ValueRecordReadContext(RowDecoder rowDecoder) {
        this.rowDecoder = rowDecoder;
    }

    public static ValueRecordReadContext createReadContext(
            KvFormat kvFormat, DataType[] fieldTypes) {
        return new ValueRecordReadContext(RowDecoder.create(kvFormat, fieldTypes));
    }

    @Override
    public RowDecoder getRowDecoder(int schemaId) {
        // assume schema id are always not changed for now.
        return rowDecoder;
    }
}
