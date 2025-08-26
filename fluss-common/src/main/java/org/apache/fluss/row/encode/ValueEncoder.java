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

package org.apache.fluss.row.encode;

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.utils.UnsafeUtils;

/** An encoder to encode {@link BinaryRow} with a schema id as value to be stored in kv store. */
public class ValueEncoder {

    static final int SCHEMA_ID_LENGTH = 2;

    /**
     * Encode the {@code row} with a {@code schemaId} to a byte array value to be expected persisted
     * to kv store.
     *
     * @param schemaId the schema id of the row
     * @param row the row to encode
     */
    public static byte[] encodeValue(short schemaId, BinaryRow row) {
        byte[] values = new byte[SCHEMA_ID_LENGTH + row.getSizeInBytes()];
        UnsafeUtils.putShort(values, 0, schemaId);
        row.copyTo(values, SCHEMA_ID_LENGTH);
        return values;
    }
}
