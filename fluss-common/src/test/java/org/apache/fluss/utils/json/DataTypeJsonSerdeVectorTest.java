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

package org.apache.fluss.utils.json;

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.VectorElementType;
import org.apache.fluss.types.VectorType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for JSON serialization/deserialization of {@link VectorType} via {@link DataTypeJsonSerde}.
 */
public class DataTypeJsonSerdeVectorTest {

    @Test
    void testSerializeVectorType() throws Exception {
        VectorType vectorType = new VectorType(1536);
        String json =
                new String(
                        JsonSerdeUtils.writeValueAsBytes(vectorType, DataTypeJsonSerde.INSTANCE),
                        StandardCharsets.UTF_8);
        assertThat(json).contains("\"type\":\"VECTOR\"");
        assertThat(json).contains("\"dimension\":1536");
        assertThat(json).contains("\"elementType\":\"FLOAT32\"");
    }

    @Test
    void testSerializeVectorTypeNotNull() throws Exception {
        VectorType vectorType = new VectorType(false, 768, VectorElementType.FLOAT32);
        String json =
                new String(
                        JsonSerdeUtils.writeValueAsBytes(vectorType, DataTypeJsonSerde.INSTANCE),
                        StandardCharsets.UTF_8);
        assertThat(json).contains("\"type\":\"VECTOR\"");
        assertThat(json).contains("\"nullable\":false");
        assertThat(json).contains("\"dimension\":768");
    }

    @Test
    void testDeserializeVectorType() {
        String json = "{\"type\":\"VECTOR\",\"dimension\":1536,\"elementType\":\"FLOAT32\"}";
        DataType result =
                JsonSerdeUtils.readValue(
                        json.getBytes(StandardCharsets.UTF_8), DataTypeJsonSerde.INSTANCE);
        assertThat(result).isInstanceOf(VectorType.class);
        VectorType vectorType = (VectorType) result;
        assertThat(vectorType.getDimension()).isEqualTo(1536);
        assertThat(vectorType.getElementType()).isEqualTo(VectorElementType.FLOAT32);
        assertThat(vectorType.isNullable()).isTrue();
    }

    @Test
    void testDeserializeVectorTypeNotNull() {
        String json =
                "{\"type\":\"VECTOR\",\"nullable\":false,\"dimension\":768,\"elementType\":\"FLOAT32\"}";
        DataType result =
                JsonSerdeUtils.readValue(
                        json.getBytes(StandardCharsets.UTF_8), DataTypeJsonSerde.INSTANCE);
        assertThat(result).isInstanceOf(VectorType.class);
        assertThat(result.isNullable()).isFalse();
        assertThat(((VectorType) result).getDimension()).isEqualTo(768);
    }

    @Test
    void testDeserializeMissingElementTypeDefaultsToFloat32() {
        // elementType field absent — should default to FLOAT32 for backward compatibility
        String json = "{\"type\":\"VECTOR\",\"dimension\":512}";
        DataType result =
                JsonSerdeUtils.readValue(
                        json.getBytes(StandardCharsets.UTF_8), DataTypeJsonSerde.INSTANCE);
        assertThat(result).isInstanceOf(VectorType.class);
        assertThat(((VectorType) result).getElementType()).isEqualTo(VectorElementType.FLOAT32);
    }

    @Test
    void testRoundTrip() throws Exception {
        VectorType original = new VectorType(1024);
        byte[] serialized = JsonSerdeUtils.writeValueAsBytes(original, DataTypeJsonSerde.INSTANCE);
        DataType deserialized = JsonSerdeUtils.readValue(serialized, DataTypeJsonSerde.INSTANCE);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void testRoundTripNotNull() throws Exception {
        VectorType original = new VectorType(false, 256, VectorElementType.FLOAT32);
        byte[] serialized = JsonSerdeUtils.writeValueAsBytes(original, DataTypeJsonSerde.INSTANCE);
        DataType deserialized = JsonSerdeUtils.readValue(serialized, DataTypeJsonSerde.INSTANCE);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void testVectorEmbeddedInRowType() throws Exception {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT().copy(false)),
                        DataTypes.FIELD("embedding", DataTypes.VECTOR(4)));
        byte[] serialized = JsonSerdeUtils.writeValueAsBytes(rowType, DataTypeJsonSerde.INSTANCE);
        DataType deserialized = JsonSerdeUtils.readValue(serialized, DataTypeJsonSerde.INSTANCE);
        assertThat(deserialized).isEqualTo(rowType);
    }
}
