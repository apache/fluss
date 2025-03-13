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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonEncoding;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

/** Test for {@link com.alibaba.fluss.server.zk.data.CoordinatorAddressJsonSerde}. */
public class CoordinatorAddressJsonSerdeTest extends JsonSerdeTestBase<CoordinatorAddress> {

    CoordinatorAddressJsonSerdeTest() {
        super(CoordinatorAddressJsonSerde.INSTANCE);
    }

    @Override
    protected CoordinatorAddress[] createObjects() {
        CoordinatorAddress coordinatorAddress =
                new CoordinatorAddress("1", Endpoint.parseEndpoints("CLIENT://localhost:1001"));
        return new CoordinatorAddress[] {coordinatorAddress};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":2,\"id\":\"1\",\"listeners\":\"CLIENT://localhost:1001\"}"
        };
    }

    @Test
    void testCompatibility() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonInVersion1 = objectMapper.readTree(serializeInVersion1(objectMapper));

        CoordinatorAddress coordinatorAddress =
                CoordinatorAddressJsonSerde.INSTANCE.deserialize(jsonInVersion1);
        CoordinatorAddress expectedCoordinator =
                new CoordinatorAddress("1", Endpoint.parseEndpoints("CLIENT://localhost:1001"));
        assertEquals(coordinatorAddress, expectedCoordinator);
    }

    private byte[] serializeInVersion1(ObjectMapper objectMapper) {
        try (ByteArrayBuilder bb =
                new ByteArrayBuilder(objectMapper.getFactory()._getBufferRecycler())) {
            JsonGenerator generator = objectMapper.createGenerator(bb, JsonEncoding.UTF8);
            generator.writeStartObject();
            generator.writeNumberField("version", 1);
            generator.writeStringField("id", "1");
            generator.writeStringField("host", "localhost");
            generator.writeNumberField("port", 1001);
            generator.writeEndObject();
            generator.close();
            final byte[] result = bb.toByteArray();
            bb.release();
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
