/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.format.json;

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataType;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the JSON contract with the streaming converter. */
class JsonToFlussComplexConvertersStreamingTest extends JsonToFlussComplexConvertersTest {
    @Override
    protected Object convert(DataType type, String json) throws Exception {
        try (JsonParser parser = new ObjectMapper().getFactory().createParser(json)) {
            parser.nextToken();
            Object value = JsonParserToFlussConverters.create(type).convert(parser, "$", null);
            assertThat(parser.nextToken()).isNull();
            return value;
        }
    }
}
