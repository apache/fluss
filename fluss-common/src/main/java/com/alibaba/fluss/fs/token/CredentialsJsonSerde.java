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

package com.alibaba.fluss.fs.token;

import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.utils.json.JsonDeserializer;
import com.alibaba.fluss.utils.json.JsonSerdeUtils;
import com.alibaba.fluss.utils.json.JsonSerializer;

import java.io.IOException;

/** Json serializer and deserializer for {@link Credentials}. */
public class CredentialsJsonSerde
        implements JsonSerializer<Credentials>, JsonDeserializer<Credentials> {

    public static final CredentialsJsonSerde INSTANCE = new CredentialsJsonSerde();

    private static final int VERSION = 1;
    private static final String VERSION_KEY = "version";
    private static final String ACCESS_KEY_ID_KEY = "access_key_id";
    private static final String ACCESS_KEY_SECRET_KEY = "access_key_secret";
    private static final String ACCESS_TOKEN_KEY = "security_token";

    @Override
    public void serialize(Credentials credentials, JsonGenerator generator) throws IOException {
        generator.writeStartObject();

        // serialize data version.
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(ACCESS_KEY_ID_KEY, credentials.getAccessKeyId());
        generator.writeStringField(ACCESS_KEY_SECRET_KEY, credentials.getSecretAccessKey());
        generator.writeStringField(ACCESS_TOKEN_KEY, credentials.getSecurityToken());

        generator.writeEndObject();
    }

    @Override
    public Credentials deserialize(JsonNode node) {
        String accessKeyId = node.get(ACCESS_KEY_ID_KEY).asText();
        String accessKeySecret = node.get(ACCESS_KEY_SECRET_KEY).asText();
        JsonNode tokenNode = node.get(ACCESS_TOKEN_KEY);
        final String securityToken;
        if (tokenNode.isNull()) {
            securityToken = null;
        } else {
            securityToken = tokenNode.asText();
        }
        return new Credentials(accessKeyId, accessKeySecret, securityToken);
    }

    /** Serialize the {@link Credentials} to json bytes. */
    public static byte[] toJson(Credentials credentials) {
        return JsonSerdeUtils.writeValueAsBytes(credentials, INSTANCE);
    }

    /** Deserialize the json bytes to {@link Credentials}. */
    public static Credentials fromJson(byte[] json) {
        return JsonSerdeUtils.readValue(json, INSTANCE);
    }
}
