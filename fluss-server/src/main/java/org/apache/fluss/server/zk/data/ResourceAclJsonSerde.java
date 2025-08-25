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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/** Json serializer and deserializer for {@link ResourceAcl}. */
public class ResourceAclJsonSerde
        implements JsonSerializer<ResourceAcl>, JsonDeserializer<ResourceAcl> {
    public static final ResourceAclJsonSerde INSTANCE = new ResourceAclJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String ACLS = "acls";
    private static final String PRINCIPAL_TYPE = "principal_type";
    private static final String PRINCIPAL_NAME = "principal_name";
    private static final String PERMISSION_TYPE = "permission_type";
    private static final String HOST = "host";
    private static final String OPERATION_TYPE = "operation";
    private static final int VERSION = 1;

    @Override
    public void serialize(ResourceAcl resourceAcl, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        serializeAccessControlEntries(generator, resourceAcl.getEntries());

        generator.writeEndObject();
    }

    @Override
    public ResourceAcl deserialize(JsonNode node) {
        Set<AccessControlEntry> entries = deserializeAccessControlEntries(node.get(ACLS));
        return new ResourceAcl(entries);
    }

    private static void serializeAccessControlEntries(
            JsonGenerator generator, Collection<AccessControlEntry> accessControlEntries)
            throws IOException {
        generator.writeArrayFieldStart(ACLS);
        for (AccessControlEntry entry : accessControlEntries) {
            generator.writeStartObject();
            generator.writeStringField(PRINCIPAL_TYPE, entry.getPrincipal().getType());
            generator.writeStringField(PRINCIPAL_NAME, entry.getPrincipal().getName());
            generator.writeStringField(PERMISSION_TYPE, entry.getPermissionType().name());
            generator.writeStringField(HOST, entry.getHost());
            generator.writeStringField(OPERATION_TYPE, entry.getOperationType().name());
            generator.writeEndObject();
        }
        generator.writeEndArray();
    }

    private static Set<AccessControlEntry> deserializeAccessControlEntries(JsonNode node) {
        Iterator<JsonNode> elements = node.elements();
        Set<AccessControlEntry> entries = new HashSet<>();
        while (elements.hasNext()) {
            JsonNode aclNode = elements.next();
            AccessControlEntry entry =
                    new AccessControlEntry(
                            new FlussPrincipal(
                                    aclNode.get(PRINCIPAL_NAME).asText(),
                                    aclNode.get(PRINCIPAL_TYPE).asText()),
                            aclNode.get(HOST).asText(),
                            OperationType.valueOf(aclNode.get(OPERATION_TYPE).asText()),
                            PermissionType.valueOf(aclNode.get(PERMISSION_TYPE).asText()));
            entries.add(entry);
        }
        return entries;
    }
}
