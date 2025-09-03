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
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

/** Test for {@link ResourceAclJsonSerde}. */
public class ResourceAclJsonSerdeTest extends JsonSerdeTestBase<ResourceAcl> {

    ResourceAclJsonSerdeTest() {
        super(ResourceAclJsonSerde.INSTANCE);
    }

    @Override
    protected ResourceAcl[] createObjects() {
        return new ResourceAcl[] {
            new ResourceAcl(
                    Collections.singleton(
                            new AccessControlEntry(
                                    new FlussPrincipal("Mike", "USER"),
                                    "*",
                                    OperationType.ALL,
                                    PermissionType.ALLOW))),
            new ResourceAcl(
                    // use LinkedHashSet to ensure the order of the acls is preserved
                    new LinkedHashSet<>(
                            Arrays.asList(
                                    new AccessControlEntry(
                                            new FlussPrincipal("John", "ROLE"),
                                            "127.0.0.1",
                                            OperationType.ALTER,
                                            PermissionType.ALLOW),
                                    new AccessControlEntry(
                                            new FlussPrincipal("Mike1233", "ROLE"),
                                            "1*",
                                            OperationType.READ,
                                            PermissionType.ALLOW)))),
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"acls\":[{\"principal_type\":\"USER\",\"principal_name\":\"Mike\",\"permission_type\":\"ALLOW\",\"host\":\"*\",\"operation\":\"ALL\"}]}",
            "{\"version\":1,\"acls\":[{\"principal_type\":\"ROLE\",\"principal_name\":\"John\",\"permission_type\":\"ALLOW\",\"host\":\"127.0.0.1\",\"operation\":\"ALTER\"}"
                    + ",{\"principal_type\":\"ROLE\",\"principal_name\":\"Mike1233\",\"permission_type\":\"ALLOW\",\"host\":\"1*\",\"operation\":\"READ\"}]}"
        };
    }
}
