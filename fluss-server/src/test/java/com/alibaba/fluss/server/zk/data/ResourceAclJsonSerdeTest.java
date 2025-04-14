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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.security.acl.AccessControlEntry;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

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
                                    PermissionType.ANY))),
            new ResourceAcl(
                    new HashSet<>(
                            Arrays.asList(
                                    new AccessControlEntry(
                                            new FlussPrincipal("John", "ROLE"),
                                            "127.0.0.1",
                                            OperationType.ALTER,
                                            PermissionType.ANY),
                                    new AccessControlEntry(
                                            new FlussPrincipal("Mike1233", "ROLE"),
                                            "1*",
                                            OperationType.FILESYSTEM_TOKEN,
                                            PermissionType.ANY)))),
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"acls\":[{\"principalType\":\"USER\",\"principalName\":\"Mike\",\"permissionType\":\"ALLOW\",\"host\":\"*\",\"host\":\"*\",\"operation\":\"ALL\"}]}",
            "{\"version\":1,\"acls\":[{\"principalType\":\"ROLE\",\"principalName\":\"John\",\"permissionType\":\"ALLOW\",\"host\":\"127.0.0.1\",\"host\":\"127.0.0.1\",\"operation\":\"ALTER\"}"
                    + ",{\"principalType\":\"ROLE\",\"principalName\":\"Mike1233\",\"permissionType\":\"ALLOW\",\"host\":\"1*\",\"host\":\"1*\",\"operation\":\"FILESYSTEM_TOKEN\"}]}"
        };
    }
}
