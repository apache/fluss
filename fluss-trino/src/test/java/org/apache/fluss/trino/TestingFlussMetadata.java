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

package org.apache.fluss.trino;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Shared factories for real Fluss table metadata and the mocked client boundary. */
final class TestingFlussMetadata {
    private TestingFlussMetadata() {}

    static FlussMetadataAccess metadataAccess(Admin admin) {
        FlussClientManager manager = mock(FlussClientManager.class);
        when(manager.getAdmin()).thenReturn(admin);
        return new FlussMetadataAccess(manager);
    }

    static TableInfo tableInfo(TableDescriptor descriptor) {
        return TableInfo.of(TablePath.of("Sales", "Users"), 42L, 3, descriptor, null, 0L, 0L);
    }

    static TableInfo usersTable() {
        return tableInfo(
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("Region", DataTypes.STRING())
                                        .column("ID", DataTypes.BIGINT())
                                        .withComment("User identifier")
                                        .column("Name", DataTypes.STRING())
                                        .primaryKey("ID", "Region")
                                        .build())
                        .partitionedBy("Region")
                        .distributedBy(4, "ID")
                        .comment("Registered users")
                        .build());
    }
}
