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

package org.apache.fluss.rpc.gateway;

import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.AlterTableResponse;
import org.apache.fluss.rpc.messages.CreateAclsRequest;
import org.apache.fluss.rpc.messages.CreateAclsResponse;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.messages.CreateDatabaseResponse;
import org.apache.fluss.rpc.messages.CreatePartitionRequest;
import org.apache.fluss.rpc.messages.CreatePartitionResponse;
import org.apache.fluss.rpc.messages.CreateTableRequest;
import org.apache.fluss.rpc.messages.CreateTableResponse;
import org.apache.fluss.rpc.messages.DropAclsRequest;
import org.apache.fluss.rpc.messages.DropAclsResponse;
import org.apache.fluss.rpc.messages.DropDatabaseRequest;
import org.apache.fluss.rpc.messages.DropDatabaseResponse;
import org.apache.fluss.rpc.messages.DropPartitionRequest;
import org.apache.fluss.rpc.messages.DropPartitionResponse;
import org.apache.fluss.rpc.messages.DropTableRequest;
import org.apache.fluss.rpc.messages.DropTableResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.RPC;

import java.util.concurrent.CompletableFuture;

/** The gateway interface between the client and the server for reading and writing metadata. */
public interface AdminGateway extends AdminReadOnlyGateway {
    /**
     * Create a database.
     *
     * @param request Create database request
     */
    @RPC(api = ApiKeys.CREATE_DATABASE)
    CompletableFuture<CreateDatabaseResponse> createDatabase(CreateDatabaseRequest request);

    /**
     * Drop a database.
     *
     * @param request Drop database request.
     */
    @RPC(api = ApiKeys.DROP_DATABASE)
    CompletableFuture<DropDatabaseResponse> dropDatabase(DropDatabaseRequest request);

    /**
     * Creates a new table.
     *
     * @param request the request to create table.
     */
    @RPC(api = ApiKeys.CREATE_TABLE)
    CompletableFuture<CreateTableResponse> createTable(CreateTableRequest request);

    /**
     * Alter a table.
     *
     * @param request the request to alter a table.
     */
    @RPC(api = ApiKeys.ALTER_TABLE_PROPERTIES)
    CompletableFuture<AlterTableResponse> alterTable(AlterTableRequest request);

    /**
     * Drop a table.
     *
     * @param request Drop table request
     */
    @RPC(api = ApiKeys.DROP_TABLE)
    CompletableFuture<DropTableResponse> dropTable(DropTableRequest request);

    /**
     * Create a new partition for a partitioned table.
     *
     * @param request Create partition request
     */
    @RPC(api = ApiKeys.CREATE_PARTITION)
    CompletableFuture<CreatePartitionResponse> createPartition(CreatePartitionRequest request);

    /**
     * Drop a partition from a partitioned table.
     *
     * @param request Drop partition request
     */
    @RPC(api = ApiKeys.DROP_PARTITION)
    CompletableFuture<DropPartitionResponse> dropPartition(DropPartitionRequest request);

    /**
     * create acls for a resource.
     *
     * @param request create acl request.
     */
    @RPC(api = ApiKeys.CREATE_ACLS)
    CompletableFuture<CreateAclsResponse> createAcls(CreateAclsRequest request);

    /**
     * Drop acls for a resource.
     *
     * @param request drop acl request.
     */
    @RPC(api = ApiKeys.DROP_ACLS)
    CompletableFuture<DropAclsResponse> dropAcls(DropAclsRequest request);

    // todo: rename table & alter table

}
