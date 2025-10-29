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

package org.apache.fluss.client.metadata;

import org.apache.fluss.client.admin.FlussAdmin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;

import java.util.concurrent.CompletableFuture;

/** Testing class for {@link ClientSchemaGetter}. */
public class TestingClientSchemaGetter extends ClientSchemaGetter {
    public TestingClientSchemaGetter(
            TablePath tablePath,
            SchemaInfo latestSchemaInfo,
            TestingMetadataUpdater metadataUpdater) {
        super(
                tablePath,
                latestSchemaInfo,
                new FlussAdmin(
                        RpcClient.create(
                                new Configuration(), TestingClientMetricGroup.newInstance(), false),
                        metadataUpdater));
    }

    @Override
    public Schema getSchema(int schemaId) {
        return schemasById.get(schemaId);
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchemaInfoAsync(int schemaId) {
        return CompletableFuture.completedFuture(
                new SchemaInfo(schemasById.get(schemaId), schemaId));
    }
}
