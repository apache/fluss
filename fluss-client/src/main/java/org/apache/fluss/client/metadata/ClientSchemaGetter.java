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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.utils.MapUtils.newConcurrentHashMap;

/** Schema getter for client. */
@Internal
public class ClientSchemaGetter implements SchemaGetter {
    private static final Logger LOG = LoggerFactory.getLogger(ClientSchemaGetter.class);

    private final TablePath tablePath;
    private final Map<Integer, Schema> schemasById;
    private final Admin admin;
    private SchemaInfo latestSchemaInfo;

    public ClientSchemaGetter(TablePath tablePath, SchemaInfo latestSchemaInfo, Admin admin) {
        this.tablePath = tablePath;
        this.latestSchemaInfo = latestSchemaInfo;
        this.admin = admin;
        this.schemasById = newConcurrentHashMap();
        schemasById.put(latestSchemaInfo.getSchemaId(), latestSchemaInfo.getSchema());
    }

    @Override
    public Schema getSchema(int schemaId) {
        return schemasById.computeIfAbsent(
                schemaId,
                (id) -> {
                    try {
                        SchemaInfo schemaInfo =
                                admin.getTableSchema(tablePath, schemaId).get(1, TimeUnit.MINUTES);
                        if (id > latestSchemaInfo.getSchemaId()) {
                            latestSchemaInfo = schemaInfo;
                        }
                        return schemaInfo.getSchema();

                    } catch (Exception e) {
                        LOG.warn("Failed to get schema for table: " + tablePath);
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public SchemaInfo getLatestSchemaInfo() {
        return latestSchemaInfo;
    }

    @Override
    public void release() {}
}
