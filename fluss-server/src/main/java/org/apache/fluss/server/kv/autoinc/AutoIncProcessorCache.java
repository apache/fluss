/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.server.zk.ZooKeeperClient;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;

/** AutoIncProcessorCache is used to cache auto increment processor for each schema ID. */
public class AutoIncProcessorCache {
    private final Cache<Integer, AutoIncProcessor> autoIncProcessorCache;
    private final SchemaGetter schemaGetter;
    private final KvFormat kvFormat;
    private AutoIncProcessor latestAutoIncProcessor;
    private int latestSchemaId;
    private final TablePath tablePath;
    private final Configuration properties;
    private final ZooKeeperClient zkClient;

    public AutoIncProcessorCache(
            SchemaGetter schemaGetter,
            KvFormat kvFormat,
            TablePath tablePath,
            Configuration properties,
            ZooKeeperClient zkClient) {
        this.autoIncProcessorCache =
                Caffeine.newBuilder()
                        .maximumSize(5)
                        .expireAfterAccess(Duration.ofMinutes(5))
                        .build();
        this.schemaGetter = schemaGetter;
        this.kvFormat = kvFormat;
        this.tablePath = tablePath;
        this.properties = properties;
        this.zkClient = zkClient;
    }

    public void configureSchema(int latestSchemaId) {
        if (latestSchemaId != this.latestSchemaId) {
            this.latestAutoIncProcessor =
                    getOrCreateAutoIncProcessor(
                            latestSchemaId, tablePath, properties, schemaGetter, zkClient);
            this.latestSchemaId = latestSchemaId;
        }
    }

    /**
     * Get or create auto increment processor for a given schema ID.
     *
     * @param schemaId the schema ID
     * @return AutoIncProcessor for the schema
     */
    private AutoIncProcessor getOrCreateAutoIncProcessor(
            int schemaId,
            TablePath tablePath,
            Configuration properties,
            SchemaGetter schemaGetter,
            ZooKeeperClient zkClient) {
        Schema schema = schemaGetter.getSchema(schemaId);
        return autoIncProcessorCache.get(
                schemaId,
                k ->
                        new AutoIncProcessor(
                                tablePath,
                                properties,
                                kvFormat,
                                (short) schemaId,
                                schema,
                                zkClient));
    }

    public BinaryValue processAutoInc(BinaryValue oldValue) {
        return latestAutoIncProcessor.processAutoInc(oldValue);
    }
}
