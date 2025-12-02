/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.FixedSchemaDecoder;
import org.apache.fluss.utils.CopyOnWriteMap;
import org.apache.fluss.utils.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Abstract lookuper implementation for common methods. */
abstract class AbstractLookuper implements Lookuper {
    protected final TableInfo tableInfo;

    protected final MetadataUpdater metadataUpdater;

    protected final LookupClient lookupClient;

    protected final short targetSchemaId;

    private final SchemaGetter schemaGetter;

    /**
     * Cache for row decoders for different schema ids. Use CopyOnWriteMap for fast access, as it is
     * not frequently updated.
     */
    private final CopyOnWriteMap<Short, FixedSchemaDecoder> decoders;

    AbstractLookuper(
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            LookupClient lookupClient,
            SchemaGetter schemaGetter) {
        this.tableInfo = tableInfo;
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;
        this.targetSchemaId = (short) tableInfo.getSchemaId();
        this.schemaGetter = schemaGetter;
        this.decoders = new CopyOnWriteMap<>();
        // initialize the decoder for the same schema
        this.decoders.put(
                targetSchemaId,
                new FixedSchemaDecoder(
                        tableInfo.getTableConfig().getKvFormat(), tableInfo.getSchema()));
    }

    protected void handleLookupResponse(
            List<byte[]> result, CompletableFuture<LookupResult> lookupFuture) {
        List<MemorySegment> valueList = new ArrayList<>(result.size());
        Set<Short> schemaIdsToRequest = new HashSet<>();
        boolean allTargetSchema = true;
        for (byte[] valueBytes : result) {
            if (valueBytes == null) {
                continue;
            }
            MemorySegment memorySegment = MemorySegment.wrap(valueBytes);
            short schemaId = memorySegment.getShort(0);
            if (targetSchemaId != schemaId) {
                allTargetSchema = false;
                if (!decoders.containsKey(schemaId)) {
                    schemaIdsToRequest.add(schemaId);
                }
            }
            valueList.add(memorySegment);
        }
        // all schema ids are the target schema id, fast path
        if (allTargetSchema) {
            lookupFuture.complete(processAllTargetSchemaRows(valueList));
            return;
        }

        // all schema ids and decoders are cached in local
        if (schemaIdsToRequest.isEmpty()) {
            lookupFuture.complete(processSchemaMismatchedRows(valueList));
            return;
        }

        // need to fetch schema infos for schema ids
        List<CompletableFuture<SchemaInfo>> schemaFutures =
                new ArrayList<>(schemaIdsToRequest.size());
        for (short schemaId : schemaIdsToRequest) {
            CompletableFuture<SchemaInfo> schemaFuture = schemaGetter.getSchemaInfoAsync(schemaId);
            schemaFutures.add(schemaFuture);
        }
        FutureUtils.ConjunctFuture<Collection<SchemaInfo>> allFutures =
                FutureUtils.combineAll(schemaFutures);
        // normal async path
        allFutures.whenComplete(
                (schemas, error) -> {
                    if (error != null) {
                        lookupFuture.completeExceptionally(
                                new RuntimeException(
                                        "Failed to get schema infos for lookup", error));
                    } else {
                        LookupResult lookupResult = processSchemaRequestedRows(schemas, valueList);
                        lookupFuture.complete(lookupResult);
                    }
                });
    }

    protected LookupResult processAllTargetSchemaRows(List<MemorySegment> valueList) {
        FixedSchemaDecoder decoder = decoders.get(targetSchemaId);
        List<InternalRow> rowList = new ArrayList<>(valueList.size());
        for (MemorySegment value : valueList) {
            rowList.add(decoder.decode(value));
        }
        return new LookupResult(rowList);
    }

    protected LookupResult processSchemaMismatchedRows(List<MemorySegment> valueList) {
        List<InternalRow> rowList = new ArrayList<>(valueList.size());
        for (MemorySegment value : valueList) {
            short schemaId = value.getShort(0);
            FixedSchemaDecoder decoder = decoders.get(schemaId);
            checkArgument(decoder != null, "Decoder for schema id %s not found", schemaId);
            InternalRow row = decoder.decode(value);
            rowList.add(row);
        }
        return new LookupResult(rowList);
    }

    protected LookupResult processSchemaRequestedRows(
            Collection<SchemaInfo> schemaInfos, List<MemorySegment> valueList) {
        // build a map from schema id to the target schema decoder
        Map<Short, Schema> schemaMap = new HashMap<>();
        for (SchemaInfo schemaInfo : schemaInfos) {
            schemaMap.put((short) schemaInfo.getSchemaId(), schemaInfo.getSchema());
        }

        // process the value list to convert to target schema
        List<InternalRow> rowList = new ArrayList<>(valueList.size());
        for (MemorySegment value : valueList) {
            short schemaId = value.getShort(0);
            FixedSchemaDecoder decoder =
                    decoders.computeIfAbsent(
                            schemaId,
                            (id) -> {
                                Schema sourceSchema = schemaMap.get(id);
                                return new FixedSchemaDecoder(
                                        tableInfo.getTableConfig().getKvFormat(),
                                        sourceSchema,
                                        tableInfo.getSchema());
                            });
            InternalRow row = decoder.decode(value);
            rowList.add(row);
        }
        return new LookupResult(rowList);
    }
}
