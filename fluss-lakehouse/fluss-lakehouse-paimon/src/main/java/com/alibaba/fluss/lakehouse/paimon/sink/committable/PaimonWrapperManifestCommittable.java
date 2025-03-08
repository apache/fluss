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

package com.alibaba.fluss.lakehouse.paimon.sink.committable;

import com.alibaba.fluss.metadata.TableBucket;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.WrappedManifestCommittable;

import java.util.HashMap;
import java.util.Map;

/**
 * Extending {@link WrappedManifestCommittable}, but add start/end offset of the log written to
 * paimon for the tables.
 */
public class PaimonWrapperManifestCommittable extends WrappedManifestCommittable {

    // mapping from table path to table id
    private final Map<Identifier, Long> tableIdByIdentifier;
    private final Map<Long, String> partitionNameById;
    // mapping from table id to log end offset
    private final Map<Long, Map<TableBucket, Long>> tableLogEndOffset;

    public PaimonWrapperManifestCommittable(long checkpointId, long watermark) {
        super(checkpointId, watermark);
        this.tableIdByIdentifier = new HashMap<>();
        this.tableLogEndOffset = new HashMap<>();
        this.partitionNameById = new HashMap<>();
    }

    public PaimonWrapperManifestCommittable(
            WrappedManifestCommittable wrappedManifestCommittable,
            Map<Identifier, Long> tableIdByIdentifiers,
            Map<Long, String> partitionNameById,
            Map<Long, Map<TableBucket, Long>> tableEndLogOffset) {
        super(wrappedManifestCommittable.checkpointId(), wrappedManifestCommittable.watermark());

        for (Map.Entry<Identifier, ManifestCommittable> manifestCommittableEntry :
                wrappedManifestCommittable.manifestCommittables().entrySet()) {
            putManifestCommittable(
                    manifestCommittableEntry.getKey(), manifestCommittableEntry.getValue());
        }

        this.partitionNameById = partitionNameById;
        this.tableIdByIdentifier = tableIdByIdentifiers;
        this.tableLogEndOffset = tableEndLogOffset;
    }

    public void putTableIdentifierAndId(Identifier identifier, long tableId) {
        tableIdByIdentifier.put(identifier, tableId);
    }

    public void putPartitions(Map<Long, String> partitionNameById) {
        this.partitionNameById.putAll(partitionNameById);
    }

    public Map<TableBucket, Long> computeEndLogOffsetIfAbsent(long tableId) {
        return tableLogEndOffset.computeIfAbsent(tableId, k -> new HashMap<>());
    }

    public Map<Identifier, Long> getTableIdByIdentifier() {
        return tableIdByIdentifier;
    }

    public Map<Long, String> getPartitionNameById() {
        return partitionNameById;
    }

    public Map<Long, Map<TableBucket, Long>> getTableLogEndOffset() {
        return tableLogEndOffset;
    }
}
