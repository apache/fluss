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

package org.apache.fluss.lake.lance.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.BucketOffset;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.lance.LanceConfig;
import org.apache.fluss.lake.lance.utils.LanceDatasetAdapter;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.json.BucketOffsetJsonSerde;
import org.apache.fluss.utils.types.Tuple2;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.ReadOptions;
import com.lancedb.lance.Transaction;
import com.lancedb.lance.Version;
import org.apache.arrow.memory.RootAllocator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.lake.writer.LakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/** Implementation of {@link LakeCommitter} for Lance. */
public class LanceLakeCommitter implements LakeCommitter<LanceWriteResult, LanceCommittable> {
    private final LanceConfig config;
    private static final String committerName = "commit-user";
    private final RootAllocator allocator = new RootAllocator();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public LanceLakeCommitter(Configuration options, TablePath tablePath) {
        this.config =
                LanceConfig.from(
                        options.toMap(),
                        Collections.emptyMap(),
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());
    }

    @Override
    public LanceCommittable toCommittable(List<LanceWriteResult> lanceWriteResults)
            throws IOException {
        List<FragmentMetadata> fragments =
                lanceWriteResults.stream()
                        .map(LanceWriteResult::commitMessage)
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        return new LanceCommittable(fragments);
    }

    @Override
    public long commit(LanceCommittable committable, Map<String, String> snapshotProperties)
            throws IOException {
        Map<String, String> properties = new HashMap<>(snapshotProperties);
        properties.put(committerName, FLUSS_LAKE_TIERING_COMMIT_USER);
        return LanceDatasetAdapter.commitAppend(config, committable.committable(), properties);
    }

    @Override
    public void abort(LanceCommittable committable) throws IOException {
        // TODO lance does not have the API to proactively delete the written files yet, see
        // https://github.com/lancedb/lance/issues/4508
    }

    @SuppressWarnings("checkstyle:LocalVariableName")
    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        Tuple2<Version, Transaction> latestLakeSnapshotIdOfLake =
                getCommittedLatestSnapshotOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);

        if (latestLakeSnapshotIdOfLake == null) {
            return null;
        }

        // we get the latest snapshot committed by fluss,
        // but the latest snapshot is not greater than latestLakeSnapshotIdOfFluss, no any missing
        // snapshot, return directly
        if (latestLakeSnapshotIdOfFluss != null
                && latestLakeSnapshotIdOfLake.f0.getId() <= latestLakeSnapshotIdOfFluss) {
            return null;
        }

        CommittedLakeSnapshot committedLakeSnapshot =
                new CommittedLakeSnapshot(latestLakeSnapshotIdOfLake.f0.getId());
        String flussOffsetProperties =
                latestLakeSnapshotIdOfLake
                        .f1
                        .transactionProperties()
                        .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        for (JsonNode node : OBJECT_MAPPER.readTree(flussOffsetProperties)) {
            BucketOffset bucketOffset = BucketOffsetJsonSerde.INSTANCE.deserialize(node);
            if (bucketOffset.getPartitionId() != null) {
                committedLakeSnapshot.addPartitionBucket(
                        bucketOffset.getPartitionId(),
                        bucketOffset.getPartitionQualifiedName(),
                        bucketOffset.getBucket(),
                        bucketOffset.getLogOffset());
            } else {
                committedLakeSnapshot.addBucket(
                        bucketOffset.getBucket(), bucketOffset.getLogOffset());
            }
        }
        return committedLakeSnapshot;
    }

    @Nullable
    private Tuple2<Version, Transaction> getCommittedLatestSnapshotOfLake(String commitUser) {
        ReadOptions.Builder builder = new ReadOptions.Builder();
        builder.setStorageOptions(LanceConfig.genStorageOptions(config));
        try (Dataset dataset = Dataset.open(allocator, config.getDatasetUri(), builder.build())) {
            List<Version> versions = dataset.listVersions();
            for (int i = versions.size() - 1; i >= 0; i--) {
                Version version = versions.get(i);
                builder.setVersion((int) version.getId());
                try (Dataset datasetRead =
                        Dataset.open(allocator, config.getDatasetUri(), builder.build())) {
                    Transaction transaction = datasetRead.readTransaction().orElse(null);
                    if (transaction != null
                            && commitUser.equals(
                                    transaction.transactionProperties().get(committerName))) {
                        return Tuple2.of(version, transaction);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        allocator.close();
    }
}
