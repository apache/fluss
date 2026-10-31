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

package org.apache.fluss.lake.delta.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.delta.DeltaConfig;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Implementation of {@link LakeCommitter} for Delta Lake.
 *
 * <p>Each tiering round becomes one Delta commit that adds the written data files. The snapshot
 * properties of the round, which point to the Fluss bucket offsets, are stored as the domain
 * metadata {@link #FLUSS_DOMAIN} in the same commit, so the offsets and the data become visible
 * atomically. {@link #getMissingLakeSnapshot} reads the domain metadata back to find a commit that
 * reached Delta but not Fluss.
 */
public class DeltaLakeCommitter implements LakeCommitter<DeltaWriteResult, DeltaCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaLakeCommitter.class);

    /** The Delta domain that keeps the snapshot properties of the latest Fluss tiering commit. */
    public static final String FLUSS_DOMAIN = "fluss.tiering";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<HashMap<String, String>> PROPERTIES_TYPE =
            new TypeReference<HashMap<String, String>>() {};

    private final Engine engine;
    private final String tablePath;

    public DeltaLakeCommitter(Configuration options, TablePath tablePath) {
        this.engine = DeltaConfig.createEngine(options);
        this.tablePath = DeltaConfig.tablePath(options, tablePath);
    }

    @Override
    public DeltaCommittable toCommittable(List<DeltaWriteResult> writeResults) {
        List<DeltaDataFile> dataFiles = new ArrayList<>();
        for (DeltaWriteResult writeResult : writeResults) {
            dataFiles.addAll(writeResult.dataFiles());
        }
        return new DeltaCommittable(dataFiles);
    }

    @Override
    public LakeCommitResult commit(DeltaCommittable committable, CommitContext context)
            throws IOException {
        Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(engine);
        Transaction transaction =
                snapshot.buildUpdateTableTransaction(DeltaConfig.ENGINE_INFO, Operation.WRITE)
                        .build(engine);
        Row transactionState = transaction.getTransactionState(engine);
        StructType schema = transaction.getSchema(engine);

        // Kernel generates the add actions per partition, since the write context depends on the
        // partition values.
        Map<Map<String, String>, List<DataFileStatus>> filesByPartition = new HashMap<>();
        for (DeltaDataFile dataFile : committable.dataFiles()) {
            filesByPartition
                    .computeIfAbsent(dataFile.partitionValues(), k -> new ArrayList<>())
                    .add(dataFile.toDataFileStatus(schema));
        }
        List<Row> actions = new ArrayList<>();
        for (Map.Entry<Map<String, String>, List<DataFileStatus>> entry :
                filesByPartition.entrySet()) {
            Map<String, Literal> partitionLiterals = new HashMap<>();
            for (Map.Entry<String, String> value : entry.getKey().entrySet()) {
                partitionLiterals.put(value.getKey(), Literal.ofString(value.getValue()));
            }
            DataWriteContext writeContext =
                    Transaction.getWriteContext(engine, transactionState, partitionLiterals);
            try (CloseableIterator<Row> addActions =
                    Transaction.generateAppendActions(
                            engine,
                            transactionState,
                            CloseableIterators.of(entry.getValue().iterator()),
                            writeContext)) {
                while (addActions.hasNext()) {
                    actions.add(addActions.next());
                }
            }
        }

        transaction.addDomainMetadata(
                FLUSS_DOMAIN, OBJECT_MAPPER.writeValueAsString(context.snapshotProperties()));
        TransactionCommitResult result =
                transaction.commit(
                        engine,
                        CloseableIterable.inMemoryIterable(
                                CloseableIterators.of(actions.iterator())));
        // Post commit hooks write checkpoints and checksums, which keep the log fast to read.
        // They are optional, so a failure must not fail the commit that already succeeded.
        for (PostCommitHook hook : result.getPostCommitHooks()) {
            try {
                hook.threadSafeInvoke(engine);
            } catch (Exception e) {
                LOG.warn(
                        "Failed to run post commit hook {} for Delta table {} at version {}.",
                        hook.getType(),
                        tablePath,
                        result.getVersion(),
                        e);
            }
        }
        return LakeCommitResult.committedIsReadable(result.getVersion());
    }

    @Override
    public void abort(DeltaCommittable committable) throws IOException {
        for (DeltaDataFile dataFile : committable.dataFiles()) {
            engine.getFileSystemClient().delete(dataFile.path());
        }
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        Snapshot latest;
        try {
            latest = TableManager.loadSnapshot(tablePath).build(engine);
        } catch (TableNotFoundException e) {
            return null;
        }
        Optional<String> properties = latest.getDomainMetadata(FLUSS_DOMAIN);
        if (!properties.isPresent()) {
            return null;
        }

        // Other writers may commit after Fluss, so walk back to the version that set the domain
        // metadata. The value holds the path of a new offsets file per commit, so it changes on
        // every Fluss commit.
        long version = latest.getVersion();
        while (version > 0
                && (latestLakeSnapshotIdOfFluss == null || version > latestLakeSnapshotIdOfFluss)) {
            Optional<String> previous =
                    TableManager.loadSnapshot(tablePath)
                            .atVersion(version - 1)
                            .build(engine)
                            .getDomainMetadata(FLUSS_DOMAIN);
            if (!properties.equals(previous)) {
                break;
            }
            version--;
        }

        if (latestLakeSnapshotIdOfFluss != null && version <= latestLakeSnapshotIdOfFluss) {
            return null;
        }
        return new CommittedLakeSnapshot(
                version, OBJECT_MAPPER.readValue(properties.get(), PROPERTIES_TYPE));
    }

    @Override
    public void close() {}
}
