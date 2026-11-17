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
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.delta.DeltaConfig;
import org.apache.fluss.lake.delta.utils.DeltaPartitionLiterals;
import org.apache.fluss.lake.delta.utils.FlussDataTypeToDeltaDataType;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.RowType;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.types.StructField;
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
    private final RowType rowType;
    private final StructType expectedSchema;
    private final List<String> expectedPartitionColumns;

    public DeltaLakeCommitter(Configuration options, CommitterInitContext committerInitContext) {
        this.engine = DeltaConfig.createEngine(options);
        this.tablePath = DeltaConfig.tablePath(options, committerInitContext.tablePath());
        TableInfo tableInfo = committerInitContext.tableInfo();
        this.rowType = tableInfo.getRowType();
        this.expectedSchema = FlussDataTypeToDeltaDataType.toDeltaSchema(rowType);
        this.expectedPartitionColumns = tableInfo.getPartitionKeys();
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
        // A concurrent change of the table fails the Kernel commit below, but a change after the
        // writers loaded the table would go unnoticed without this check.
        checkTableMatchesFluss(snapshot);
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
            Map<String, Literal> partitionLiterals =
                    DeltaPartitionLiterals.toLiterals(entry.getKey(), rowType);
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

        // The value holds the path of a new offsets file per commit, so it changes on every Fluss
        // commit. If the version known to Fluss still has the latest value, no Fluss commit came
        // after it, and the commits of other writers need not be walked.
        long version = latest.getVersion();
        long oldestCandidate = 0;
        if (latestLakeSnapshotIdOfFluss != null) {
            if (version <= latestLakeSnapshotIdOfFluss) {
                return null;
            }
            Optional<Snapshot> known = loadRetainedSnapshot(latestLakeSnapshotIdOfFluss);
            if (known.isPresent()
                    && properties.equals(known.get().getDomainMetadata(FLUSS_DOMAIN))) {
                return null;
            }
            oldestCandidate = latestLakeSnapshotIdOfFluss + 1;
        }

        // Other writers may commit after Fluss, so walk back to the version that set the value.
        // Log cleanup may have removed that version. The walk then stops at the oldest version
        // left, which holds the same offsets and can still be loaded. If the version known to
        // Fluss is gone as well, that may report offsets Fluss already has. Recording them again
        // is harmless, while missing a commit would tier its data twice.
        while (version > oldestCandidate) {
            Optional<Snapshot> previous = loadRetainedSnapshot(version - 1);
            if (!previous.isPresent()
                    || !properties.equals(previous.get().getDomainMetadata(FLUSS_DOMAIN))) {
                break;
            }
            version--;
        }
        return new CommittedLakeSnapshot(
                version, OBJECT_MAPPER.readValue(properties.get(), PROPERTIES_TYPE));
    }

    /**
     * Fails the round if another engine changed the schema or the partition columns of the Delta
     * table. The data files of the round follow the Fluss table, so adding them would leave files
     * that don't match the table.
     */
    private void checkTableMatchesFluss(Snapshot snapshot) {
        StructType schema = snapshot.getSchema();
        if (!schema.equals(expectedSchema)) {
            throw new IllegalStateException(
                    String.format(
                            "The schema of Delta table %s no longer matches the Fluss table: %s. "
                                    + "Tiering to Delta Lake doesn't support schema changes yet.",
                            tablePath, schemaDifference(schema, expectedSchema)));
        }
        List<String> partitionColumns = snapshot.getPartitionColumnNames();
        if (!partitionColumns.equals(expectedPartitionColumns)) {
            throw new IllegalStateException(
                    String.format(
                            "Delta table %s is partitioned by %s, but the Fluss table is "
                                    + "partitioned by %s.",
                            tablePath, partitionColumns, expectedPartitionColumns));
        }
    }

    /** Describes how the schema of the Delta table differs from the one of the Fluss table. */
    static String schemaDifference(StructType delta, StructType fluss) {
        List<String> differences = new ArrayList<>();
        for (StructField field : fluss.fields()) {
            int index = delta.indexOf(field.getName());
            if (index < 0) {
                differences.add(String.format("column '%s' is missing", field.getName()));
            } else if (!delta.at(index).equals(field)) {
                differences.add(
                        String.format(
                                "column '%s' is %s, expected %s",
                                field.getName(), describe(delta.at(index)), describe(field)));
            }
        }
        for (StructField field : delta.fields()) {
            if (fluss.indexOf(field.getName()) < 0) {
                differences.add(String.format("column '%s' is unexpected", field.getName()));
            }
        }
        if (differences.isEmpty()) {
            differences.add(
                    String.format(
                            "columns are ordered %s, expected %s",
                            delta.fieldNames(), fluss.fieldNames()));
        }
        return String.join("; ", differences);
    }

    private static String describe(StructField field) {
        String metadata =
                field.getMetadata().getEntries().isEmpty() ? "" : " " + field.getMetadata();
        return field.getDataType() + (field.isNullable() ? "" : " NOT NULL") + metadata;
    }

    /** Loads the snapshot at the given version, or empty if the log no longer holds it. */
    private Optional<Snapshot> loadRetainedSnapshot(long version) {
        try {
            return Optional.of(
                    TableManager.loadSnapshot(tablePath).atVersion(version).build(engine));
        } catch (KernelException e) {
            // Kernel fails this way when log cleanup or a manual deletion removed the files that
            // rebuild the version. Engine I/O errors are KernelEngineException and still fail.
            LOG.info(
                    "Version {} of Delta table {} is no longer in the log: {}",
                    version,
                    tablePath,
                    e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public void close() {}
}
