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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.exception.InvalidBulkLoadSnapshotException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadata.FileHandle;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Validates physical identity, the fixed recovery boundary, and standard file descriptors. */
class BulkLoadSnapshotValidatorTest {
    @TempDir Path root;
    private TableInfo table;
    private BulkLoadTarget target;
    private FsPath directory;
    private final TableBucket bucket = new TableBucket(1, 0);
    private final BulkLoadBucket allocation = new BulkLoadBucket(7, 100, BulkLoadBucket.ALLOCATED);

    @BeforeEach
    void setUp() throws Exception {
        String remote = root.toUri().toString();
        table =
                TableInfo.of(
                        TablePath.of("db", "t"),
                        1,
                        1,
                        TableDescriptor.builder()
                                .schema(
                                        Schema.newBuilder()
                                                .column("k", DataTypes.INT())
                                                .primaryKey("k")
                                                .build())
                                .distributedBy(1, "k")
                                .build(),
                        remote,
                        1,
                        1);
        target = new BulkLoadTarget(null, null, 1, remote, false);
        directory =
                FlussPaths.remoteKvSnapshotDir(
                        FlussPaths.remoteKvTabletDir(
                                new FsPath(remote, FlussPaths.REMOTE_KV_DIR_NAME),
                                PhysicalTablePath.of(table.getTablePath()),
                                bucket),
                        allocation.snapshotId);
        Files.createDirectories(Paths.get(directory.toUri()));
    }

    @Test
    void testEmptySnapshotIsAccepted() throws Exception {
        write(bucket, 100, 0L, Collections.emptyList(), 0);
        assertThat(
                        BulkLoadSnapshotValidator.validate(
                                        table.getTablePath(),
                                        table.getTableConfig(),
                                        target,
                                        bucket,
                                        allocation)
                                .getSnapshotID())
                .isEqualTo(7);
    }

    @Test
    void testNegativeRowCountIsRejected() throws Exception {
        write(bucket, 100, -1L, Collections.emptyList(), 0);
        assertThatThrownBy(
                        () ->
                                BulkLoadSnapshotValidator.validate(
                                        table.getTablePath(),
                                        table.getTableConfig(),
                                        target,
                                        bucket,
                                        allocation))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("Negative snapshot row count");
    }

    @Test
    void testMissingMetadataIsRejected() {
        assertThatThrownBy(
                        () ->
                                BulkLoadSnapshotValidator.validate(
                                        table.getTablePath(),
                                        table.getTableConfig(),
                                        target,
                                        bucket,
                                        allocation))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class);
    }

    @Test
    void testOrdinarySnapshotIsValidatedWithoutRewritingInput() throws Exception {
        FileHandle file = file("000001.sst", new byte[] {1, 2});
        write(bucket, 100, file);
        byte[] original = Files.readAllBytes(metadataPath());
        BulkLoadSnapshotValidator.validate(
                table.getTablePath(), table.getTableConfig(), target, bucket, allocation);
        assertThat(Files.readAllBytes(metadataPath())).isEqualTo(original);
    }

    @Test
    void testWrongPhysicalIdentityOrRecoveryOffsetIsRejected() throws Exception {
        FileHandle file = file("000001.sst", new byte[] {1});
        write(new TableBucket(2, 0), 100, file);
        assertThatThrownBy(
                        () ->
                                BulkLoadSnapshotValidator.validate(
                                        table.getTablePath(),
                                        table.getTableConfig(),
                                        target,
                                        bucket,
                                        allocation))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("physical identity");
        write(bucket, 99, file);
        assertThatThrownBy(
                        () ->
                                BulkLoadSnapshotValidator.validate(
                                        table.getTablePath(),
                                        table.getTableConfig(),
                                        target,
                                        bucket,
                                        allocation))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("recovery offset");
    }

    @Test
    void testTruncatedFileAndUnsafeLocalPathAreRejected() throws Exception {
        FileHandle file = file("000001.sst", new byte[] {1});
        write(bucket, 100, new FileHandle(file.getPath(), 2, "000001.sst"));
        assertThatThrownBy(
                        () ->
                                BulkLoadSnapshotValidator.validate(
                                        table.getTablePath(),
                                        table.getTableConfig(),
                                        target,
                                        bucket,
                                        allocation))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("length");
        write(bucket, 100, new FileHandle(file.getPath(), 1, "../000001.sst"));
        assertThatThrownBy(
                        () ->
                                BulkLoadSnapshotValidator.validate(
                                        table.getTablePath(),
                                        table.getTableConfig(),
                                        target,
                                        bucket,
                                        allocation))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class)
                .hasMessageContaining("Unsafe");
    }

    @Test
    void testMalformedMetadataHasSnapshotInputError() throws Exception {
        Files.write(metadataPath(), new byte[] {0});
        assertThatThrownBy(
                        () ->
                                BulkLoadSnapshotValidator.validate(
                                        table.getTablePath(),
                                        table.getTableConfig(),
                                        target,
                                        bucket,
                                        allocation))
                .isInstanceOf(InvalidBulkLoadSnapshotException.class);
    }

    private FileHandle file(String name, byte[] bytes) throws Exception {
        FsPath path = new FsPath(directory, name);
        Files.write(Paths.get(path.toUri()), bytes);
        return new FileHandle(path.toString(), bytes.length, name);
    }

    private void write(TableBucket physical, long offset, FileHandle file) throws Exception {
        write(physical, offset, 1L, Collections.singletonList(file), file.getSize());
    }

    private void write(
            TableBucket physical, long offset, long rowCount, List<FileHandle> files, long size)
            throws Exception {
        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        physical,
                        7,
                        directory.toString(),
                        Collections.emptyList(),
                        files,
                        size,
                        offset,
                        rowCount,
                        null);
        Files.write(metadataPath(), KvSnapshotFileMetadataJsonSerde.toJson(metadata));
    }

    private Path metadataPath() {
        return Paths.get(new FsPath(directory, "_METADATA").toUri());
    }
}
