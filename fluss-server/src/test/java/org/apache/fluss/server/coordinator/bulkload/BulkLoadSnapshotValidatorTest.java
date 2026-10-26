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

import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.shaded.guava32.com.google.common.hash.HashCode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.server.testutils.KvTestUtils.writeSnapshotMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Complete, immutable inputs and file validation before the durable Commit decision. */
class BulkLoadSnapshotValidatorTest {
    @TempDir private Path tempDir;
    private BulkLoadHandle handle;
    private List<FsPath> metadata;
    private String inputSha256;

    @BeforeEach
    void prepareSnapshots() throws Exception {
        handle =
                new BulkLoadHandle(
                        PhysicalTablePath.of("db", "table", null),
                        41L,
                        null,
                        "550e8400-e29b-41d4-a716-446655440000");
        metadata =
                Arrays.asList(
                        writeSnapshotMetadata(
                                tempDir.toUri().toString(),
                                handle,
                                new TableBucket(handle.getTableId(), 0),
                                17L,
                                0L,
                                0L),
                        writeSnapshotMetadata(
                                tempDir.toUri().toString(),
                                handle,
                                new TableBucket(handle.getTableId(), 1),
                                18L,
                                2L,
                                2L));
        inputSha256 = digest(metadata);
    }

    @Test
    void testValidEmptyAndNonEmptyBuckets() {
        List<CompletedSnapshot> parsed = validate(inputSha256, 1024 * 1024);
        assertThat(parsed)
                .extracting(CompletedSnapshot::getTableBucket)
                .extracting(TableBucket::getBucket)
                .containsExactly(0, 1);
        assertThat(parsed).extracting(CompletedSnapshot::getSnapshotID).containsExactly(17L, 18L);
        assertThat(parsed).extracting(CompletedSnapshot::getLogOffset).containsExactly(0L, 2L);
    }

    @Test
    void testDigestBindsBucketOrderAndMetadataContent() throws Exception {
        assertThatThrownBy(
                        () ->
                                validate(
                                        digest(Arrays.asList(metadata.get(1), metadata.get(0))),
                                        1024 * 1024))
                .isInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("SHA-256");
        Path first = Paths.get(metadata.get(0).toUri());
        Files.write(
                first,
                (new String(Files.readAllBytes(first), StandardCharsets.UTF_8) + " ")
                        .getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> validate(inputSha256, 1024 * 1024))
                .isInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("SHA-256");
    }

    @Test
    void testRejectsMissingBucketAndTruncatedDataFile() throws Exception {
        Path first = Paths.get(metadata.get(0).toUri());
        Files.write(first.resolveSibling("file.sst"), new byte[0]);
        assertThatThrownBy(() -> validate(inputSha256, 1024 * 1024))
                .isInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("length differs");
        Files.write(first.resolveSibling("file.sst"), new byte[] {0});
        Files.delete(Paths.get(metadata.get(1).toUri()));
        assertThatThrownBy(() -> validate(inputSha256, 1024 * 1024))
                .isInstanceOf(InvalidBulkLoadRequestException.class);
    }

    @Test
    void testRejectsUnsafeLocalPath() throws Exception {
        Path first = Paths.get(metadata.get(0).toUri());
        String json = new String(Files.readAllBytes(first), StandardCharsets.UTF_8);
        assertThat(json).contains("\"local_path\":\"file.sst\"");
        Files.write(
                first,
                json.replace("\"local_path\":\"file.sst\"", "\"local_path\":\"a//b\"")
                        .getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> validate(digest(metadata), 1024 * 1024))
                .isInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("local path");
    }

    @Test
    void testRejectsOversizedSnapshotMetadataBeforeReadingContent() throws Exception {
        Files.write(Paths.get(metadata.get(0).toUri()), new byte[1025]);
        assertThatThrownBy(() -> validate(inputSha256, 1024))
                .isInstanceOf(InvalidBulkLoadRequestException.class)
                .hasMessageContaining("Snapshot metadata exceeds its byte limit");
    }

    private List<CompletedSnapshot> validate(String digest, long limit) {
        return new BulkLoadSnapshotValidator()
                .validate(
                        handle,
                        tempDir.toUri().toString(),
                        digest,
                        new long[] {17L, 18L},
                        ChangelogImage.FULL,
                        limit);
    }

    private static String digest(List<FsPath> paths) throws Exception {
        MessageDigest combined = MessageDigest.getInstance("SHA-256");
        for (FsPath path : paths) {
            combined.update(
                    MessageDigest.getInstance("SHA-256")
                            .digest(Files.readAllBytes(Paths.get(path.toUri()))));
        }
        return HashCode.fromBytes(combined.digest()).toString();
    }
}
