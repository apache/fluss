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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.flink.action.orphan.audit.AuditLogger;
import org.apache.fluss.flink.action.orphan.fs.SafeDeleter;
import org.apache.fluss.flink.action.orphan.rule.BucketActiveRefs;
import org.apache.fluss.flink.action.orphan.rule.RuleDispatcher;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.RateLimiter;
import org.apache.fluss.utils.FlussPaths;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class BucketCleanerTest {

    @Test
    void removesOldEmptySegmentDirAfterDeletingExpiredFiles(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path segmentDir =
                Files.createDirectories(bucketRoot.resolve("11111111-1111-1111-1111-111111111111"));
        Path logFile =
                Files.write(
                        segmentDir.resolve(
                                FlussPaths.filenamePrefixFromOffset(0L)
                                        + FlussPaths.LOG_FILE_SUFFIX),
                        new byte[] {0x42});
        long cutoff = System.currentTimeMillis() - 1000L;
        makeOld(logFile, cutoff - 1000L);
        makeOld(segmentDir, cutoff - 1000L);
        makeOld(bucketRoot, cutoff - 1000L);

        BucketCleaner cleaner = createCleaner(bucketRoot, cutoff);

        BucketCleaner.BucketCleanStats stats =
                cleaner.clean(BucketActiveRefs.empty(), new FsPath(bucketRoot.toString()));

        assertThat(stats.scanned).isEqualTo(1L);
        assertThat(stats.deleted).isEqualTo(2L);
        assertThat(stats.emptyDirsRemoved).isEqualTo(1L);
        assertThat(Files.exists(logFile)).isFalse();
        assertThat(Files.exists(segmentDir)).isFalse();
        assertThat(Files.exists(bucketRoot)).isTrue();
    }

    @Test
    void keepsFreshEmptySegmentDir(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path segmentDir =
                Files.createDirectories(bucketRoot.resolve("11111111-1111-1111-1111-111111111111"));
        long cutoff = System.currentTimeMillis() - 1000L;

        BucketCleaner cleaner = createCleaner(bucketRoot, cutoff);

        BucketCleaner.BucketCleanStats stats =
                cleaner.clean(
                        new BucketActiveRefs(
                                Collections.<String>emptySet(),
                                Collections.<String>emptySet(),
                                Collections.<String>emptySet()),
                        new FsPath(bucketRoot.toString()));

        assertThat(stats.deleted).isEqualTo(0L);
        assertThat(stats.emptyDirsRemoved).isEqualTo(0L);
        assertThat(Files.exists(segmentDir)).isTrue();
    }

    @Test
    void scansButDoesNotDeleteUnknownDotFiles(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path segmentDir =
                Files.createDirectories(bucketRoot.resolve("11111111-1111-1111-1111-111111111111"));
        Path dotFile = Files.write(segmentDir.resolve(".unknown"), new byte[] {0x42});
        long cutoff = System.currentTimeMillis() - 1000L;
        makeOld(dotFile, cutoff - 1000L);
        makeOld(segmentDir, cutoff - 1000L);
        makeOld(bucketRoot, cutoff - 1000L);

        BucketCleaner cleaner = createCleaner(bucketRoot, cutoff);

        BucketCleaner.BucketCleanStats stats =
                cleaner.clean(BucketActiveRefs.empty(), new FsPath(bucketRoot.toString()));

        assertThat(stats.scanned).isEqualTo(1L);
        assertThat(stats.deleted).isEqualTo(0L);
        assertThat(stats.emptyDirsRemoved).isEqualTo(0L);
        assertThat(Files.exists(dotFile)).isTrue();
        assertThat(Files.exists(segmentDir)).isTrue();
    }

    @Test
    void scansSharedDirectoryAndDeletesOrphanSst(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path sharedDir = Files.createDirectories(bucketRoot.resolve("shared"));
        Path activeSst = Files.write(sharedDir.resolve("active.sst"), new byte[] {0x42});
        Path orphanSst = Files.write(sharedDir.resolve("orphan.sst"), new byte[] {0x42});
        long cutoff = System.currentTimeMillis() - 1000L;
        makeOld(activeSst, cutoff - 1000L);
        makeOld(orphanSst, cutoff - 1000L);
        makeOld(sharedDir, cutoff - 1000L);
        makeOld(bucketRoot, cutoff - 1000L);

        Set<String> activeSharedSstFiles = new HashSet<>(Arrays.asList("active.sst"));
        BucketActiveRefs activeRefs =
                new BucketActiveRefs(
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet(),
                        activeSharedSstFiles);

        BucketCleaner cleaner = createCleaner(bucketRoot, cutoff);
        BucketCleaner.BucketCleanStats stats =
                cleaner.clean(activeRefs, new FsPath(bucketRoot.toString()));

        assertThat(stats.scanned).isEqualTo(2L);
        // orphan.sst deleted; active.sst kept
        assertThat(stats.deleted).isGreaterThanOrEqualTo(1L);
        assertThat(Files.exists(activeSst)).isTrue();
        assertThat(Files.exists(orphanSst)).isFalse();
    }

    @Test
    void keepsAllSharedSstWhenActiveSetIsEmpty(@TempDir Path tmp) throws IOException {
        Path bucketRoot = Files.createDirectories(tmp.resolve("bucket"));
        Path sharedDir = Files.createDirectories(bucketRoot.resolve("shared"));
        Path sst1 = Files.write(sharedDir.resolve("file1.sst"), new byte[] {0x42});
        Path sst2 = Files.write(sharedDir.resolve("file2.sst"), new byte[] {0x42});
        long cutoff = System.currentTimeMillis() - 1000L;
        makeOld(sst1, cutoff - 1000L);
        makeOld(sst2, cutoff - 1000L);
        makeOld(sharedDir, cutoff - 1000L);
        makeOld(bucketRoot, cutoff - 1000L);

        // Empty active set = conservative keep-all behavior
        BucketCleaner cleaner = createCleaner(bucketRoot, cutoff);
        BucketCleaner.BucketCleanStats stats =
                cleaner.clean(BucketActiveRefs.empty(), new FsPath(bucketRoot.toString()));

        // Both files kept (KEEP_ACTIVE due to empty active set)
        assertThat(Files.exists(sst1)).isTrue();
        assertThat(Files.exists(sst2)).isTrue();
    }

    private static void makeOld(Path path, long timestampMillis) throws IOException {
        Files.setLastModifiedTime(path, FileTime.fromMillis(timestampMillis));
    }

    private static BucketCleaner createCleaner(Path bucketRoot, long cutoff) throws IOException {
        RateLimiter remoteFsOpRateLimiter = RateLimiter.create(1000.0);
        return new BucketCleaner(
                new RuleDispatcher(),
                new SafeDeleter(
                        new FsPath(bucketRoot.toString()).getFileSystem(),
                        false,
                        new AuditLogger(),
                        remoteFsOpRateLimiter),
                new AuditLogger(),
                cutoff,
                remoteFsOpRateLimiter);
    }
}
