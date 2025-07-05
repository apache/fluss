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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.concurrent.Executors;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.server.kv.snapshot.SnapshotsCleaner}. */
class SnapshotsCleanerTest {

    @Test
    void testNotCleanSnapshotInUse() {
        TableBucket tableBucket = new TableBucket(1, 1);
        SnapshotsCleaner snapshotsCleaner = new SnapshotsCleaner();
        TestCompletedSnapshot cp1 = createSnapshot(tableBucket, 1);
        snapshotsCleaner.addSubsumedSnapshot(cp1);
        TestCompletedSnapshot cp2 = createSnapshot(tableBucket, 2);
        snapshotsCleaner.addSubsumedSnapshot(cp2);
        TestCompletedSnapshot cp3 = createSnapshot(tableBucket, 2);
        snapshotsCleaner.addSubsumedSnapshot(cp3);

        snapshotsCleaner.cleanSubsumedSnapshots(
                3, Collections.singleton(1L), () -> {}, Executors.directExecutor());
        // cp 1 is in use, shouldn't discard.
        assertThat(cp1.isDiscarded()).isFalse();
        assertThat(cp2.isDiscarded()).isTrue();
    }

    @Test
    void testNotCleanHigherSnapshot() {
        TableBucket tableBucket = new TableBucket(1, 1);
        SnapshotsCleaner snapshotsCleaner = new SnapshotsCleaner();

        TestCompletedSnapshot cp1 = createSnapshot(tableBucket, 1);
        snapshotsCleaner.addSubsumedSnapshot(cp1);
        TestCompletedSnapshot cp2 = createSnapshot(tableBucket, 2);
        snapshotsCleaner.addSubsumedSnapshot(cp2);
        TestCompletedSnapshot cp3 = createSnapshot(tableBucket, 3);
        snapshotsCleaner.addSubsumedSnapshot(cp3);
        snapshotsCleaner.cleanSubsumedSnapshots(
                2, Collections.emptySet(), () -> {}, Executors.directExecutor());

        assertThat(cp1.isDiscarded()).isTrue();
        // cp2 is the lowest snapshot that is still valid, shouldn't discard.
        assertThat(cp2.isDiscarded()).isFalse();
        // cp3 is higher than cp2, shouldn't discard.
        assertThat(cp3.isDiscarded()).isFalse();
    }

    public static class TestCompletedSnapshot extends CompletedSnapshot {

        private boolean isDiscarded;

        public TestCompletedSnapshot(
                TableBucket tableBucket,
                long snapshotID,
                FsPath snapshotLocation,
                TestKvSnapshotHandle kvSnapshotHandle) {
            super(tableBucket, snapshotID, snapshotLocation, kvSnapshotHandle);
        }

        public CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            CompletableFuture<Void> resultFuture = new CompletableFuture<>();
            super.discardAsync(ioExecutor)
                    .whenComplete(
                            (ignore, throwable) -> {
                                if (throwable != null) {
                                    resultFuture.completeExceptionally(
                                            new FlussRuntimeException(
                                                    "Fail to discard TestCompletedSnapshot.",
                                                    throwable));
                                } else {
                                    isDiscarded = true;
                                    resultFuture.complete(null);
                                }
                            });
            return resultFuture;
        }

        public boolean isDiscarded() {
            return isDiscarded;
        }
    }

    private static class TestKvSnapshotHandle extends KvSnapshotHandle {

        private boolean isDiscarded;

        public TestKvSnapshotHandle(
                List<KvFileHandleAndLocalPath> sharedFileHandles,
                List<KvFileHandleAndLocalPath> privateFileHandles) {
            super(sharedFileHandles, privateFileHandles, -1);
        }

        @Override
        public void discard() {
            isDiscarded = true;
        }

        public boolean isDiscarded() {
            return isDiscarded;
        }
    }

    public static void verifySnapshotDiscarded(TestCompletedSnapshot completedSnapshot) {
        assertThat(completedSnapshot.isDiscarded()).isTrue();
        verifySnapshotDiscarded((TestKvSnapshotHandle) completedSnapshot.getKvSnapshotHandle());
    }

    public static void verifySnapshotDiscarded(TestKvSnapshotHandle testKvSnapshotHandle) {
        assertThat(testKvSnapshotHandle.isDiscarded()).isTrue();
    }

    public static TestCompletedSnapshot createSnapshot(TableBucket tableBucket, long snapshotId) {
        return createSnapshot(tableBucket, snapshotId, new FsPath("testpath"));
    }

    public static TestCompletedSnapshot createSnapshot(
            TableBucket tableBucket, long snapshotId, @Nullable FsPath snapshotPath) {

        List<KvFileHandleAndLocalPath> sharedFileHandles = new ArrayList<>();
        List<KvFileHandleAndLocalPath> privateFileHandles = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            sharedFileHandles.add(
                    KvFileHandleAndLocalPath.of(
                            new KvFileHandle("share_remote_path", 1), "share_local_path"));
            privateFileHandles.add(
                    KvFileHandleAndLocalPath.of(
                            new KvFileHandle("private_remote_path", 1), "private_local_path"));
        }

        TestKvSnapshotHandle kvSnapshotHandle =
                new TestKvSnapshotHandle(sharedFileHandles, privateFileHandles);
        return new TestCompletedSnapshot(tableBucket, snapshotId, snapshotPath, kvSnapshotHandle);
    }
}
