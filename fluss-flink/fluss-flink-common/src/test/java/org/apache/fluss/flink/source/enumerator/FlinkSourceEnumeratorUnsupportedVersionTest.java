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

package org.apache.fluss.flink.source.enumerator;

import org.apache.fluss.client.admin.KvSnapshotLease;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.AcquireKvSnapshotLeaseResult;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
import org.apache.fluss.flink.source.reader.LeaseContext;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for handling UnsupportedVersionException in FlinkSourceEnumerator. */
class FlinkSourceEnumeratorUnsupportedVersionTest {

    private static final TablePath TEST_TABLE_PATH =
            TablePath.of("test_db", "test_table_unsupported_version");

    /**
     * Tests that getLatestKvSnapshotsAndRegister handles UnsupportedVersionException gracefully
     * when server doesn't support ACQUIRE_KV_SNAPSHOT_LEASE API.
     */
    @Test
    void testGetLatestKvSnapshotsAndRegisterWithUnsupportedVersionException() throws Exception {
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            FlinkSourceEnumerator enumerator = createTestEnumerator(context);

            // Use an admin that returns valid snapshots but fails on acquireSnapshots
            setAdminField(enumerator, new UnsupportedVersionAdminWithSnapshots());

            // Call getLatestKvSnapshotsAndRegister via reflection
            Method method =
                    FlinkSourceEnumerator.class.getDeclaredMethod(
                            "getLatestKvSnapshotsAndRegister", String.class);
            method.setAccessible(true);

            // Should not throw, just log warning and return valid snapshots
            KvSnapshots result = (KvSnapshots) method.invoke(enumerator, (String) null);
            assertThat(result).isNotNull();
            assertThat(result.getTableId()).isEqualTo(1L);
            assertThat(result.getBucketIds()).containsExactly(0);
        }
    }

    /**
     * Tests that notifyCheckpointComplete handles UnsupportedVersionException gracefully when
     * server doesn't support RELEASE_KV_SNAPSHOT_LEASE API.
     */
    @Test
    void testNotifyCheckpointCompleteWithUnsupportedVersionException() throws Exception {
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            FlinkSourceEnumerator enumerator = createTestEnumerator(context);

            // Set admin via reflection (avoid start() which needs cluster connection)
            setAdminField(enumerator, new UnsupportedVersionAdmin());

            // Add a consumed bucket to trigger release
            TableBucket bucket = new TableBucket(1L, 0);
            enumerator.addConsumedBucket(1L, bucket);

            // Should not throw exception, just log warning
            assertThatCode(() -> enumerator.notifyCheckpointComplete(1L))
                    .doesNotThrowAnyException();
        }
    }

    /**
     * Tests that maybeDropKvSnapshotLease handles UnsupportedVersionException gracefully when
     * server doesn't support DROP_KV_SNAPSHOT_LEASE API.
     */
    @Test
    void testMaybeDropKvSnapshotLeaseWithUnsupportedVersionException() throws Exception {
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            FlinkSourceEnumerator enumerator =
                    new FlinkSourceEnumerator(
                            TEST_TABLE_PATH,
                            new Configuration(),
                            true,
                            false,
                            context,
                            OffsetsInitializer.full(),
                            0L,
                            true,
                            null,
                            null,
                            LeaseContext.DEFAULT,
                            false);

            // Set admin via reflection (avoid start() which needs cluster connection)
            setAdminField(enumerator, new UnsupportedVersionAdmin());

            // Should not throw exception, just log warning
            assertThatCode(enumerator::close).doesNotThrowAnyException();
        }
    }

    /**
     * Tests that other exceptions in releaseSnapshots still cause re-enqueue of consumed buckets.
     */
    @Test
    void testNotifyCheckpointCompleteWithOtherException() throws Exception {
        try (MockSplitEnumeratorContext<SourceSplitBase> context =
                new MockSplitEnumeratorContext<>(1)) {
            FlinkSourceEnumerator enumerator = createTestEnumerator(context);

            // Set admin via reflection (avoid start() which needs cluster connection)
            setAdminField(enumerator, new FailingAdmin(new RuntimeException("Network error")));

            // Add a consumed bucket to trigger release
            TableBucket bucket = new TableBucket(1L, 0);
            enumerator.addConsumedBucket(1L, bucket);

            // Should not throw exception but should re-enqueue the bucket
            assertThatCode(() -> enumerator.notifyCheckpointComplete(1L))
                    .doesNotThrowAnyException();

            // Verify bucket was re-enqueued (checkpoint 1L should have the bucket)
            Set<TableBucket> reenqueuedBuckets = enumerator.getAndRemoveConsumedBucketsUpTo(2L);
            assertThat(reenqueuedBuckets).contains(bucket);
        }
    }

    private FlinkSourceEnumerator createTestEnumerator(
            MockSplitEnumeratorContext<SourceSplitBase> context) {
        return new FlinkSourceEnumerator(
                TEST_TABLE_PATH,
                new Configuration(),
                true,
                false,
                context,
                OffsetsInitializer.full(),
                0L,
                true,
                null,
                null,
                LeaseContext.DEFAULT,
                true);
    }

    private void setAdminField(FlinkSourceEnumerator enumerator, TestAdminAdapter admin)
            throws Exception {
        Field adminField = FlinkSourceEnumerator.class.getDeclaredField("flussAdmin");
        adminField.setAccessible(true);
        adminField.set(enumerator, admin);
    }

    // -------------------------------------------------------------------------
    //  Test Admin implementations
    // -------------------------------------------------------------------------

    /**
     * A test Admin that returns a {@link KvSnapshotLease} whose lease operations all fail with
     * {@link UnsupportedVersionException}, simulating an old Fluss server that does not support the
     * kv snapshot lease API.
     */
    private static class UnsupportedVersionAdmin extends TestAdminAdapter {
        @Override
        public KvSnapshotLease createKvSnapshotLease(String leaseId, long leaseDurationMs) {
            return new UnsupportedVersionKvSnapshotLease(leaseId, leaseDurationMs);
        }
    }

    /**
     * Extends {@link UnsupportedVersionAdmin} by additionally supporting {@code
     * getLatestKvSnapshots}, so that the {@code getLatestKvSnapshotsAndRegister} code path can
     * reach the {@code acquireSnapshots} call.
     */
    private static class UnsupportedVersionAdminWithSnapshots extends UnsupportedVersionAdmin {
        @Override
        public CompletableFuture<KvSnapshots> getLatestKvSnapshots(TablePath tablePath) {
            Map<Integer, Long> snapshotIds = new HashMap<>();
            snapshotIds.put(0, 1L);
            Map<Integer, Long> logOffsets = new HashMap<>();
            logOffsets.put(0, 0L);
            return CompletableFuture.completedFuture(
                    new KvSnapshots(1L, null, snapshotIds, logOffsets));
        }
    }

    /**
     * A test Admin that returns a {@link KvSnapshotLease} whose lease operations all fail with the
     * given exception, used to verify non-{@link UnsupportedVersionException} error handling.
     */
    private static class FailingAdmin extends TestAdminAdapter {
        private final Exception cause;

        FailingAdmin(Exception cause) {
            this.cause = cause;
        }

        @Override
        public KvSnapshotLease createKvSnapshotLease(String leaseId, long leaseDurationMs) {
            return new FailingKvSnapshotLease(leaseId, leaseDurationMs, cause);
        }
    }

    /** A {@link KvSnapshotLease} that fails all operations with UnsupportedVersionException. */
    private static class UnsupportedVersionKvSnapshotLease implements KvSnapshotLease {
        private final String leaseId;
        private final long leaseDurationMs;

        UnsupportedVersionKvSnapshotLease(String leaseId, long leaseDurationMs) {
            this.leaseId = leaseId;
            this.leaseDurationMs = leaseDurationMs;
        }

        @Override
        public String leaseId() {
            return leaseId;
        }

        @Override
        public long leaseDurationMs() {
            return leaseDurationMs;
        }

        @Override
        public CompletableFuture<AcquireKvSnapshotLeaseResult> acquireSnapshots(
                Map<TableBucket, Long> snapshotIds) {
            return failedFuture(
                    new UnsupportedVersionException(
                            "The server does not support ACQUIRE_KV_SNAPSHOT_LEASE(1056)"));
        }

        @Override
        public CompletableFuture<Void> renew() {
            return failedFuture(
                    new UnsupportedVersionException(
                            "The server does not support RENEW_KV_SNAPSHOT_LEASE"));
        }

        @Override
        public CompletableFuture<Void> releaseSnapshots(Set<TableBucket> bucketsToRelease) {
            return failedFuture(
                    new UnsupportedVersionException(
                            "The server does not support RELEASE_KV_SNAPSHOT_LEASE(1057)"));
        }

        @Override
        public CompletableFuture<Void> dropLease() {
            return failedFuture(
                    new UnsupportedVersionException(
                            "The server does not support DROP_KV_SNAPSHOT_LEASE(1058)"));
        }
    }

    /** A {@link KvSnapshotLease} that fails all operations with the given exception. */
    private static class FailingKvSnapshotLease implements KvSnapshotLease {
        private final String leaseId;
        private final long leaseDurationMs;
        private final Exception cause;

        FailingKvSnapshotLease(String leaseId, long leaseDurationMs, Exception cause) {
            this.leaseId = leaseId;
            this.leaseDurationMs = leaseDurationMs;
            this.cause = cause;
        }

        @Override
        public String leaseId() {
            return leaseId;
        }

        @Override
        public long leaseDurationMs() {
            return leaseDurationMs;
        }

        @Override
        public CompletableFuture<AcquireKvSnapshotLeaseResult> acquireSnapshots(
                Map<TableBucket, Long> snapshotIds) {
            return failedFuture(cause);
        }

        @Override
        public CompletableFuture<Void> renew() {
            return failedFuture(cause);
        }

        @Override
        public CompletableFuture<Void> releaseSnapshots(Set<TableBucket> bucketsToRelease) {
            return failedFuture(cause);
        }

        @Override
        public CompletableFuture<Void> dropLease() {
            return failedFuture(cause);
        }
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }
}
