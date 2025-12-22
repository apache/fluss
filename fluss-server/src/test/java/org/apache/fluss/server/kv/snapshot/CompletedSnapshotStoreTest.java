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

import org.apache.fluss.exception.FlussException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CompletedSnapshotStore}. */
class CompletedSnapshotStoreTest {

    private final long timeout = 100L;

    private ExecutorService executorService;
    private TestCompletedSnapshotHandleStore.Builder builder;
    private TestCompletedSnapshotHandleStore defaultHandleStore;
    private @TempDir Path tempDir;

    @BeforeEach
    void setup() {
        builder = TestCompletedSnapshotHandleStore.newBuilder();
        defaultHandleStore = builder.build();
        executorService = Executors.newFixedThreadPool(2, new ExecutorThreadFactory("IO-Executor"));
    }

    @AfterEach
    void after() {
        executorService.shutdown();
    }

    @Test
    void testAtLeastOneSnapshotRetained() throws Exception {
        CompletedSnapshot cp1 = getSnapshot(1L);
        CompletedSnapshot cp2 = getSnapshot(2L);
        CompletedSnapshot cp3 = getSnapshot(3L);
        testSnapshotRetention(1, asList(cp1, cp2, cp3), Collections.singletonList(cp3));
    }

    @Test
    void testNotSubsumedIfNotNeeded() throws Exception {
        CompletedSnapshot cp1 = getSnapshot(1L);
        testSnapshotRetention(1, Collections.singletonList(cp1), Collections.singletonList(cp1));
    }

    @Test
    void testRetainSnapshots() throws Exception {
        CompletedSnapshot cp1 = getSnapshot(1L);
        CompletedSnapshot cp2 = getSnapshot(2L);
        CompletedSnapshot cp3 = getSnapshot(3L);

        testSnapshotRetention(2, asList(cp1, cp2, cp3), Arrays.asList(cp2, cp3));

        testSnapshotRetention(3, asList(cp1, cp2, cp3), Arrays.asList(cp1, cp2, cp3));
    }

    @Test
    void testLastSnapshot() throws Exception {
        final TestCompletedSnapshotHandleStore completedSnapshotHandleStore = builder.build();
        CompletedSnapshotStore completedSnapshotStore =
                createCompletedSnapshotStore(
                        1, completedSnapshotHandleStore, Collections.emptyList());
        assertThat(completedSnapshotStore.getLatestSnapshot()).isEmpty();
        CompletedSnapshot snapshot = getSnapshot(1);
        completedSnapshotStore.add(snapshot);
        assertThat(completedSnapshotStore.getLatestSnapshot().get()).isEqualTo(snapshot);
    }

    @Test
    void testAddSnapshotSuccessfullyShouldRemoveOldOnes() throws Exception {
        final int num = 1;
        final CompletableFuture<CompletedSnapshotHandle> addFuture = new CompletableFuture<>();
        List<Tuple2<CompletedSnapshotHandle, String>> snapshotHandles = createSnapshotHandles(num);
        final TestCompletedSnapshotHandleStore completedSnapshotHandleStore =
                builder.setAddFunction(
                                (snapshot) -> {
                                    addFuture.complete(snapshot);
                                    return null;
                                })
                        .build();
        final List<CompletedSnapshot> completedSnapshots = mapToCompletedSnapshot(snapshotHandles);
        final CompletedSnapshotStore completedSnapshotStore =
                createCompletedSnapshotStore(1, completedSnapshotHandleStore, completedSnapshots);

        assertThat(completedSnapshotStore.getAllSnapshots()).hasSize(num);
        assertThat(completedSnapshotStore.getAllSnapshots().get(0).getSnapshotID()).isOne();

        final long ckpId = 100L;
        final CompletedSnapshot ckp = getSnapshot(ckpId);
        completedSnapshotStore.add(ckp);

        // We should persist the completed snapshot to snapshot handle store.
        final CompletedSnapshotHandle addedCkpHandle =
                addFuture.get(timeout, TimeUnit.MILLISECONDS);

        assertThat(addedCkpHandle.retrieveCompleteSnapshot().getSnapshotID()).isEqualTo(ckpId);

        // Check the old snapshot is removed and new one is added.
        assertThat(completedSnapshotStore.getAllSnapshots()).hasSize(num);
        assertThat(completedSnapshotStore.getAllSnapshots().get(0).getSnapshotID())
                .isEqualTo(ckpId);
    }

    @Test
    void testAddSnapshotFailedShouldNotRemoveOldOnes() {
        final int num = 1;
        final String errMsg = "Add to snapshot handle failed.";
        final TestCompletedSnapshotHandleStore handleStore =
                builder.setAddFunction(
                                (ckp) -> {
                                    throw new FlussException(errMsg);
                                })
                        .build();

        List<Tuple2<CompletedSnapshotHandle, String>> snapshotHandles = createSnapshotHandles(num);
        final List<CompletedSnapshot> completedSnapshots = mapToCompletedSnapshot(snapshotHandles);
        final CompletedSnapshotStore completedSnapshotStore =
                createCompletedSnapshotStore(1, handleStore, completedSnapshots);

        assertThat(completedSnapshotStore.getAllSnapshots()).hasSize(num);
        assertThat(completedSnapshotStore.getAllSnapshots().get(0).getSnapshotID()).isOne();

        final long ckpId = 100L;
        final CompletedSnapshot ckp = getSnapshot(ckpId);

        assertThatThrownBy(() -> completedSnapshotStore.add(ckp))
                .as("We should get an exception when add snapshot to failed..")
                .hasMessageContaining(errMsg)
                .isInstanceOf(FlussException.class);

        // Check the old snapshot still exists.
        assertThat(completedSnapshotStore.getAllSnapshots()).hasSize(num);
        assertThat(completedSnapshotStore.getAllSnapshots().get(0).getSnapshotID()).isOne();
        assertThat(completedSnapshotStore.getLatestSnapshot().get().getSnapshotID()).isOne();
    }

    @Test
    void testConcurrentAdds() throws Exception {
        final CompletedSnapshotStore completedSnapshotStore =
                createCompletedSnapshotStore(10, defaultHandleStore, Collections.emptyList());

        final int numThreads = 10;
        final int snapshotsPerThread = 5;
        final ExecutorService testExecutor =
                Executors.newFixedThreadPool(
                        numThreads, new ExecutorThreadFactory("concurrent-add-thread"));

        try {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(numThreads);
            AtomicInteger exceptionCount = new AtomicInteger(0);

            // Spin up threads to add snapshots concurrently
            for (int threadId = 0; threadId < numThreads; threadId++) {
                final int finalThreadId = threadId;
                testExecutor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                for (int i = 0; i < snapshotsPerThread; i++) {
                                    long snapshotId =
                                            (long) finalThreadId * snapshotsPerThread + i + 1;
                                    CompletedSnapshot snapshot = getSnapshot(snapshotId);
                                    completedSnapshotStore.add(snapshot);
                                }
                            } catch (Exception e) {
                                exceptionCount.incrementAndGet();
                            } finally {
                                completionLatch.countDown();
                            }
                        });
            }

            // Start all threads simultaneously
            startLatch.countDown();
            boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
            assertThat(completed).as("All threads should complete").isTrue();

            // Ensure time for async cleanup to finish
            Thread.sleep(100);

            assertThat(exceptionCount.get()).as("No exceptions should occur").isEqualTo(0);

            List<CompletedSnapshot> allSnapshots = completedSnapshotStore.getAllSnapshots();
            assertThat(allSnapshots.size())
                    .as("Should retain at most maxNumberOfSnapshotsToRetain snapshots")
                    .isLessThanOrEqualTo(10);

            Set<Long> snapshotIds = new HashSet<>();
            for (CompletedSnapshot snapshot : allSnapshots) {
                assertThat(snapshotIds.add(snapshot.getSnapshotID()))
                        .as("Snapshot IDs should be unique (no corruption)")
                        .isTrue();
            }

            long numSnapshots = completedSnapshotStore.getNumSnapshots();
            assertThat(numSnapshots)
                    .as("getNumSnapshots() should match getAllSnapshots().size()")
                    .isEqualTo(allSnapshots.size());

            if (!allSnapshots.isEmpty()) {
                Optional<CompletedSnapshot> latest = completedSnapshotStore.getLatestSnapshot();
                assertThat(latest).as("Latest snapshot should be present").isPresent();
                assertThat(latest.get())
                        .as("Latest snapshot should match last in getAllSnapshots()")
                        .isEqualTo(allSnapshots.get(allSnapshots.size() - 1));
            }
        } finally {
            testExecutor.shutdown();
        }
    }

    @Test
    void testConcurrentReadsAndWrites() throws Exception {
        final CompletedSnapshotStore completedSnapshotStore =
                createCompletedSnapshotStore(5, defaultHandleStore, Collections.emptyList());

        final int numWriterThreads = 5;
        final int numReaderThreads = 3;
        final int snapshotsPerWriter = 3;
        final ExecutorService testExecutor =
                Executors.newFixedThreadPool(
                        numWriterThreads + numReaderThreads,
                        new ExecutorThreadFactory("concurrent-read-thread"));

        try {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch =
                    new CountDownLatch(numWriterThreads + numReaderThreads);
            AtomicInteger exceptionCount = new AtomicInteger(0);

            // Spin up snapshot writer threads
            for (int threadId = 0; threadId < numWriterThreads; threadId++) {
                final int finalThreadId = threadId;
                testExecutor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                for (int i = 0; i < snapshotsPerWriter; i++) {
                                    long snapshotId =
                                            (long) finalThreadId * snapshotsPerWriter + i + 1;
                                    CompletedSnapshot snapshot = getSnapshot(snapshotId);
                                    completedSnapshotStore.add(snapshot);
                                }
                            } catch (Exception e) {
                                exceptionCount.incrementAndGet();
                            } finally {
                                completionLatch.countDown();
                            }
                        });
            }

            // Spin up snapshot reader threads (during writes)
            for (int threadId = 0; threadId < numReaderThreads; threadId++) {
                testExecutor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                for (int i = 0; i < 50; i++) {
                                    // Read operations
                                    completedSnapshotStore.getNumSnapshots();
                                    completedSnapshotStore.getAllSnapshots();
                                    completedSnapshotStore.getLatestSnapshot();
                                    // Introduce tiny wait to intersperse reads/writes
                                    Thread.sleep(2);
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                exceptionCount.incrementAndGet();
                            } catch (Exception e) {
                                exceptionCount.incrementAndGet();
                            } finally {
                                completionLatch.countDown();
                            }
                        });
            }

            // Start all threads simultaneously
            startLatch.countDown();
            boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
            assertThat(completed).as("All threads should complete").isTrue();

            // Ensure time for async cleanup to finish
            Thread.sleep(100);

            assertThat(exceptionCount.get()).as("No exceptions should occur").isEqualTo(0);

            long numSnapshots = completedSnapshotStore.getNumSnapshots();
            List<CompletedSnapshot> allSnapshots = completedSnapshotStore.getAllSnapshots();

            assertThat(numSnapshots)
                    .as("getNumSnapshots() should match getAllSnapshots().size()")
                    .isEqualTo(allSnapshots.size());

            assertThat(numSnapshots)
                    .as("Should retain at most maxNumberOfSnapshotsToRetain snapshots")
                    .isLessThanOrEqualTo(5);

            if (!allSnapshots.isEmpty()) {
                Set<Long> snapshotIds = new HashSet<>();
                for (CompletedSnapshot snapshot : allSnapshots) {
                    assertThat(snapshotIds.add(snapshot.getSnapshotID()))
                            .as("Snapshot IDs should be unique (no corruption)")
                            .isTrue();
                }
            }

            if (!allSnapshots.isEmpty()) {
                Optional<CompletedSnapshot> latest = completedSnapshotStore.getLatestSnapshot();
                assertThat(latest).as("Latest snapshot should be present").isPresent();
                assertThat(latest.get())
                        .as("Latest snapshot should match last in getAllSnapshots()")
                        .isEqualTo(allSnapshots.get(allSnapshots.size() - 1));
            }
        } finally {
            testExecutor.shutdown();
        }
    }

    @Test
    void testConcurrentAddsWithSnapshotRetention() throws Exception {
        final int maxRetain = 3;
        final CompletedSnapshotStore completedSnapshotStore =
                createCompletedSnapshotStore(
                        maxRetain, defaultHandleStore, Collections.emptyList());

        final int numThreads = 5;
        final int snapshotsPerThread = 3;
        final ExecutorService testExecutor =
                Executors.newFixedThreadPool(
                        numThreads, new ExecutorThreadFactory("concurrent-add-retention-thread"));

        try {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(numThreads);
            AtomicInteger exceptionCount = new AtomicInteger(0);

            // Spin up threads to add snapshots concurrently
            for (int threadId = 0; threadId < numThreads; threadId++) {
                final int finalThreadId = threadId;
                testExecutor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                for (int i = 0; i < snapshotsPerThread; i++) {
                                    long snapshotId =
                                            (long) finalThreadId * snapshotsPerThread + i + 1;
                                    CompletedSnapshot snapshot = getSnapshot(snapshotId);
                                    completedSnapshotStore.add(snapshot);
                                }
                            } catch (Exception e) {
                                exceptionCount.incrementAndGet();
                            } finally {
                                completionLatch.countDown();
                            }
                        });
            }

            // Start all threads simultaneously
            startLatch.countDown();
            boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
            assertThat(completed).as("All threads should complete").isTrue();

            // Ensure time for async cleanup to finish
            Thread.sleep(100);

            assertThat(exceptionCount.get()).as("No exceptions should occur").isEqualTo(0);

            List<CompletedSnapshot> allSnapshots = completedSnapshotStore.getAllSnapshots();

            assertThat(allSnapshots.size())
                    .as("Should retain at most maxNumberOfSnapshotsToRetain snapshots")
                    .isLessThanOrEqualTo(maxRetain);

            Set<Long> snapshotIds = new HashSet<>();
            for (CompletedSnapshot snapshot : allSnapshots) {
                assertThat(snapshotIds.add(snapshot.getSnapshotID()))
                        .as("Snapshot IDs should be unique (no corruption)")
                        .isTrue();
            }

            long numSnapshots = completedSnapshotStore.getNumSnapshots();
            assertThat(numSnapshots)
                    .as("getNumSnapshots() should match getAllSnapshots().size()")
                    .isEqualTo(allSnapshots.size());

            if (!allSnapshots.isEmpty()) {
                Optional<CompletedSnapshot> latest = completedSnapshotStore.getLatestSnapshot();
                assertThat(latest).as("Latest snapshot should be present").isPresent();
                assertThat(latest.get())
                        .as("Latest snapshot should match last in getAllSnapshots()")
                        .isEqualTo(allSnapshots.get(allSnapshots.size() - 1));
            }
        } finally {
            testExecutor.shutdown();
        }
    }

    @Test
    void testConcurrentGetNumSnapshotsAccuracy() throws Exception {
        final CompletedSnapshotStore completedSnapshotStore =
                createCompletedSnapshotStore(10, defaultHandleStore, Collections.emptyList());

        final int numOperations = 30;
        final ExecutorService testExecutor =
                Executors.newFixedThreadPool(
                        10, new ExecutorThreadFactory("concurrent-read-thread"));

        try {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(numOperations);
            AtomicInteger exceptionCount = new AtomicInteger(0);

            // Spin up various different snapshot operations
            for (int i = 0; i < numOperations; i++) {
                final int operationId = i;
                testExecutor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                if (operationId % 2 == 0) {
                                    // Add snapshot
                                    CompletedSnapshot snapshot = getSnapshot(operationId + 1);
                                    completedSnapshotStore.add(snapshot);
                                } else {
                                    // Read snapshot
                                    long numSnapshots = completedSnapshotStore.getNumSnapshots();
                                    List<CompletedSnapshot> allSnapshots =
                                            completedSnapshotStore.getAllSnapshots();
                                    assertThat(numSnapshots)
                                            .as(
                                                    "getNumSnapshots() should match getAllSnapshots().size()")
                                            .isEqualTo(allSnapshots.size());
                                }
                            } catch (AssertionError e) {
                                throw e;
                            } catch (Exception e) {
                                exceptionCount.incrementAndGet();
                            } finally {
                                completionLatch.countDown();
                            }
                        });
            }

            // Start all operations simultaneously
            startLatch.countDown();
            boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
            assertThat(completed).as("All operations should complete").isTrue();

            // Ensure time for async cleanup to finish
            Thread.sleep(100);

            assertThat(exceptionCount.get()).as("No exceptions should occur").isEqualTo(0);

            long numSnapshots = completedSnapshotStore.getNumSnapshots();
            List<CompletedSnapshot> allSnapshots = completedSnapshotStore.getAllSnapshots();
            assertThat(numSnapshots)
                    .as("Final getNumSnapshots() should match getAllSnapshots().size()")
                    .isEqualTo(allSnapshots.size());
        } finally {
            testExecutor.shutdown();
        }
    }

    private List<CompletedSnapshot> mapToCompletedSnapshot(
            List<Tuple2<CompletedSnapshotHandle, String>> snapshotHandles) {
        return snapshotHandles.stream()
                .map(
                        handle -> {
                            try {
                                return handle.f0.retrieveCompleteSnapshot();
                            } catch (Exception e) {
                                throw new FlussRuntimeException(
                                        "Fail to retrieve complete snapshot.");
                            }
                        })
                .collect(Collectors.toList());
    }

    private CompletedSnapshot getSnapshot(long id) {
        TableBucket tableBucket = new TableBucket(1, 1);
        return new CompletedSnapshot(
                tableBucket,
                id,
                new FsPath(tempDir.toString(), "test_snapshot"),
                new KvSnapshotHandle(Collections.emptyList(), Collections.emptyList(), 0));
    }

    private void testSnapshotRetention(
            int numToRetain,
            List<CompletedSnapshot> completed,
            List<CompletedSnapshot> expectedRetained)
            throws Exception {
        List<Tuple2<CompletedSnapshotHandle, String>> snapshotHandles = createSnapshotHandles(3);
        final List<CompletedSnapshot> completedSnapshots = mapToCompletedSnapshot(snapshotHandles);
        final TestCompletedSnapshotHandleStore snapshotHandleStore = builder.build();
        final CompletedSnapshotStore completedSnapshotStore =
                createCompletedSnapshotStore(numToRetain, snapshotHandleStore, completedSnapshots);

        for (CompletedSnapshot c : completed) {
            completedSnapshotStore.add(c);
        }
        assertThat(completedSnapshotStore.getAllSnapshots()).isEqualTo(expectedRetained);
    }

    private CompletedSnapshotStore createCompletedSnapshotStore(
            int numToRetain,
            CompletedSnapshotHandleStore snapshotHandleStore,
            Collection<CompletedSnapshot> completedSnapshots) {

        SharedKvFileRegistry sharedKvFileRegistry = new SharedKvFileRegistry();
        return new CompletedSnapshotStore(
                numToRetain,
                sharedKvFileRegistry,
                completedSnapshots,
                snapshotHandleStore,
                executorService);
    }

    private List<Tuple2<CompletedSnapshotHandle, String>> createSnapshotHandles(int num) {
        return createSnapshotHandles(num, Collections.emptySet());
    }

    private List<Tuple2<CompletedSnapshotHandle, String>> createSnapshotHandles(
            int num, Set<Integer> failSnapshots) {
        final List<Tuple2<CompletedSnapshotHandle, String>> stateHandles = new ArrayList<>();
        for (int i = 1; i <= num; i++) {
            final CompletedSnapshot completedSnapshot =
                    new CompletedSnapshot(
                            new TableBucket(1, 1),
                            i,
                            new FsPath("test_snapshot"),
                            new KvSnapshotHandle(
                                    Collections.emptyList(), Collections.emptyList(), -1));
            final CompletedSnapshotHandle snapshotStateHandle =
                    new TestingCompletedSnapshotHandle(
                            completedSnapshot, failSnapshots.contains(num));
            stateHandles.add(new Tuple2<>(snapshotStateHandle, String.valueOf(i)));
        }
        return stateHandles;
    }
}
