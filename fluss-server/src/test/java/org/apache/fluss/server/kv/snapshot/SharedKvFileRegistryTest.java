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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.server.kv.snapshot.SharedKvFileRegistry}. */
class SharedKvFileRegistryTest {

    @Test
    void testRegistryNormal() throws Exception {
        SharedKvFileRegistry sharedKvFileRegistry = new SharedKvFileRegistry();

        TestKvHandle firstHandle = new TestKvHandle("first");

        // register one handle
        KvFileHandle result =
                sharedKvFileRegistry.registerReference(
                        SharedKvFileRegistryKey.fromKvFileHandle(firstHandle), firstHandle, 0);
        assertThat(result).isSameAs(firstHandle);

        // register another handle
        TestKvHandle secondHandle = new TestKvHandle("second");
        result =
                sharedKvFileRegistry.registerReference(
                        SharedKvFileRegistryKey.fromKvFileHandle(secondHandle), secondHandle, 0);
        assertThat(result).isSameAs(secondHandle);
        assertThat(firstHandle.discarded).isFalse();
        assertThat(secondHandle.discarded).isFalse();

        sharedKvFileRegistry.unregisterUnusedKvFile(1L);
        assertThat(firstHandle.discarded).isTrue();
        assertThat(secondHandle.discarded).isTrue();

        // now, we test the case register a handle again with a placeholder
        sharedKvFileRegistry.close();
        sharedKvFileRegistry = new SharedKvFileRegistry();
        TestKvHandle testKvHandle = new TestKvHandle("test");
        KvFileHandle handle =
                sharedKvFileRegistry.registerReference(
                        SharedKvFileRegistryKey.fromKvFileHandle(testKvHandle), testKvHandle, 0);

        KvFileHandle placeHolder = new PlaceholderKvFileHandler(handle);
        sharedKvFileRegistry.registerReference(
                SharedKvFileRegistryKey.fromKvFileHandle(placeHolder), placeHolder, 1);
        sharedKvFileRegistry.unregisterUnusedKvFile(1L);
        // the handle shouldn't be discarded since snapshot1 is still referring to it
        assertThat(testKvHandle.discarded).isFalse();

        sharedKvFileRegistry.unregisterUnusedKvFile(2L);
        // now, should be discarded
        assertThat(testKvHandle.discarded).isTrue();
    }

    /** Validate that unregister a nonexistent snapshot will not throw exception. */
    @Test
    void testUnregisterWithUnexistedKey() {
        SharedKvFileRegistry sharedStateRegistry = new SharedKvFileRegistry();
        sharedStateRegistry.unregisterUnusedKvFile(-1);
        sharedStateRegistry.unregisterUnusedKvFile(Long.MAX_VALUE);
    }

    /**
     * Test that stillInUse snapshot IDs protect KV files from being deleted during unregister.
     *
     * <p>Scenario: file created by snapshot 1, used by snapshot 1 through 3. If snapshot 1 is
     * leased (in stillInUse), the file should NOT be deleted even if lowestSnapshotID > 3.
     */
    @Test
    void testUnregisterWithStillInUseProtection() {
        SharedKvFileRegistry registry = new SharedKvFileRegistry();

        // Register fileA: created by snapshot 1
        TestKvHandle fileA = new TestKvHandle("fileA");
        registry.registerReference(SharedKvFileRegistryKey.fromKvFileHandle(fileA), fileA, 1L);

        // fileA used by snapshot 2 and 3 (placeholder references)
        PlaceholderKvFileHandler placeholder2 = new PlaceholderKvFileHandler(fileA);
        registry.registerReference(
                SharedKvFileRegistryKey.fromKvFileHandle(placeholder2), placeholder2, 2L);
        PlaceholderKvFileHandler placeholder3 = new PlaceholderKvFileHandler(fileA);
        registry.registerReference(
                SharedKvFileRegistryKey.fromKvFileHandle(placeholder3), placeholder3, 3L);

        // Register fileB: created by snapshot 2, only used by snapshot 2
        TestKvHandle fileB = new TestKvHandle("fileB");
        registry.registerReference(SharedKvFileRegistryKey.fromKvFileHandle(fileB), fileB, 2L);

        // Register fileC: created by snapshot 4, used by snapshot 4
        TestKvHandle fileC = new TestKvHandle("fileC");
        registry.registerReference(SharedKvFileRegistryKey.fromKvFileHandle(fileC), fileC, 4L);

        // lowestSnapshotID = 4, stillInUse = {1}
        // fileA: createdBy=1, lastUsed=3 → lastUsed(3) < 4, but snapshot 1 ∈ [1,3] → NOT deleted
        // fileB: createdBy=2, lastUsed=2 → lastUsed(2) < 4, snapshot 1 NOT ∈ [2,2] → DELETED
        // fileC: createdBy=4, lastUsed=4 → lastUsed(4) >= 4 → NOT deleted
        Set<Long> stillInUse = new HashSet<>();
        stillInUse.add(1L);
        registry.unregisterUnusedKvFile(4L, stillInUse);

        assertThat(fileA.discarded).isFalse();
        assertThat(fileB.discarded).isTrue();
        assertThat(fileC.discarded).isFalse();

        // Now remove the lease: stillInUse = empty
        // fileA: lastUsed(3) < 4, no stillInUse protection → DELETED
        registry.unregisterUnusedKvFile(4L, Collections.emptySet());
        assertThat(fileA.discarded).isTrue();
        // fileC still NOT deleted
        assertThat(fileC.discarded).isFalse();
    }

    /** Test with multiple leased snapshots protecting different file ranges. */
    @Test
    void testUnregisterWithMultipleStillInUseSnapshots() {
        SharedKvFileRegistry registry = new SharedKvFileRegistry();

        // fileA: createdBy=1, lastUsed=3
        TestKvHandle fileA = new TestKvHandle("fileA");
        registry.registerReference(SharedKvFileRegistryKey.fromKvFileHandle(fileA), fileA, 1L);
        PlaceholderKvFileHandler pA3 = new PlaceholderKvFileHandler(fileA);
        registry.registerReference(SharedKvFileRegistryKey.fromKvFileHandle(pA3), pA3, 3L);

        // fileB: createdBy=2, lastUsed=4
        TestKvHandle fileB = new TestKvHandle("fileB");
        registry.registerReference(SharedKvFileRegistryKey.fromKvFileHandle(fileB), fileB, 2L);
        PlaceholderKvFileHandler pB4 = new PlaceholderKvFileHandler(fileB);
        registry.registerReference(SharedKvFileRegistryKey.fromKvFileHandle(pB4), pB4, 4L);

        // fileC: createdBy=5, lastUsed=5
        TestKvHandle fileC = new TestKvHandle("fileC");
        registry.registerReference(SharedKvFileRegistryKey.fromKvFileHandle(fileC), fileC, 5L);

        // lowestSnapshotID = 6, stillInUse = {1, 3}
        // fileA: lastUsed(3) < 6, snapshot 1 ∈ [1,3] → protected
        // fileA: lastUsed(3) < 6, snapshot 3 ∈ [1,3] → protected
        // fileB: lastUsed(4) < 6, snapshot 1 NOT ∈ [2,4], snapshot 3 ∈ [2,4] → protected
        // fileC: lastUsed(5) < 6, snapshot 1 NOT ∈ [5,5], snapshot 3 NOT ∈ [5,5] → DELETED
        Set<Long> stillInUse = new HashSet<>();
        stillInUse.add(1L);
        stillInUse.add(3L);
        registry.unregisterUnusedKvFile(6L, stillInUse);

        assertThat(fileA.discarded).isFalse();
        assertThat(fileB.discarded).isFalse();
        assertThat(fileC.discarded).isTrue();
    }

    private static class TestKvHandle extends KvFileHandle {

        private static final long serialVersionUID = 1;

        private boolean discarded;

        public TestKvHandle(String path) {
            super(path, 0);
        }

        @Override
        public void discard() throws Exception {
            this.discarded = true;
        }
    }
}
