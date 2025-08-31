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
