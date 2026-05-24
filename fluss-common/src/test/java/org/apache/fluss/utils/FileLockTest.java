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

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileLock}. */
class FileLockTest {

    @TempDir Path tempDir;

    @Test
    void testTryLockAndUnlock() throws Exception {
        File lockFile = tempDir.resolve("lock1").toFile();
        FileLock fileLock = new FileLock(lockFile.getAbsolutePath());

        assertThat(fileLock.tryLock()).isTrue();
        assertThat(fileLock.isValid()).isTrue();
        assertThat(lockFile).exists();

        fileLock.unlock();
        assertThat(fileLock.isValid()).isFalse();

        fileLock.unlockAndDestroy();
        assertThat(lockFile).doesNotExist();
    }

    @Test
    void testTryLockReturnsFalseOnSameJvmContention() throws Exception {
        // Two FileLock instances in the same JVM target the same underlying file. The second
        // attempt must return false instead of swallowing OverlappingFileLockException as an
        // I/O failure or leaking a file descriptor.
        File lockFile = tempDir.resolve("lockcontended").toFile();
        FileLock first = new FileLock(lockFile.getAbsolutePath());
        FileLock second = new FileLock(lockFile.getAbsolutePath());

        try {
            assertThat(first.tryLock()).isTrue();
            assertThat(second.tryLock()).isFalse();
            assertThat(second.isValid()).isFalse();
        } finally {
            second.unlockAndDestroy();
            first.unlockAndDestroy();
        }
        assertThat(lockFile).doesNotExist();
    }

    @Test
    void testTryLockAfterUnlockAndDestroyIsReusable() throws Exception {
        // After unlockAndDestroy() the internal output stream must be closed and reset so a
        // subsequent tryLock() can re-initialize it without leaking the previous descriptor.
        File lockFile = tempDir.resolve("lockreuse").toFile();
        FileLock fileLock = new FileLock(lockFile.getAbsolutePath());

        assertThat(fileLock.tryLock()).isTrue();
        fileLock.unlockAndDestroy();
        assertThat(lockFile).doesNotExist();

        // Reusing the same instance should transparently re-create the underlying file.
        assertThat(fileLock.tryLock()).isTrue();
        assertThat(fileLock.isValid()).isTrue();
        fileLock.unlockAndDestroy();
        assertThat(lockFile).doesNotExist();
    }

    @Test
    void testUnlockAndDestroyWithoutAcquiringLock() throws Exception {
        // Calling unlockAndDestroy() without ever acquiring the lock must not throw and must
        // leave no resources behind.
        File lockFile = tempDir.resolve("lockneveracquired").toFile();
        FileLock fileLock = new FileLock(lockFile.getAbsolutePath());

        fileLock.unlockAndDestroy();
        assertThat(fileLock.isValid()).isFalse();
        assertThat(lockFile).doesNotExist();
    }

    @Test
    void testIsValidIsFalseBeforeTryLock() {
        FileLock fileLock = new FileLock(tempDir.resolve("lockfresh").toString());
        assertThat(fileLock.isValid()).isFalse();
    }

    @Test
    void testConstructorRejectsFileNameWithoutLegalCharacters() {
        // The constructor strips everything outside [\w/\\] from the file name; a name made up
        // entirely of illegal characters must fail fast rather than silently locking an empty
        // path.
        assertThatThrownBy(() -> new FileLock(tempDir.resolve("???").toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("legal characters");
    }
}
