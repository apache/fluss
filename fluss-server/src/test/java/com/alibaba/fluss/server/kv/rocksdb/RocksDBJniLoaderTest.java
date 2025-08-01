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

package com.alibaba.fluss.server.kv.rocksdb;

import com.alibaba.fluss.exception.FlussRuntimeException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.NativeLibraryLoader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RocksDBJniLoader}. */
public class RocksDBJniLoaderTest {

    @AfterEach
    public void tearDown() {
        // Reset the loader state after each test
        RocksDBJniLoader.resetRocksDbInitialized();
    }

    @Test
    void testTempLibFolderDeletedOnFail(@TempDir Path tempDir) {
        RocksDBJniLoader.resetRocksDbInitialized();
        assertThatThrownBy(
                        () ->
                                RocksDBJniLoader.ensureRocksDBIsLoaded(
                                        tempDir.toString(),
                                        () -> {
                                            throw new FlussRuntimeException("expected exception");
                                        }))
                .isInstanceOf(IOException.class);
        File[] files = tempDir.toFile().listFiles();
        assertThat(files).isNotNull();
        assertThat(files).isEmpty();
    }

    @Test
    public void testEnsureRocksDBIsLoaded(@TempDir Path tempDir) throws IOException {
        // Initially not loaded
        assertThat(RocksDBJniLoader.isRocksDBLoaded()).isFalse();

        // Load the library
        RocksDBJniLoader.ensureRocksDBIsLoaded(tempDir.toString());

        // Should be loaded now
        assertThat(RocksDBJniLoader.isRocksDBLoaded()).isTrue();

        // Second call should not fail and should still be loaded
        RocksDBJniLoader.ensureRocksDBIsLoaded(tempDir.toString());
        assertThat(RocksDBJniLoader.isRocksDBLoaded()).isTrue();
    }

    @Test
    public void testEnsureRocksDBIsLoadedWithCustomLoader(@TempDir Path tempDir)
            throws IOException {
        // Initially not loaded
        assertThat(RocksDBJniLoader.isRocksDBLoaded()).isFalse();

        // Load the library with custom loader
        RocksDBJniLoader.ensureRocksDBIsLoaded(
                tempDir.toString(), NativeLibraryLoader::getInstance);

        // Should be loaded now
        assertThat(RocksDBJniLoader.isRocksDBLoaded()).isTrue();
    }

    @Test
    public void testEnsureRocksDBIsLoadedWithInvalidDirectory() {
        // Try to load with an invalid directory
        String invalidDir = "/invalid/directory/that/does/not/exist";

        assertThatThrownBy(() -> RocksDBJniLoader.ensureRocksDBIsLoaded(invalidDir))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Could not load the native RocksDB library");
    }

    @Test
    public void testEnsureRocksDBIsLoadedWithFailingLoader(@TempDir Path tempDir) {
        // Create a failing loader supplier
        java.util.function.Supplier<NativeLibraryLoader> failingSupplier =
                () -> {
                    throw new RuntimeException("Simulated loading failure");
                };

        assertThatThrownBy(
                        () ->
                                RocksDBJniLoader.ensureRocksDBIsLoaded(
                                        tempDir.toString(), failingSupplier))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Could not load the native RocksDB library");
    }

    @Test
    public void testResetRocksDbInitialized() {
        // Initially not loaded
        assertThat(RocksDBJniLoader.isRocksDBLoaded()).isFalse();

        // Reset should not change the state
        RocksDBJniLoader.resetRocksDbInitialized();
        assertThat(RocksDBJniLoader.isRocksDBLoaded()).isFalse();
    }

    @Test
    public void testResetRocksDBLoadedFlag() throws Exception {
        // This test verifies that the reset method can be called without throwing an exception
        // The actual reset functionality is tested indirectly through the loading process
        RocksDBJniLoader.resetRocksDBLoadedFlag();
        // If we reach here, the method executed successfully
    }

    @Test
    public void testMultipleThreadsLoading(@TempDir Path tempDir) throws Exception {
        // Test that multiple threads can safely call the loader
        Thread[] threads = new Thread[5];
        Exception[] exceptions = new Exception[5];

        for (int i = 0; i < threads.length; i++) {
            final int index = i;
            threads[i] =
                    new Thread(
                            () -> {
                                try {
                                    RocksDBJniLoader.ensureRocksDBIsLoaded(tempDir.toString());
                                } catch (Exception e) {
                                    exceptions[index] = e;
                                }
                            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Check that no exceptions occurred
        for (Exception exception : exceptions) {
            assertThat(exception).isNull();
        }

        // Verify that the library is loaded
        assertThat(RocksDBJniLoader.isRocksDBLoaded()).isTrue();
    }
}
