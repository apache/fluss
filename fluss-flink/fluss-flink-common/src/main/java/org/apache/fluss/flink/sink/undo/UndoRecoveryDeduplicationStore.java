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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.utils.FileUtils;

import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Exact membership of submitted undo keys for one local recovery attempt. */
final class UndoRecoveryDeduplicationStore implements AutoCloseable {

    private static final byte[] PRESENT = new byte[0];
    private static final long WRITE_BUFFER_SIZE = 4 * 1024 * 1024L;

    private final Path directory;
    private final WriteOptions writeOptions;
    private final RocksDB database;

    UndoRecoveryDeduplicationStore(Path temporaryRoot) throws IOException, RocksDBException {
        RocksDB.loadLibrary();
        Files.createDirectories(temporaryRoot);
        directory = Files.createTempDirectory(temporaryRoot, "fluss-undo-recovery-");
        try (Options options = new Options()) {
            options.setCreateIfMissing(true)
                    .setWriteBufferSize(WRITE_BUFFER_SIZE)
                    .setMaxWriteBufferNumber(2);
            writeOptions = new WriteOptions();
            try {
                // A retry computes its own recovery keys from the changelog.
                writeOptions.setDisableWAL(true);
                database = RocksDB.open(options, directory.toString());
            } catch (Throwable failure) {
                writeOptions.close();
                throw failure;
            }
        } catch (Throwable failure) {
            try {
                FileUtils.deleteDirectory(directory.toFile());
            } catch (IOException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    boolean contains(byte[] key) {
        try {
            return database.get(key) != null;
        } catch (RocksDBException failure) {
            throw new FlussRuntimeException("Failed to read the undo recovery key store", failure);
        }
    }

    void add(byte[] key) {
        try {
            database.put(writeOptions, key, PRESENT);
        } catch (RocksDBException failure) {
            throw new FlussRuntimeException("Failed to record an undo recovery key", failure);
        }
    }

    @Override
    public void close() throws IOException {
        database.close();
        writeOptions.close();
        FileUtils.deleteDirectory(directory.toFile());
    }
}
