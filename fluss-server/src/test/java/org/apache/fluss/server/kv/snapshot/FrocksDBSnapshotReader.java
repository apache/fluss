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

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Reads a RocksDB snapshot in an isolated process using the legacy FRocksDB JNI dependency. */
public final class FrocksDBSnapshotReader {

    /** Switches the reader to a full scan that expects the given number of rows. */
    static final String COUNT_ARG = "--count";

    private FrocksDBSnapshotReader() {}

    /** Opens a snapshot and verifies the key/value pairs supplied after the database path. */
    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length % 2 == 0) {
            throw new IllegalArgumentException(
                    "Expected a database path followed by one or more key/value pairs.");
        }

        RocksDB.loadLibrary();
        try (Options options = new Options();
                RocksDB rocksDB = RocksDB.openReadOnly(options, args[0])) {
            if (COUNT_ARG.equals(args[1])) {
                assertRowCount(rocksDB, Long.parseLong(args[2]));
                return;
            }
            for (int i = 1; i < args.length; i += 2) {
                byte[] actualValue = rocksDB.get(args[i].getBytes(StandardCharsets.UTF_8));
                byte[] expectedValue = args[i + 1].getBytes(StandardCharsets.UTF_8);
                if (!Arrays.equals(actualValue, expectedValue)) {
                    throw new AssertionError(
                            String.format(
                                    "Unexpected value for key %s: expected %s but was %s",
                                    args[i], args[i + 1], Arrays.toString(actualValue)));
                }
            }
        }
    }

    private static void assertRowCount(RocksDB rocksDB, long expectedRows) throws Exception {
        long rows = 0;
        try (RocksIterator iterator = rocksDB.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                rows++;
            }
            iterator.status();
        }
        if (rows != expectedRows) {
            throw new AssertionError(
                    String.format("Expected %d rows but scanned %d", expectedRows, rows));
        }
    }
}
