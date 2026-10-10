/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.rocksdb;

import io.github.fluss_contrib.rocksdb.RocksDBException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/** Test utilities for {@link RocksDBKv}. */
public final class RocksDBKvTestUtils {

    private RocksDBKvTestUtils() {}

    /** Returns a spy that fails when enabling avoid-flush during shutdown. */
    public static RocksDBKv spyWithAvoidFlushFailure(RocksDBKv rocksDBKv, RocksDBException failure)
            throws RocksDBException {
        RocksDBKv rocksDBKvSpy = spy(rocksDBKv);
        doThrow(failure).when(rocksDBKvSpy).setAvoidFlushDuringShutdown();
        return rocksDBKvSpy;
    }
}
