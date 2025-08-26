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

package org.apache.fluss.server.kv;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * A batch writer for KV which will first buffer the data written in method {@link #put(byte[],
 * byte[])}/{@link #delete(byte[])}, and flush the buffer in method {@link #flush()}.
 */
public interface KvBatchWriter extends AutoCloseable {

    /** Put a key-value pair. */
    void put(@Nonnull byte[] key, @Nonnull byte[] value) throws IOException;

    /** Delete a key-value pair by the given key. */
    void delete(@Nonnull byte[] key) throws IOException;

    /** Flush the written key-value pair. */
    void flush() throws IOException;
}
