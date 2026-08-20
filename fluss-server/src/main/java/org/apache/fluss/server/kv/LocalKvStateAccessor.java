/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;

import javax.annotation.Nullable;

import java.io.IOException;

/** Accessor for a KV tablet's local prewrite buffer and RocksDB state. */
final class LocalKvStateAccessor implements KvStateAccessor {

    private final KvPreWriteBuffer preWriteBuffer;
    private final RocksDBKv rocksDBKv;

    LocalKvStateAccessor(KvPreWriteBuffer preWriteBuffer, RocksDBKv rocksDBKv) {
        this.preWriteBuffer = preWriteBuffer;
        this.rocksDBKv = rocksDBKv;
    }

    @Override
    public Key encodeKey(byte[] primaryKey) {
        return Key.of(primaryKey);
    }

    @Override
    public KvStateLookupResult lookup(Key key) throws IOException {
        KvPreWriteBuffer.Value bufferedValue = preWriteBuffer.get(key);
        if (bufferedValue != null) {
            byte[] value = bufferedValue.get();
            return value == null
                    ? KvStateLookupResult.deleted()
                    : KvStateLookupResult.present(value);
        }

        byte[] value = rocksDBKv.get(key.get());
        if (value == null) {
            return KvStateLookupResult.notFound();
        }
        // Historical KV tablets persist deletes as empty values so that a local miss does not
        // expose a stale value from lake storage after the buffered delete has been flushed.
        return value.length == 0
                ? KvStateLookupResult.deleted()
                : KvStateLookupResult.present(value);
    }

    @Override
    public void insert(Key key, byte[] value, long logOffset) {
        preWriteBuffer.insert(key, value, logOffset);
    }

    @Override
    public void update(Key key, @Nullable byte[] value, long logOffset) {
        preWriteBuffer.update(key, value, logOffset);
    }

    @Override
    public void delete(Key key, long logOffset) {
        preWriteBuffer.delete(key, logOffset);
    }

    @Override
    public void truncateTo(long logOffset, TruncateReason reason) {
        preWriteBuffer.truncateTo(logOffset, reason);
    }
}
