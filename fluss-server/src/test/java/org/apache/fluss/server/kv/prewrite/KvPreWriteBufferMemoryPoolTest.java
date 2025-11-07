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

package org.apache.fluss.server.kv.prewrite;

import org.apache.fluss.exception.PreWriteBufferFullException;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvPreWriteBufferMemoryPool}. */
public class KvPreWriteBufferMemoryPoolTest {
    private static final long GLOBAL_LIMIT = 1000 * 1024; // 1mb total
    private static final long PER_BUCKET_LIMIT = 600 * 1024; // 600KB per bucket

    private KvPreWriteBufferMemoryPool pool;
    private TableBucket tb1;
    private TableBucket tb2;

    @BeforeEach
    void setup() {
        pool = new KvPreWriteBufferMemoryPool(GLOBAL_LIMIT, PER_BUCKET_LIMIT);
        tb1 = new TableBucket(0, 1);
        tb2 = new TableBucket(0, 2);
    }

    @AfterEach
    void teardown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void testSuccessAllocateAndRelease() {
        byte[] data = new byte[] {0, 1, 2};
        ByteBuffer src = ByteBuffer.wrap(data);
        ByteBuffer buffer = pool.allocate(tb1, src);

        assertThat(buffer).isNotNull();
        assertThat(buffer.isDirect()).isTrue();
        assertThat(buffer.capacity()).isEqualTo(3);
        assertThat(buffer.position()).isZero();
        assertThat(buffer.limit()).isEqualTo(3);

        // Verify content
        ByteBuffer dup = buffer.duplicate();
        byte[] actual = new byte[dup.remaining()];
        dup.get(actual);
        assertThat(actual).hasSize(3).containsExactly(data);

        // Release
        pool.release(tb1, buffer);

        assertThat(pool.getTotalUsed()).isZero();
        assertThat(pool.getPerBucketUsage(tb1)).isZero();
    }

    @Test
    void testEnforcePerBucketLimit() {
        int chunk = (int) (PER_BUCKET_LIMIT / 3);

        // First two chunks OK
        pool.allocate(tb1, new byte[chunk]);
        pool.allocate(tb1, new byte[chunk]);

        assertThat(pool.getPerBucketUsage(tb1)).isEqualTo(chunk * 2);

        // Third chunk exceeds limit.
        assertThatThrownBy(() -> pool.allocate(tb1, new byte[chunk + 1]))
                .isInstanceOf(PreWriteBufferFullException.class)
                .hasMessageContaining(
                        "PreWriteBuffer memory limit exceeded for table bucket: "
                                + "TableBucket{tableId=0, bucket=1}, Current: 409600, Request: 204801, Limit: 614400");
    }

    @Test
    void testEnforceGlobalMemoryLimit() {
        int chunk = (int) (GLOBAL_LIMIT / 3);

        pool.allocate(tb1, new byte[chunk]);
        pool.allocate(tb2, new byte[chunk]);

        assertThat(pool.getTotalUsed()).isEqualTo(chunk * 2);

        // Third allocation exceeds global limit.
        assertThatThrownBy(() -> pool.allocate(tb1, new byte[chunk + 5]))
                .isInstanceOf(PreWriteBufferFullException.class)
                .hasMessageContaining(
                        "Global PreWriteBuffer memory pool exhausted. Requested: 341338 bytes, "
                                + "Total used: 682666, Limit: 1024000");
    }
}
