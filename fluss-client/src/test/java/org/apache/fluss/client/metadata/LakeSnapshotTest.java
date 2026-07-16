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

package org.apache.fluss.client.metadata;

import org.apache.fluss.lake.committer.LakeTieringTableState;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LakeSnapshot}, in particular the lazy parsing of the tiering state. */
class LakeSnapshotTest {

    @Test
    void testLazyParse() {
        // absent -> null.
        assertThat(new LakeSnapshot(1L, Collections.emptyMap()).getLakeTieringTableState())
                .isNull();

        // present -> parsed lazily.
        byte[] json =
                new LakeTieringTableState(true, Collections.singletonMap(5L, 1000L)).toJsonBytes();
        LakeTieringTableState state =
                new LakeSnapshot(1L, Collections.emptyMap(), json).getLakeTieringTableState();
        assertThat(state).isNotNull();
        assertThat(state.isPartitionDoneInitialized()).isTrue();
        assertThat(state.getPartitionUpdateTimes()).containsEntry(5L, 1000L);
    }

    @Test
    void testCorruptTieringStateDoesNotBlockBucketOffsets() {
        TableBucket bucket = new TableBucket(1L, 0);
        Map<TableBucket, Long> offsets = Collections.singletonMap(bucket, 100L);
        LakeSnapshot snapshot =
                new LakeSnapshot(1L, offsets, "{not-json".getBytes(StandardCharsets.UTF_8));

        // bucket offsets remain accessible even though the tiering state is corrupt.
        assertThat(snapshot.getTableBucketsOffset()).containsEntry(bucket, 100L);
        // only the explicit state accessor surfaces the parse failure.
        assertThatThrownBy(snapshot::getLakeTieringTableState).isInstanceOf(RuntimeException.class);
    }
}
