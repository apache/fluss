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

package org.apache.fluss.client.tiering;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.InstantiationUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests Java serialization and copying of engine-agnostic tiering splits. */
class TieringSplitTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testJavaSerializationAndCopyPreserveMetadata(boolean snapshotSplit) throws Exception {
        TablePath tablePath = TablePath.of("db", "table");
        TableBucket tableBucket = new TableBucket(1L, 2L, 3);
        TieringSplit split =
                snapshotSplit
                        ? new TieringSnapshotSplit(
                                tablePath, tableBucket, "p1", 4L, 5L, 6, 1, 1000L)
                        : new TieringLogSplit(tablePath, tableBucket, "p1", 4L, 5L, 6, 1, 1000L);
        split.skipCurrentRound();

        TieringSplit restored = InstantiationUtils.clone(split);
        assertThat(restored).isEqualTo(split).isNotSameAs(split);
        assertThat(restored.splitId()).isEqualTo(split.splitId());
        assertThat(restored.shouldSkipCurrentRound()).isTrue();
        assertThat(restored.getSplitIndex()).isEqualTo(1);
        assertThat(restored.getTieringRoundTimestamp()).isEqualTo(1000L);

        TieringSplit copy = restored.copy(restored.getNumberOfSplits());
        assertThat(copy).isEqualTo(restored).isNotSameAs(restored);
        TieringSplit copyWithMetadata = restored.copy(10, 2, 2000L);
        assertThat(copyWithMetadata.getNumberOfSplits()).isEqualTo(10);
        assertThat(copyWithMetadata.getSplitIndex()).isEqualTo(2);
        assertThat(copyWithMetadata.getTieringRoundTimestamp()).isEqualTo(2000L);
        assertThat(copyWithMetadata.shouldSkipCurrentRound()).isTrue();
        assertThat(copyWithMetadata.copy(6, 1, 1000L)).isEqualTo(restored);
    }
}
