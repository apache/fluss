/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.sink;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartitionBucketCountResolver}. */
class PartitionBucketCountResolverTest {

    private static final List<String> PARTITION_KEYS = Collections.singletonList("dt");

    private static PartitionInfo partitionInfo(String partitionName, int bucketCount) {
        return new PartitionInfo(
                partitionName.hashCode(),
                ResolvedPartitionSpec.fromPartitionName(PARTITION_KEYS, partitionName),
                null,
                bucketCount);
    }

    /** Records every fetch invocation so tests can assert how often metadata is queried. */
    private static class CountingFetcher
            implements PartitionBucketCountResolver.PartitionMetadataFetcher {

        private final List<PartitionInfo> partitions;
        private final int tableBucketCount;
        private int fetchCount;

        private CountingFetcher(List<PartitionInfo> partitions, int tableBucketCount) {
            this.partitions = partitions;
            this.tableBucketCount = tableBucketCount;
        }

        @Override
        public List<PartitionInfo> listPartitionInfos(TablePath tablePath) {
            fetchCount++;
            return partitions;
        }

        @Override
        public int tableBucketCount(TablePath tablePath) {
            return tableBucketCount;
        }
    }

    /**
     * Test-only resolver variant that injects a fake metadata source, keeping the production class
     * free of any test wiring.
     */
    @VisibleForTesting
    private static final class FakeSourceResolver extends PartitionBucketCountResolver {

        private static final long serialVersionUID = 1L;

        private final PartitionMetadataFetcher fetcher;

        private FakeSourceResolver(int defaultBucketCount, PartitionMetadataFetcher fetcher) {
            super(null, null, defaultBucketCount);
            this.fetcher = fetcher;
        }

        @Override
        protected PartitionMetadataFetcher fetcher() {
            return fetcher;
        }
    }

    @Test
    void testExistingPartitionResolvesAuthoritativeCountAndIsCached() {
        CountingFetcher fetcher =
                new CountingFetcher(
                        Arrays.asList(partitionInfo("2024-01", 4), partitionInfo("2024-02", 8)), 8);
        PartitionBucketCountResolver resolver = new FakeSourceResolver(8, fetcher);

        // The partition exists: its authoritative count wins over the table-level default.
        assertThat(resolver.bucketCountOf("2024-01")).isEqualTo(4);
        assertThat(resolver.bucketCountOf("2024-02")).isEqualTo(8);

        // One RPC warms up every partition of the table; subsequent lookups never fetch again.
        assertThat(fetcher.fetchCount).isEqualTo(1);
        assertThat(resolver.bucketCountOf("2024-01")).isEqualTo(4);
        assertThat(resolver.bucketCountOf("2024-02")).isEqualTo(8);
        assertThat(fetcher.fetchCount).isEqualTo(1);
    }

    @Test
    void testMissingPartitionFallsBackToCurrentTableLevelCount() {
        // Only the old partition exists: the new partition is not created yet (its creation is
        // triggered by the downstream writer after the sharding step).
        CountingFetcher fetcher =
                new CountingFetcher(Collections.singletonList(partitionInfo("2024-01", 4)), 8);
        PartitionBucketCountResolver resolver = new FakeSourceResolver(8, fetcher);

        // The missing partition falls back to the current table-level bucket count.
        assertThat(resolver.bucketCountOf("2024-02")).isEqualTo(8);
        assertThat(fetcher.fetchCount).isEqualTo(1);

        // The fallback value is cached as well, so no further RPC happens.
        assertThat(resolver.bucketCountOf("2024-02")).isEqualTo(8);
        assertThat(fetcher.fetchCount).isEqualTo(1);
    }

    @Test
    void testEveryPartitionOfTheResponseIsCached() {
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            partitions.add(partitionInfo("2024-0" + (i + 1), 4));
        }
        CountingFetcher fetcher = new CountingFetcher(partitions, 8);
        PartitionBucketCountResolver resolver = new FakeSourceResolver(8, fetcher);

        // Resolving one partition warms up all partitions carried by the same response.
        assertThat(resolver.bucketCountOf("2024-03")).isEqualTo(4);
        assertThat(fetcher.fetchCount).isEqualTo(1);
        for (int i = 0; i < 5; i++) {
            assertThat(resolver.bucketCountOf("2024-0" + (i + 1))).isEqualTo(4);
        }
        assertThat(fetcher.fetchCount).isEqualTo(1);
    }

    @Test
    void testMetadataFailureRetriesThenFailsFast() {
        PartitionBucketCountResolver.PartitionMetadataFetcher failingFetcher =
                new PartitionBucketCountResolver.PartitionMetadataFetcher() {
                    @Override
                    public List<PartitionInfo> listPartitionInfos(TablePath tablePath)
                            throws Exception {
                        throw new RuntimeException("metadata rpc error");
                    }

                    @Override
                    public int tableBucketCount(TablePath tablePath) {
                        return 8;
                    }
                };
        PartitionBucketCountResolver resolver = new FakeSourceResolver(8, failingFetcher);

        assertThatThrownBy(() -> resolver.bucketCountOf("2024-01"))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("after 3 retries");
    }

    @Test
    void testTableLookupFailureRetriesThenFailsFast() {
        // The table itself is not retrievable (null response): fall through to the table-level
        // fallback, whose own failure must surface after the bounded retries.
        PartitionBucketCountResolver.PartitionMetadataFetcher failingFetcher =
                new PartitionBucketCountResolver.PartitionMetadataFetcher() {
                    @Override
                    public List<PartitionInfo> listPartitionInfos(TablePath tablePath) {
                        return null;
                    }

                    @Override
                    public int tableBucketCount(TablePath tablePath) throws Exception {
                        throw new RuntimeException("table rpc error");
                    }
                };
        PartitionBucketCountResolver resolver = new FakeSourceResolver(8, failingFetcher);

        assertThatThrownBy(() -> resolver.bucketCountOf("2024-01"))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("after 3 retries");
    }
}
