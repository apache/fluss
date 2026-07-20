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

package org.apache.fluss.flink.source;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.encode.KeyEncoder;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLookupCustomShuffle.InputDataPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FlinkLookupShuffleTableSource}, focusing on the invariant that a wrapped
 * source (which has already advertised {@code SupportsLookupCustomShuffle}, so Flink suppresses its
 * own hash shuffle) must always return a partitioner, and that a copy made between {@code
 * getLookupRuntimeProvider()} and {@code getPartitioner()} keeps the stashed lookup normalizer.
 */
class FlinkLookupShuffleTableSourceTest {

    private static FlinkTableSource baseSource(int[] primaryKeys, int[] bucketKeys) {
        RowType tableOutputType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("address", DataTypes.STRING()))
                                .getLogicalType();
        Configuration flussConfig = new Configuration();
        flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");
        FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                new FlinkConnectorOptionsUtils.StartupOptions();
        startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.EARLIEST;
        return new FlinkTableSource(
                TablePath.of("test_db", "dim"),
                flussConfig,
                new TableConfig(new Configuration()),
                tableOutputType,
                primaryKeys,
                bucketKeys,
                new int[] {}, // partition key indexes
                true, // streaming
                startupOptions,
                false, // lookup async (sync avoids any async runtime setup)
                false, // insert if not exists
                null, // cache
                1000L, // scan partition discovery interval
                false, // is data lake enabled
                null, // merge engine type
                new HashMap<>(),
                null); // lease context
    }

    @Test
    void testGetPartitionerPresentForFullPkLookup() {
        // primary key == bucket key == id; lookup on the full primary key.
        FlinkLookupShuffleTableSource source =
                new FlinkLookupShuffleTableSource(baseSource(new int[] {0}, new int[] {0}), 3);
        source.getLookupRuntimeProvider(new TestLookupContext(new int[][] {{0}}));
        assertThat(source.getPartitioner()).isPresent();
    }

    @Test
    void testGetPartitionerPresentForPrefixLookup() {
        // primary key (id, name), bucket key = id; a lookup on id alone is a prefix lookup.
        FlinkLookupShuffleTableSource source =
                new FlinkLookupShuffleTableSource(baseSource(new int[] {0, 1}, new int[] {0}), 3);
        source.getLookupRuntimeProvider(new TestLookupContext(new int[][] {{0}}));
        assertThat(source.getPartitioner()).isPresent();
    }

    @Test
    void testGetPartitionerFailsBeforeRuntimeProvider() {
        // getPartitioner() before getLookupRuntimeProvider() violates the call order the source
        // relies on: it must surface an internal-state error, not silently return empty (which the
        // planner treats as "no shuffle").
        FlinkLookupShuffleTableSource source =
                new FlinkLookupShuffleTableSource(baseSource(new int[] {0}, new int[] {0}), 3);
        assertThatThrownBy(source::getPartitioner)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("getLookupRuntimeProvider");
    }

    @Test
    void testCopyPreservesAbilityAndStashedNormalizer() {
        int numBuckets = 3;
        FlinkLookupShuffleTableSource source =
                new FlinkLookupShuffleTableSource(
                        baseSource(new int[] {0}, new int[] {0}), numBuckets);
        // stash the normalizer, then copy (as the planner may do) before getPartitioner().
        source.getLookupRuntimeProvider(new TestLookupContext(new int[][] {{0}}));

        DynamicTableSource copy = source.copy();
        assertThat(copy).isInstanceOf(FlinkLookupShuffleTableSource.class);

        Optional<InputDataPartitioner> partitioner =
                ((FlinkLookupShuffleTableSource) copy).getPartitioner();
        assertThat(partitioner)
                .as("the copy must keep the stashed normalizer so the shuffle stays enabled")
                .isPresent();

        // The copy must also carry numBuckets over: route several keys and confirm they land on the
        // bucket computed with numBuckets == 3 (a dropped or defaulted numBuckets would misroute).
        InputDataPartitioner copiedPartitioner = partitioner.get();
        int numPartitions = 2;
        for (int id = 0; id < 30; id++) {
            assertThat(copiedPartitioner.partition(GenericRowData.of(id), numPartitions))
                    .as("id=%d must route by numBuckets=%d", id, numBuckets)
                    .isEqualTo(Math.floorMod(flussBucketOf(id, numBuckets), numPartitions));
        }
    }

    /** Independently computes the Fluss bucket id for an int key (matching the client routing). */
    private static int flussBucketOf(int id, int numBuckets) {
        RowType keyRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        org.apache.fluss.types.RowType flussKeyType = FlinkConversions.toFlussRowType(keyRowType);
        KeyEncoder encoder =
                KeyEncoder.ofBucketKeyEncoder(flussKeyType, Collections.singletonList("id"), null);
        byte[] bytes = encoder.encodeKey(new FlinkAsFlussRow().replace(GenericRowData.of(id)));
        return BucketingFunction.of(null).bucketing(bytes, numBuckets);
    }

    /** Minimal {@link LookupTableSource.LookupContext} exposing only the lookup key indexes. */
    private static class TestLookupContext implements LookupTableSource.LookupContext {
        private final int[][] keys;

        private TestLookupContext(int[][] keys) {
            this.keys = keys;
        }

        @Override
        public int[][] getKeys() {
            return keys;
        }

        @Override
        public boolean preferCustomShuffle() {
            return true;
        }

        @Override
        public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                DataType producedDataType) {
            throw new UnsupportedOperationException();
        }
    }
}
