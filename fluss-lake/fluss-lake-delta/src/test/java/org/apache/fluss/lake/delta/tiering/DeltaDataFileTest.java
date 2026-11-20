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

package org.apache.fluss.lake.delta.tiering;

import org.apache.fluss.utils.InstantiationUtils;

import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DeltaDataFile}. */
class DeltaDataFileTest {

    private static final StructType SCHEMA =
            new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

    private static final String STATS_JSON =
            "{\"numRecords\":3,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":3},"
                    + "\"nullCount\":{\"id\":0}}";

    @Test
    void testRoundTripWithStatistics() {
        DataFileStatistics stats = DataFileStatistics.deserializeFromJson(STATS_JSON, SCHEMA).get();
        DataFileStatus status = new DataFileStatus("part-0.parquet", 100L, 42L, Optional.of(stats));
        Map<String, String> partition = Collections.singletonMap("dt", "2025-01-01");

        DeltaDataFile dataFile = DeltaDataFile.of(status, SCHEMA, partition);
        assertThat(dataFile.path()).isEqualTo("part-0.parquet");
        assertThat(dataFile.partitionValues()).isEqualTo(partition);

        DataFileStatus restored = dataFile.toDataFileStatus(SCHEMA);
        assertThat(restored.getPath()).isEqualTo("part-0.parquet");
        assertThat(restored.getSize()).isEqualTo(100L);
        assertThat(restored.getModificationTime()).isEqualTo(42L);
        assertThat(restored.getStatistics()).isPresent();
        assertThat(restored.getStatistics().get().getNumRecords()).isEqualTo(3L);
    }

    @Test
    void testRoundTripWithoutStatistics() {
        DataFileStatus status = new DataFileStatus("part-1.parquet", 7L, 8L, Optional.empty());

        DeltaDataFile dataFile = DeltaDataFile.of(status, SCHEMA, Collections.emptyMap());
        DataFileStatus restored = dataFile.toDataFileStatus(SCHEMA);
        assertThat(restored.getPath()).isEqualTo("part-1.parquet");
        assertThat(restored.getSize()).isEqualTo(7L);
        assertThat(restored.getModificationTime()).isEqualTo(8L);
        assertThat(restored.getStatistics()).isNotPresent();
        assertThat(dataFile.partitionValues()).isEmpty();
    }

    @Test
    void testJavaSerialization() throws Exception {
        DeltaDataFile dataFile =
                new DeltaDataFile(
                        "part-2.parquet",
                        10L,
                        20L,
                        STATS_JSON,
                        Collections.singletonMap("dt", "d"));

        byte[] bytes = InstantiationUtils.serializeObject(dataFile);
        DeltaDataFile restored =
                InstantiationUtils.deserializeObject(bytes, getClass().getClassLoader());

        assertThat(restored).isEqualTo(dataFile);
        assertThat(restored.hashCode()).isEqualTo(dataFile.hashCode());
    }

    @Test
    void testEqualsHashCodeAndToString() {
        Map<String, String> partition = new HashMap<>();
        partition.put("dt", "2025-01-01");
        DeltaDataFile a = new DeltaDataFile("p", 1L, 2L, STATS_JSON, partition);
        DeltaDataFile same = new DeltaDataFile("p", 1L, 2L, STATS_JSON, partition);
        DeltaDataFile differentPath = new DeltaDataFile("q", 1L, 2L, STATS_JSON, partition);
        DeltaDataFile differentSize = new DeltaDataFile("p", 9L, 2L, STATS_JSON, partition);
        DeltaDataFile differentTime = new DeltaDataFile("p", 1L, 9L, STATS_JSON, partition);
        DeltaDataFile noStats = new DeltaDataFile("p", 1L, 2L, null, partition);
        DeltaDataFile differentPartition =
                new DeltaDataFile("p", 1L, 2L, STATS_JSON, Collections.emptyMap());

        assertThat(a).isEqualTo(a).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(a)
                .isNotEqualTo(null)
                .isNotEqualTo("p")
                .isNotEqualTo(differentPath)
                .isNotEqualTo(differentSize)
                .isNotEqualTo(differentTime)
                .isNotEqualTo(noStats)
                .isNotEqualTo(differentPartition);
        // the partition map is copied, so later mutation does not leak into the data file
        partition.put("other", "x");
        assertThat(a).isEqualTo(same);

        assertThat(a.toString())
                .isEqualTo("DeltaDataFile{path='p', size=1, partitionValues={dt=2025-01-01}}");
    }
}
