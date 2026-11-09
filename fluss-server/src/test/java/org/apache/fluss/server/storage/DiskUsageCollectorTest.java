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

package org.apache.fluss.server.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;

/** Test for {@link DiskUsageCollector}. */
class DiskUsageCollectorTest {

    @TempDir private File tempDir;

    @Test
    void testCollectReturnsRatioInRange() throws Exception {
        File dataDir = new File(tempDir, "data-0");
        assertThat(dataDir.mkdirs()).isTrue();

        DiskUsageCollector collector = new DiskUsageCollector(Collections.singletonList(dataDir));

        double ratio = collector.collect();
        assertThat(ratio).isBetween(0.0, 1.0);
    }

    @Test
    void testMultipleDirsOnSameFileStoreDeduplicated() throws Exception {
        File dataDir1 = new File(tempDir, "data-1");
        File dataDir2 = new File(tempDir, "data-2");
        assertThat(dataDir1.mkdirs()).isTrue();
        assertThat(dataDir2.mkdirs()).isTrue();

        DiskUsageCollector twoDirs = new DiskUsageCollector(Arrays.asList(dataDir1, dataDir2));
        DiskUsageCollector oneDir = new DiskUsageCollector(Collections.singletonList(dataDir1));

        // both directories share the same FileStore -> result should match a single-dir collector
        assertThat(twoDirs.collect()).isCloseTo(oneDir.collect(), offset(1e-6));
    }

    @Test
    void testEmptyDataDirsReturnsZero() throws Exception {
        DiskUsageCollector collector = new DiskUsageCollector(Collections.emptyList());
        assertThat(collector.collect()).isEqualTo(0.0);
    }

    @Test
    void testPartialFailureSkipsBadDirAndReportsRemaining() throws Exception {
        File goodDir = new File(tempDir, "good");
        assertThat(goodDir.mkdirs()).isTrue();
        File badDir = new File(tempDir, "does-not-exist/nested");

        DiskUsageCollector collector = new DiskUsageCollector(Arrays.asList(badDir, goodDir));
        double ratio = collector.collect();
        assertThat(ratio).isBetween(0.0, 1.0);
    }

    @Test
    void testAllDirsFailThrowsIOException() {
        File bad1 = new File("/__fluss_non_existent_1__/x");
        File bad2 = new File("/__fluss_non_existent_2__/y");

        DiskUsageCollector collector = new DiskUsageCollector(Arrays.asList(bad1, bad2));
        assertThatThrownBy(collector::collect)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("All 2 data directories failed");
    }
}
