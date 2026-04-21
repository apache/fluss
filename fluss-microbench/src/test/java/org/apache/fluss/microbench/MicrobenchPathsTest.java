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

package org.apache.fluss.microbench;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class MicrobenchPathsTest {

    @TempDir Path tempDir;

    @Test
    void runDirResolution() {
        MicrobenchPaths paths = new MicrobenchPaths(tempDir);
        Path runDir = paths.runDir("myScenario", "20260407_120000");
        assertThat(runDir).isEqualTo(tempDir.resolve("runs/myScenario/20260407_120000"));
    }

    @Test
    void baselineDirResolution() {
        MicrobenchPaths paths = new MicrobenchPaths(tempDir);
        Path baselineDir = paths.baselineDir("v1");
        assertThat(baselineDir).isEqualTo(tempDir.resolve("baselines/v1"));
    }

    @Test
    void datasetsDirResolution() {
        MicrobenchPaths paths = new MicrobenchPaths(tempDir);
        assertThat(paths.datasetsDir()).isEqualTo(tempDir.resolve("datasets"));
    }

    @Test
    void resolveRefPrefersBaselinesOverRuns() throws Exception {
        MicrobenchPaths paths = new MicrobenchPaths(tempDir);
        // Create both baselines/ref and runs/ref directories
        Files.createDirectories(tempDir.resolve("baselines/ref"));
        Files.createDirectories(tempDir.resolve("runs/ref"));
        assertThat(paths.resolveRef("ref")).isEqualTo(tempDir.resolve("baselines/ref"));
    }

    @Test
    void resolveRefFallsBackToRuns() {
        MicrobenchPaths paths = new MicrobenchPaths(tempDir);
        // No baselines/ref directory exists
        assertThat(paths.resolveRef("ref")).isEqualTo(tempDir.resolve("runs/ref"));
    }

    @Test
    void fromSystemPropertyDefault() {
        // Clear the system property to test default behavior
        String previous = System.getProperty("microbench.root");
        try {
            System.clearProperty("microbench.root");
            MicrobenchPaths paths = MicrobenchPaths.fromSystemProperty();
            assertThat(paths.root().getFileName().toString()).isEqualTo(".microbench");
        } finally {
            if (previous != null) {
                System.setProperty("microbench.root", previous);
            }
        }
    }
}
