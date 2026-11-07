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

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the output location used by benchmark runs. */
class MicrobenchPathsTest {

    @Test
    void resolvesRunDirectory(@TempDir Path root) {
        MicrobenchPaths paths = new MicrobenchPaths(root);
        assertThat(paths.runDir("write", "20260101-000000"))
                .isEqualTo(root.resolve("runs/write/20260101-000000"));
    }

    @Test
    void usesConfiguredRoot(@TempDir Path root) {
        String original = System.getProperty("microbench.root");
        try {
            System.setProperty("microbench.root", root.toString());
            assertThat(MicrobenchPaths.fromSystemProperty().runDir("write", "run"))
                    .isEqualTo(root.resolve("runs/write/run"));
        } finally {
            if (original == null) {
                System.clearProperty("microbench.root");
            } else {
                System.setProperty("microbench.root", original);
            }
        }
    }
}
