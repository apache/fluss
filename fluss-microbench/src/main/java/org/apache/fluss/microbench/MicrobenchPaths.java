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

import java.nio.file.Path;
import java.nio.file.Paths;

/** Output paths for benchmark runs. */
public final class MicrobenchPaths {

    private final Path root;

    public MicrobenchPaths(Path root) {
        this.root = root;
    }

    public static MicrobenchPaths fromSystemProperty() {
        String configured = System.getProperty("microbench.root");
        Path root =
                configured == null || configured.isEmpty()
                        ? Paths.get(System.getProperty("user.dir"), ".microbench")
                        : Paths.get(configured);
        return new MicrobenchPaths(root);
    }

    public Path runDir(String scenario, String timestamp) {
        return root.resolve("runs").resolve(scenario).resolve(timestamp);
    }
}
