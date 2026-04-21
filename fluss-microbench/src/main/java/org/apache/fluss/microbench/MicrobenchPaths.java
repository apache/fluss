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

import java.nio.file.Files;
import java.nio.file.Path;

/** Central path resolution utility. All output paths derive from a single root. */
public class MicrobenchPaths {
    private final Path root;

    public MicrobenchPaths(Path root) {
        this.root = root;
    }

    public static MicrobenchPaths fromSystemProperty() {
        String prop = System.getProperty("microbench.root");
        if (prop != null && !prop.isEmpty()) {
            return new MicrobenchPaths(Path.of(prop));
        }
        return new MicrobenchPaths(Path.of(System.getProperty("user.dir"), ".microbench"));
    }

    public Path root() {
        return root;
    }

    public Path runsDir() {
        return root.resolve("runs");
    }

    public Path runDir(String scenario, String timestamp) {
        return runsDir().resolve(scenario).resolve(timestamp);
    }

    public Path latestLink(String scenario) {
        return runsDir().resolve(scenario).resolve("latest");
    }

    public Path previousLink(String scenario) {
        return runsDir().resolve(scenario).resolve("previous");
    }

    public Path datasetsDir() {
        return root.resolve("datasets");
    }

    public Path baselinesDir() {
        return root.resolve("baselines");
    }

    public Path baselineDir(String name) {
        return baselinesDir().resolve(name);
    }

    /** Resolve a ref: baselines/{ref} first, then runs/{ref}. */
    public Path resolveRef(String ref) {
        Path baselinePath = baselinesDir().resolve(ref);
        if (Files.isDirectory(baselinePath)) {
            return baselinePath;
        }
        return runsDir().resolve(ref);
    }
}
