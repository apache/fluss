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

package org.apache.fluss.microbench.baseline;

import org.apache.fluss.microbench.MicrobenchPaths;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Manages named baselines (saved report snapshots) for regression comparison. */
public class BaselineManager {

    private static final String[] BASELINE_FILES = {
        "summary.json", "timeseries.csv", "config-snapshot.yaml"
    };

    private final Path baselineDir;

    public BaselineManager(MicrobenchPaths paths) {
        this.baselineDir = paths.baselinesDir();
    }

    /**
     * Saves a baseline by copying key report files from the given report directory into a named
     * baseline subdirectory.
     */
    public void save(String name, Path reportDir) throws IOException {
        validateBaselineName(name);
        Path target = baselineDir.resolve(name);
        Files.createDirectories(target);
        for (String file : BASELINE_FILES) {
            Path src = reportDir.resolve(file);
            if (Files.exists(src)) {
                Files.copy(src, target.resolve(file), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    /** Lists all saved baseline names. */
    public List<String> list() throws IOException {
        if (!Files.isDirectory(baselineDir)) {
            return Collections.emptyList();
        }
        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(baselineDir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    names.add(entry.getFileName().toString());
                }
            }
        }
        Collections.sort(names);
        return names;
    }

    /** Deletes a named baseline directory. */
    public void delete(String name) throws IOException {
        validateBaselineName(name);
        Path target = baselineDir.resolve(name);
        if (Files.isDirectory(target)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(target)) {
                for (Path entry : stream) {
                    Files.deleteIfExists(entry);
                }
            }
            Files.delete(target);
        }
    }

    /**
     * Resolves a baseline name to a directory path under the managed baselines directory.
     *
     * @param name the baseline name (must not contain path separators)
     */
    public Path resolveByName(String name) {
        validateBaselineName(name);
        return baselineDir.resolve(name);
    }

    private static void validateBaselineName(String name) {
        if (name.contains("/") || name.contains("\\") || name.contains("..")) {
            throw new IllegalArgumentException("Invalid baseline name: " + name);
        }
    }
}
