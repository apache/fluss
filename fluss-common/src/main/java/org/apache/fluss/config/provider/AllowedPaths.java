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

package org.apache.fluss.config.provider;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * The mandatory {@code allowed.paths} parameter of file-reading {@link ConfigProvider}s: a
 * comma-separated list of roots the provider may read from.
 */
final class AllowedPaths {

    static final String PARAM = "allowed.paths";

    private final String identifier;
    private final List<Path> roots;

    private AllowedPaths(String identifier, List<Path> roots) {
        this.identifier = identifier;
        this.roots = roots;
    }

    static AllowedPaths parse(String identifier, @Nullable String value) {
        List<Path> roots = new ArrayList<>();
        if (value != null) {
            for (String root : value.split(",")) {
                String trimmed = root.trim();
                if (!trimmed.isEmpty()) {
                    roots.add(canonicalize(Paths.get(trimmed)));
                }
            }
        }
        if (roots.isEmpty()) {
            throw new IllegalArgumentException(
                    "The '"
                            + identifier
                            + "' config provider requires the '"
                            + PARAM
                            + "' parameter (comma-separated roots it may read from); set "
                            + "config.providers."
                            + identifier
                            + ".param."
                            + PARAM
                            + ".");
        }
        return new AllowedPaths(identifier, roots);
    }

    /** Canonicalizes {@code file} and checks it lies under one of the allowed roots. */
    Path check(Path file) {
        Path canonical = canonicalize(file);
        for (Path root : roots) {
            if (canonical.startsWith(root)) {
                return canonical;
            }
        }
        throw new IllegalArgumentException(
                "File '"
                        + canonical
                        + "' is outside the "
                        + PARAM
                        + " "
                        + roots
                        + " of the '"
                        + identifier
                        + "' config provider.");
    }

    private static Path canonicalize(Path path) {
        try {
            return Paths.get(path.toFile().getCanonicalPath());
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Cannot canonicalize path '" + path + "' for the allowed-paths check.", e);
        }
    }
}
