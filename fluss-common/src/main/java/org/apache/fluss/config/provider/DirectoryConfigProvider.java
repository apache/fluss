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

import org.apache.fluss.annotation.Internal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A {@link ConfigProvider} resolving <code>${directory:/dir:file}</code> markers to the verbatim
 * UTF-8 content of {@code /dir/file}, matching a Kubernetes {@code Secret} mounted as a volume. The
 * mandatory {@code allowed.paths} parameter restricts which directories may be read.
 */
@Internal
public class DirectoryConfigProvider implements ConfigProvider {

    public static final String IDENTIFIER = "directory";

    private AllowedPaths allowedPaths;

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public void configure(Map<String, String> params) {
        ProviderParams.validateKeys(IDENTIFIER, params, AllowedPaths.PARAM);
        allowedPaths = AllowedPaths.parse(IDENTIFIER, params.get(AllowedPaths.PARAM));
    }

    @Override
    public String resolve(String path, String key) {
        checkState(allowedPaths != null, "Provider '%s' has not been configured.", IDENTIFIER);
        if (path.isEmpty()) {
            throw new IllegalArgumentException(
                    "The 'directory' config provider requires a directory path; use "
                            + "${directory:/dir:file}.");
        }
        Path file = allowedPaths.check(Paths.get(path, key));
        try {
            return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to read secret file '"
                            + file
                            + "' referenced by ${directory:"
                            + path
                            + ":"
                            + key
                            + "}.",
                    e);
        }
    }
}
