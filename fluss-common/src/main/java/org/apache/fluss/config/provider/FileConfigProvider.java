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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A {@link ConfigProvider} resolving <code>${file:/path/creds.properties:key}</code> markers to a
 * single property of a Java properties file. The mandatory {@code allowed.paths} parameter
 * restricts which files may be read.
 */
@Internal
public class FileConfigProvider implements ConfigProvider {

    public static final String IDENTIFIER = "file";

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
                    "The 'file' config provider requires a file path; use "
                            + "${file:/path/to/file.properties:property}.");
        }
        Path file = allowedPaths.check(Paths.get(path));
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            properties.load(in);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to read properties file '" + path + "' for the 'file' config provider.",
                    e);
        }
        String value = properties.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Property '" + key + "' not found in file '" + path + "'.");
        }
        return value;
    }
}
