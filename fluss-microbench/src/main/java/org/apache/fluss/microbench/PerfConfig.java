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

import org.apache.fluss.microbench.config.ScenarioConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Loads YAML scenarios from a file or a bundled preset. */
public final class PerfConfig {

    private PerfConfig() {}

    public static ScenarioConfig load(String presetOrFile) throws IOException {
        return ScenarioConfig.parse(loadYaml(presetOrFile));
    }

    public static String loadYaml(String presetOrFile) throws IOException {
        Path file = Paths.get(presetOrFile);
        if (Files.isRegularFile(file)) {
            return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        }
        String resource = "presets/" + presetOrFile + ".yaml";
        try (InputStream in = PerfConfig.class.getClassLoader().getResourceAsStream(resource)) {
            if (in != null) {
                java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int n;
                while ((n = in.read(buffer)) != -1) {
                    out.write(buffer, 0, n);
                }
                return new String(out.toByteArray(), StandardCharsets.UTF_8);
            }
        }
        throw new IOException("Scenario not found: " + presetOrFile);
    }
}
