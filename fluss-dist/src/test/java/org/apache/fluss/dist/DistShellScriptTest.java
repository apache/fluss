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

package org.apache.fluss.dist;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

final class DistShellScriptTest {

    private static final String LOG4J_SHUTDOWN_HOOK_DISABLED = "-Dlog4j.shutdownHookEnabled=false";

    @Test
    void testForegroundAndDaemonDisableLog4jShutdownHook() throws Exception {
        assertThat(readBinScript("fluss-console.sh"))
                .contains(LOG4J_SHUTDOWN_HOOK_DISABLED)
                .contains("log4j-console.properties");

        assertThat(readBinScript("fluss-daemon.sh"))
                .contains(LOG4J_SHUTDOWN_HOOK_DISABLED)
                .contains("log4j.properties");
    }

    private static String readBinScript(String scriptName) throws IOException {
        Path scriptPath =
                Paths.get(
                        System.getProperty("project.basedir"),
                        "src",
                        "main",
                        "resources",
                        "bin",
                        scriptName);
        return new String(Files.readAllBytes(scriptPath), StandardCharsets.UTF_8);
    }
}
