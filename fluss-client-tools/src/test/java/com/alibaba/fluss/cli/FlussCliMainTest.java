/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.cli;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.alibaba.fluss.cli.base.BaseTest;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.GlobalConfiguration;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

public class FlussCliMainTest extends BaseTest {

    @TempDir static Path tempDir;

    private FlussCliMain cli;

    private Path confFile;

    @BeforeEach
    void setUp() throws IOException {
        confFile = tempDir.resolve("fluss.conf");
        confFile.toFile().createNewFile();
        cli = new FlussCliMain();
    }

    @AfterEach
    public void close() {
        confFile.toFile().delete();
    }

    @Test
    void testHelpFlagReturnsZero() {
        int exitCode = cli.run(new String[] {"--help"});
        assertEquals(0, exitCode);
        exitCode = cli.run(new String[] {"--version"});
        assertEquals(0, exitCode);
    }

    @Test
    void testInvalidConfigFileThrows() {
        String invalidPath = tempDir.resolve("nonexistent.conf").toString();
        cli.run(new String[] {"--config", invalidPath, "test-parent"});
        assertTrue(
                outContent.toString().contains("Config file not found")
                        && outContent.toString().contains("nonexistent.conf"));
    }

    @Test
    void testIllegalBootstrapServerThrows() {
        Configuration config = mock(Configuration.class);

        try (MockedStatic<GlobalConfiguration> mocked = mockStatic(GlobalConfiguration.class)) {
            mocked.when(() -> GlobalConfiguration.loadConfiguration(any(), any()))
                    .thenReturn(config);
            cli.run(new String[] {"-c", confFile.toString(), "test-parent"});
            when(config.get(ConfigOptions.BOOTSTRAP_SERVERS)).thenReturn(null);
            assertTrue(outContent.toString().contains("Missing bootstrap servers"));

            when(config.get(ConfigOptions.BOOTSTRAP_SERVERS))
                    .thenReturn(Collections.singletonList("&"));
            cli.run(new String[] {"test-parent"});
            assertTrue(outContent.toString().contains("Invalid server format"));
        }
    }

    @Test
    void testValidCoordinatorFallback() {
        Configuration config = mock(Configuration.class);
        when(config.toMap()).thenReturn(new HashMap<>());
        when(config.get(ConfigOptions.BOOTSTRAP_SERVERS))
                .thenReturn(Collections.singletonList("localhost:9092"));
        try (MockedStatic<GlobalConfiguration> mocked = mockStatic(GlobalConfiguration.class)) {
            mocked.when(() -> GlobalConfiguration.loadConfiguration(any(), any()))
                    .thenReturn(config);
            int result = cli.run(new String[] {"-c", confFile.toString(), "dummy"});
            assertEquals(1, result);
        }
    }

    @Test
    void testBootstrapServerFormatValidationFails() {
        Configuration config = mock(Configuration.class);
        when(config.get(ConfigOptions.BOOTSTRAP_SERVERS))
                .thenReturn(Collections.singletonList("localhost"));
        try (MockedStatic<GlobalConfiguration> mocked = mockStatic(GlobalConfiguration.class)) {
            mocked.when(() -> GlobalConfiguration.loadConfiguration(any(), any()))
                    .thenReturn(config);
            cli.run(new String[] {"-c", confFile.toString(), "test-parent"});
            assertTrue(outContent.toString().contains("Invalid server format"));
        }
    }

    @Test
    void testSplitByKeywordsNoKeywords() {
        String[] args = {"--config", confFile.toString(), "--help"};
        List<String[]> result = invokeSplitByKeywords(args, Collections.singletonList("run"));
        assertArrayEquals(args, result.get(0));
        assertEquals(0, result.get(1).length);
    }

    @Test
    void testSplitByKeywordsWithKeyword() {
        String[] args = {"--config", confFile.toString(), "run", "--arg1"};
        List<String[]> result = invokeSplitByKeywords(args, Collections.singletonList("run"));
        assertArrayEquals(new String[] {"--config", confFile.toString(), "run"}, result.get(0));
        assertArrayEquals(new String[] {"--arg1"}, result.get(1));
    }

    @SuppressWarnings("unchecked")
    private List<String[]> invokeSplitByKeywords(String[] args, List<String> kw) {
        try {
            Field field = FlussCliMain.class.getDeclaredField("keywords");
            field.setAccessible(true);
            List<String> keywords = (List<String>) field.get(null);
            keywords.clear();
            keywords.addAll(kw);
            Method method = FlussCliMain.class.getDeclaredMethod("splitByKeywords", String[].class);
            method.setAccessible(true);
            return (List<String[]>) method.invoke(null, (Object) args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
