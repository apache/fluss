/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.codegen;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Utilities for code generation tests with expected file comparison.
 *
 * <p>Supports two modes:
 *
 * <ul>
 *   <li><b>Normal mode:</b> Compares generated code against expected files
 *   <li><b>Update mode:</b> When {@code -Dcodegen.update.expected=true}, overwrites expected files
 * </ul>
 *
 * <p>Expected files location: {@code src/test/resources/expected/<relativePath>}
 */
public final class CodeGenTestUtils {

    private static final String EXPECTED_DIR = "expected";
    private static final String UPDATE_PROPERTY = "codegen.update.expected";

    private CodeGenTestUtils() {}

    /**
     * Asserts that the generated code matches the expected file content.
     *
     * @param actual the actual generated code
     * @param relativePath path relative to the expected directory
     */
    public static void assertMatchesExpected(String actual, String relativePath) {
        assertMatchesExpectedInternal(actual, relativePath, false);
    }

    /**
     * Asserts that the generated code matches the expected file content, with normalization.
     *
     * <p>Normalization: trims trailing whitespace, normalizes line endings to LF, ensures single
     * trailing newline.
     *
     * @param actual the actual generated code
     * @param relativePath path relative to the expected directory
     */
    public static void assertMatchesExpectedNormalized(String actual, String relativePath) {
        assertMatchesExpectedInternal(actual, relativePath, true);
    }

    private static void assertMatchesExpectedInternal(
            String actual, String relativePath, boolean normalize) {
        String actualContent = normalize ? normalize(actual) : actual;

        if (shouldUpdateExpected()) {
            updateExpectedFile(actualContent, relativePath);
            return;
        }

        String expected = readExpectedFile(relativePath);
        String expectedContent = normalize ? normalize(expected) : expected;

        assertThat(actualContent)
                .as("Generated code should match expected file: %s", relativePath)
                .isEqualTo(expectedContent);
    }

    private static String readExpectedFile(String relativePath) {
        String resourcePath = EXPECTED_DIR + "/" + relativePath;
        URL resource = CodeGenTestUtils.class.getClassLoader().getResource(resourcePath);

        if (resource == null) {
            fail(
                    "Expected file not found: %s\n"
                            + "Create at: src/test/resources/%s\n"
                            + "Or run with -D%s=true to auto-generate",
                    resourcePath, resourcePath, UPDATE_PROPERTY);
        }

        try (InputStream is = resource.openStream()) {
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            return result.toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new RuntimeException("Failed to read expected file: " + resourcePath, e);
        }
    }

    private static boolean shouldUpdateExpected() {
        return "true".equalsIgnoreCase(System.getProperty(UPDATE_PROPERTY));
    }

    private static void updateExpectedFile(String content, String relativePath) {
        Path sourceRoot = findSourceRoot();
        Path expectedFile =
                sourceRoot.resolve(
                        "fluss-codegen/src/test/resources/" + EXPECTED_DIR + "/" + relativePath);

        try {
            Files.createDirectories(expectedFile.getParent());
            try (OutputStream os = Files.newOutputStream(expectedFile)) {
                os.write(content.getBytes(StandardCharsets.UTF_8));
            }
            System.out.println("[CodeGenTestUtils] Updated: " + expectedFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update expected file: " + expectedFile, e);
        }
    }

    private static Path findSourceRoot() {
        Path current = Paths.get("").toAbsolutePath();

        // Walk up until we find fluss-codegen directory
        for (Path path = current; path != null; path = path.getParent()) {
            if (Files.exists(path.resolve("fluss-codegen"))) {
                return path;
            }
        }

        throw new RuntimeException(
                "Cannot find fluss source root from: "
                        + current
                        + "\nExpected file update requires running from the project directory.");
    }

    private static String normalize(String code) {
        if (code == null || code.isEmpty()) {
            return "\n";
        }

        String[] lines = code.replace("\r\n", "\n").replace("\r", "\n").split("\n", -1);
        StringBuilder sb = new StringBuilder();
        int lastNonEmpty = -1;

        // Single pass: build result and track last non-empty line
        for (int i = 0; i < lines.length; i++) {
            String trimmed = trimTrailing(lines[i]);
            if (!trimmed.isEmpty()) {
                lastNonEmpty = sb.length() + trimmed.length();
            }
            if (i > 0) {
                sb.append("\n");
            }
            sb.append(trimmed);
        }

        // Truncate to last non-empty content and add single trailing newline
        if (lastNonEmpty > 0) {
            sb.setLength(lastNonEmpty);
        }
        sb.append("\n");
        return sb.toString();
    }

    private static String trimTrailing(String s) {
        int end = s.length();
        while (end > 0 && Character.isWhitespace(s.charAt(end - 1))) {
            end--;
        }
        return end == s.length() ? s : s.substring(0, end);
    }
}
