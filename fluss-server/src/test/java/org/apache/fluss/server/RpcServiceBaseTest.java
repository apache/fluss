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

package org.apache.fluss.server;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A meta-test for {@link RpcServiceBase} and related server-side catalog/coordinator code.
 *
 * <p>This test scans all Java source files under {@code fluss-server/src/main/java} to detect
 * patterns where a {@code catch} block catches an exception variable but throws a new exception
 * without passing the caught variable as the cause. This helps prevent loss of root-cause
 * information in exception chains.
 */
class RpcServiceBaseTest {

    private static final Path SERVER_SOURCE_DIR =
            Paths.get("src/main/java/org/apache/fluss/server");

    private static final Pattern CATCH_PATTERN =
            Pattern.compile("\\s*catch\\s*\\(\\s*\\w+(?:\\.\\w+)*\\s+(\\w+)\\s*\\)\\s*\\{?");

    private static final Pattern THROW_NEW_PATTERN =
            Pattern.compile("throw\\s+new\\s+\\w+Exception\\s*\\(");

    @Test
    void metaTestNoCatchRethrowWithoutCause() throws IOException {
        assertThat(Files.exists(SERVER_SOURCE_DIR))
                .as("Server source directory must exist: " + SERVER_SOURCE_DIR.toAbsolutePath())
                .isTrue();

        List<String> violations = new ArrayList<>();
        Files.walkFileTree(
                SERVER_SOURCE_DIR,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (file.toString().endsWith(".java")) {
                            scanFileForViolations(file, violations);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });

        assertThat(violations)
                .as(
                        "Found catch blocks that throw new exceptions without preserving the cause. "
                                + "Please pass the caught exception as the second argument to the "
                                + "new exception constructor.\nViolations:\n"
                                + String.join("\n", violations))
                .isEmpty();
    }

    private void scanFileForViolations(Path file, List<String> violations) {
        List<String> lines;
        try {
            lines = Files.readAllLines(file);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + file, e);
        }

        List<CatchContext> activeCatches = new ArrayList<>();
        int braceDepth = 0;

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            int lineNum = i + 1;

            Matcher catchMatcher = CATCH_PATTERN.matcher(line);
            if (catchMatcher.find()) {
                String varName = catchMatcher.group(1);
                activeCatches.add(new CatchContext(varName, lineNum, braceDepth));
            }

            for (int j = 0; j < line.length(); j++) {
                char ch = line.charAt(j);
                if (ch == '{') {
                    braceDepth++;
                } else if (ch == '}') {
                    braceDepth--;
                    final int currentDepth = braceDepth;
                    activeCatches.removeIf(ctx -> ctx.catchBraceDepth >= currentDepth);
                }
            }

            if (THROW_NEW_PATTERN.matcher(line).find()) {
                String throwStatement = collectThrowStatement(lines, i);
                for (CatchContext ctx : activeCatches) {
                    if (!throwStatement.contains(ctx.varName)) {
                        String trimmed = throwStatement.replaceAll("\\s+", " ").trim();
                        violations.add(
                                String.format(
                                        "  %s:%d: catch(%s) -> %s",
                                        file, lineNum, ctx.varName, trimmed));
                    }
                }
            }
        }
    }

    private String collectThrowStatement(List<String> lines, int startIndex) {
        StringBuilder sb = new StringBuilder();
        int parenDepth = 0;
        boolean started = false;
        for (int i = startIndex; i < lines.size(); i++) {
            String line = lines.get(i);
            for (int j = 0; j < line.length(); j++) {
                char ch = line.charAt(j);
                if (ch == '(') {
                    parenDepth++;
                    started = true;
                } else if (ch == ')') {
                    parenDepth--;
                }
                if (started) {
                    sb.append(ch);
                }
            }
            sb.append(" ");
            if (started && parenDepth <= 0) {
                break;
            }
        }
        return sb.toString();
    }

    private static class CatchContext {
        private final String varName;
        private final int catchStartLine;
        private final int catchBraceDepth;

        CatchContext(String varName, int catchStartLine, int catchBraceDepth) {
            this.varName = varName;
            this.catchStartLine = catchStartLine;
            this.catchBraceDepth = catchBraceDepth;
        }
    }
}
