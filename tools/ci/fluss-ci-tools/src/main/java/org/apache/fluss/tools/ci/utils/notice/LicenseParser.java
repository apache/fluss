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

package org.apache.fluss.tools.ci.utils.notice;

import org.apache.fluss.tools.ci.utils.shared.Dependency;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses LICENSE files for bundled dependency lines.
 *
 * <p>Dependencies listed in LICENSE (non-Apache) are considered declared alongside NOTICE for
 * license-check purposes. Lines match: "- groupId:artifactId:version" or "-
 * groupId:artifactId:version (License Name)".
 */
public final class LicenseParser {

    // "- dnsjava:dnsjava:3.4.0 (BSD 3-Clause)" or "- javax.xml.bind:jaxb-api:2.3.1"
    private static final Pattern LICENSE_DEPENDENCY_PATTERN =
            Pattern.compile(
                    "^\\-\\s+"
                            + "(?<groupId>[^:]+):"
                            + "(?<artifactId>[^:]+):"
                            + "(?<version>[^:\\s(]+)"
                            + "(?::(?<classifier>[^\\s(]+))?"
                            + "(\\s|\\(|$)");

    private LicenseParser() {}

    /**
     * Parses a LICENSE file and returns the set of dependencies declared in the "bundled
     * dependencies" sections (non-Apache license sections). Returns an empty set if the file does
     * not exist or cannot be read.
     */
    public static Set<Dependency> parseLicenseFile(Path licensePath) throws IOException {
        Set<Dependency> declared = new LinkedHashSet<>();
        if (!Files.exists(licensePath)) {
            return declared;
        }
        for (String line : Files.readAllLines(licensePath)) {
            Matcher m = LICENSE_DEPENDENCY_PATTERN.matcher(line);
            if (m.find()) {
                String groupId = m.group("groupId");
                String artifactId = m.group("artifactId");
                String version = m.group("version");
                String classifier = m.group("classifier");
                declared.add(Dependency.create(groupId, artifactId, version, classifier));
            }
        }
        return declared;
    }
}
