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

package org.apache.fluss.flink.action.orphan.rule;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.FlussPaths;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Rule for log-segment files under a remote log bucket.
 *
 * <p>Each segment UUID directory contains four files ({@code .log}, {@code .index}, {@code
 * .timeindex}, {@code .writer_snapshot}) that form an atomic unit. They are uploaded together by
 * tiering and referenced together by manifests. If the segment is not in any active manifest and
 * the file modification time is older than the cutoff, all four files are eligible for deletion.
 */
@Internal
public final class LogSegmentRule implements FileRule {

    private static final Pattern SEGMENT_DIR_PATTERN =
            Pattern.compile(
                    "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
                            + "-[0-9a-fA-F]{12}");

    private static final Set<String> KNOWN_SUFFIXES =
            new HashSet<String>(Arrays.asList(".log", ".index", ".timeindex", ".writer_snapshot"));

    public LogSegmentRule() {}

    @Override
    public RuleId id() {
        return RuleId.LOG_SEGMENT;
    }

    @Override
    public Decision evaluate(FileMeta file, BucketActiveRefs activeRefs, long cutoffMillis) {
        FsPath path = file.path();
        FsPath parent = path.getParent();
        if (parent == null || !isSegmentDir(parent.getName()) || !hasKnownSuffix(path.getName())) {
            return Decision.SKIP_UNKNOWN;
        }

        String relativePath = parent.getName() + "/" + path.getName();
        if (activeRefs.logSegmentRelativePaths().contains(relativePath)) {
            return Decision.KEEP_ACTIVE;
        }

        return file.modificationTime() < cutoffMillis ? Decision.DELETE : Decision.DEFER;
    }

    static boolean isSegmentDir(String dirName) {
        return SEGMENT_DIR_PATTERN.matcher(dirName).matches();
    }

    private static boolean hasKnownSuffix(String fileName) {
        String name = fileName;
        if (name.endsWith(FlussPaths.DELETED_FILE_SUFFIX)) {
            name = name.substring(0, name.length() - FlussPaths.DELETED_FILE_SUFFIX.length());
        }
        for (String suffix : KNOWN_SUFFIXES) {
            if (name.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }
}
