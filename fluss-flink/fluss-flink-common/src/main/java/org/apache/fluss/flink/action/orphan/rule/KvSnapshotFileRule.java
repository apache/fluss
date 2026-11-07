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
 * Rule for files under a {@code snap-<id>/} KV snapshot directory.
 *
 * <p>Match key is the file's parent {@code snap-<id>} directory name: if that name is in {@link
 * BucketActiveRefs#kvActiveSnapDirs()} (which carries the per-bucket union of RETAINED +
 * STILL_IN_USE entries from {@code ListKvSnapshots}, see that getter's javadoc) the file is {@link
 * Decision#KEEP_ACTIVE}.
 *
 * <p>The set-based check is what prevents retained non-latest snapshots from being misclassified as
 * orphan — e.g. with {@code kv.snapshot.num-retained=2}, {@code snap-9} is still active while
 * {@code snap-10} is the latest.
 */
@Internal
public final class KvSnapshotFileRule implements FileRule {

    private static final String SNAP_DIR_PREFIX = FlussPaths.REMOTE_KV_SNAPSHOT_DIR_PREFIX;

    private static final Set<String> KNOWN_FIXED_NAMES =
            new HashSet<String>(Arrays.asList("_METADATA", "CURRENT", "LOG", "IDENTITY"));

    // SnapshotLocation uploads every remote KV file under a generated UUID name.
    private static final Pattern REMOTE_FILE_UUID =
            Pattern.compile(
                    "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    @Override
    public RuleId id() {
        return RuleId.KV_SNAPSHOT_FILE;
    }

    @Override
    public Decision evaluate(FileMeta file, BucketActiveRefs activeRefs, long cutoffMillis) {
        FsPath parent = file.path().getParent();
        if (parent == null) {
            return Decision.SKIP_UNKNOWN;
        }

        String parentName = parent.getName();
        if (!parentName.startsWith(SNAP_DIR_PREFIX)) {
            return Decision.SKIP_UNKNOWN;
        }

        // Parent must be snap-<digits>; reject e.g. snap-, snap-abc.
        String snapIdPart = parentName.substring(SNAP_DIR_PREFIX.length());
        if (snapIdPart.isEmpty()) {
            return Decision.SKIP_UNKNOWN;
        }
        for (int i = 0; i < snapIdPart.length(); i++) {
            if (!Character.isDigit(snapIdPart.charAt(i))) {
                return Decision.SKIP_UNKNOWN;
            }
        }

        if (!isKnownSnapshotFile(file.path().getName())) {
            return Decision.SKIP_UNKNOWN;
        }

        if (activeRefs.kvActiveSnapDirs().contains(parentName)) {
            return Decision.KEEP_ACTIVE;
        }

        return file.modificationTime() < cutoffMillis ? Decision.DELETE : Decision.DEFER;
    }

    private static boolean isKnownSnapshotFile(String fileName) {
        if (REMOTE_FILE_UUID.matcher(fileName).matches()) {
            return true;
        }
        if (KNOWN_FIXED_NAMES.contains(fileName)) {
            return true;
        }
        if (fileName.startsWith("MANIFEST-") || fileName.startsWith("OPTIONS-")) {
            return true;
        }
        return fileName.endsWith(".sst") || fileName.endsWith(".log");
    }
}
