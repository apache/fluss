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

import java.util.regex.Pattern;

/**
 * Rule for shared SST files under the {@code shared/} KV directory.
 *
 * <p>Determines whether a shared SST file is still referenced by any active snapshot. The active
 * set is built from the union of remote {@code shared_file_handles[*].kv_file_handle.path}
 * basenames across all active snapshots' {@code _METADATA} files.
 *
 * <p>Safety: completeness is explicit. An unresolved set conservatively returns {@link
 * Decision#KEEP_ACTIVE}; a resolved-but-empty set proves that no active snapshot references a
 * shared object and therefore allows the normal cutoff policy to run.
 */
@Internal
public final class KvSharedSstRule implements FileRule {

    private static final Pattern REMOTE_FILE_UUID =
            Pattern.compile(
                    "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    @Override
    public RuleId id() {
        return RuleId.KV_SHARED_SST;
    }

    @Override
    public Decision evaluate(FileMeta file, BucketActiveRefs activeRefs, long cutoffMillis) {
        FsPath parent = file.path().getParent();
        if (parent == null || !FlussPaths.REMOTE_KV_SNAPSHOT_SHARED_DIR.equals(parent.getName())) {
            return Decision.SKIP_UNKNOWN;
        }
        String fileName = file.path().getName();
        if (!fileName.endsWith(".sst") && !REMOTE_FILE_UUID.matcher(fileName).matches()) {
            return Decision.SKIP_UNKNOWN;
        }

        if (activeRefs.kvSharedSstFileNames().contains(fileName)) {
            return Decision.KEEP_ACTIVE;
        }

        if (!activeRefs.kvSharedSstRefsComplete()) {
            return Decision.KEEP_ACTIVE;
        }

        return file.modificationTime() < cutoffMillis ? Decision.DELETE : Decision.DEFER;
    }
}
