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

/** Dispatches a candidate file to the matching orphan-cleanup rule. */
@Internal
public final class RuleDispatcher {

    private static final FileRule UNKNOWN_RULE =
            new FileRule() {
                @Override
                public RuleId id() {
                    return RuleId.UNKNOWN;
                }

                @Override
                public Decision evaluate(
                        FileMeta file, BucketActiveRefs activeRefs, long cutoffMillis) {
                    return Decision.SKIP_UNKNOWN;
                }
            };

    private final FileRule logSegmentRule;
    private final FileRule logManifestRule;
    private final FileRule kvSnapshotFileRule = new KvSnapshotFileRule();
    private final FileRule kvSharedSstRule = new KvSharedSstRule();

    public RuleDispatcher() {
        this(false, false);
    }

    public RuleDispatcher(boolean allowDeleteManifest) {
        this(allowDeleteManifest, false);
    }

    public RuleDispatcher(boolean allowDeleteManifest, boolean orphanDirMode) {
        this.logSegmentRule = new LogSegmentRule(orphanDirMode);
        this.logManifestRule = new LogManifestRule(allowDeleteManifest);
    }

    public FileRule dispatch(FileMeta file) {
        FsPath path = file.path();
        FsPath parent = path.getParent();
        if (parent == null) {
            return UNKNOWN_RULE;
        }

        String parentName = parent.getName();
        if (FlussPaths.REMOTE_LOG_METADATA_DIR_NAME.equals(parentName)) {
            return logManifestRule;
        }
        if (FlussPaths.REMOTE_KV_SNAPSHOT_SHARED_DIR.equals(parentName)) {
            return kvSharedSstRule;
        }
        if (parentName.startsWith(FlussPaths.REMOTE_KV_SNAPSHOT_DIR_PREFIX)) {
            return kvSnapshotFileRule;
        }
        if (LogSegmentRule.isSegmentDir(parentName)) {
            return logSegmentRule;
        }
        return UNKNOWN_RULE;
    }
}
