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

/**
 * Rule for shared SST files under the {@code shared/} KV directory.
 *
 * <p>Always returns {@link Decision#KEEP_ACTIVE}. The true active set for shared SSTs lives inside
 * the engine's {@code SharedKvFileRegistry}; orphan cleanup has no read path into that registry, so
 * any deletion here would be a guess. Per the action's hard constraint "prefer leak over
 * mis-delete," the rule never deletes, and as a consequence orphan PK-table / orphan-partition
 * directories permanently retain their {@code shared/} subtree as accepted residue (recovering that
 * residue would require a registry-backed GC channel that is out of scope for this action).
 */
@Internal
public final class KvSharedSstRule implements FileRule {

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
        if (!file.path().getName().endsWith(".sst")) {
            return Decision.SKIP_UNKNOWN;
        }
        return Decision.KEEP_ACTIVE;
    }
}
