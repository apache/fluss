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

import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KvSharedSstRule}. */
class KvSharedSstRuleTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final long DAY_MS = 24L * 60L * 60L * 1000L;
    private static final long CUTOFF_MS = NOW - DAY_MS;

    private final KvSharedSstRule rule = new KvSharedSstRule();

    @Test
    void keepsExpiredUnreferencedSharedSst() {
        FileMeta file = file("/kv/db/t-1/0/shared/abc-001.sst", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.KEEP_ACTIVE);
    }

    @Test
    void keepsReferencedSharedSst() {
        FileMeta file = file("/kv/db/t-1/0/shared/abc-001.sst", NOW - 2 * DAY_MS);
        Set<String> sharedFiles = new HashSet<String>();
        sharedFiles.add("abc-001.sst");
        BucketActiveRefs activeRefs =
                new BucketActiveRefs(
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet(),
                        sharedFiles);

        assertThat(rule.evaluate(file, activeRefs, CUTOFF_MS)).isEqualTo(Decision.KEEP_ACTIVE);
    }

    @Test
    void skipsUnknownNonSstFileUnderSharedDirectory() {
        FileMeta file = file("/kv/db/t-1/0/shared/abc-001.meta", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.SKIP_UNKNOWN);
    }

    @Test
    void skipsSstOutsideSharedDirectory() {
        FileMeta file = file("/kv/db/t-1/0/snap-5/abc-001.sst", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.SKIP_UNKNOWN);
    }

    private static FileMeta file(String path, long modificationTime) {
        return new FileMeta(new FsPath(path), 1L, modificationTime);
    }
}
