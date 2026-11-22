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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LogManifestRule}. */
class LogManifestRuleTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final long DAY_MS = 24L * 60L * 60L * 1000L;
    private static final long CUTOFF_MS = NOW - DAY_MS;

    /** Default-conservative rule (allowDeleteManifest=false): never deletes manifests. */
    private final LogManifestRule defaultRule = new LogManifestRule();

    /** Opt-in rule (allowDeleteManifest=true): uses active-set + cutoff semantics. */
    private final LogManifestRule optInRule = new LogManifestRule(true);

    @Test
    void deletesExpiredNonActiveManifest() {
        FileMeta file = file("/log/db/t-1/0/metadata/old.manifest", NOW - 2 * DAY_MS);
        BucketActiveRefs activeRefs =
                new BucketActiveRefs(
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet(),
                        Collections.singleton("/log/db/t-1/0/metadata/current.manifest"));

        assertThat(optInRule.evaluate(file, activeRefs, CUTOFF_MS)).isEqualTo(Decision.DELETE);
    }

    @Test
    void keepsManifestListedInActiveManifestPaths() {
        FileMeta file = file("/log/db/t-1/0/metadata/active.manifest", NOW - 2 * DAY_MS);
        BucketActiveRefs activeRefs =
                new BucketActiveRefs(
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet(),
                        Collections.singleton("/log/db/t-1/0/metadata/active.manifest"));

        assertThat(optInRule.evaluate(file, activeRefs, CUTOFF_MS)).isEqualTo(Decision.KEEP_ACTIVE);
    }

    @Test
    void defersManifestWhenMtimeAtOrAfterCutoff() {
        FileMeta file = file("/log/db/t-1/0/metadata/fresh.manifest", NOW - DAY_MS / 2);

        assertThat(optInRule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.DEFER);
    }

    @Test
    void skipsUnknownFileInMetadataDirectory() {
        FileMeta file = file("/log/db/t-1/0/metadata/readme.txt", NOW - 2 * DAY_MS);

        assertThat(defaultRule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.SKIP_UNKNOWN);
        assertThat(optInRule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.SKIP_UNKNOWN);
    }

    @Test
    void skipsManifestOutsideMetadataDirectory() {
        FileMeta file =
                file(
                        "/log/db/t-1/0/11111111-1111-1111-1111-111111111111/file.manifest",
                        NOW - 2 * DAY_MS);

        assertThat(defaultRule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.SKIP_UNKNOWN);
        assertThat(optInRule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.SKIP_UNKNOWN);
    }

    @Test
    void defaultRuleNeverDeletesEvenWhenStaleAndOrphan() {
        // mtime=0L (very old); active-set lists a different manifest as active; under the
        // default-conservative branch the rule MUST still return KEEP_ACTIVE rather than DELETE.
        FileMeta file = file("/log/db/t-1/0/metadata/orphan.manifest", 0L);
        BucketActiveRefs activeRefs =
                new BucketActiveRefs(
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet(),
                        Collections.singleton("/log/db/t-1/0/metadata/current.manifest"));

        assertThat(defaultRule.evaluate(file, activeRefs, CUTOFF_MS))
                .isEqualTo(Decision.KEEP_ACTIVE);
    }

    private static FileMeta file(String path, long modificationTime) {
        return new FileMeta(new FsPath(path), 1L, modificationTime);
    }
}
