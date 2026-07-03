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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KvSnapshotFileRule}. */
class KvSnapshotFileRuleTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final long DAY_MS = 24L * 60L * 60L * 1000L;

    /** Absolute cutoff = NOW - 1d. Files with mtime strictly less are deletion-eligible. */
    private static final long CUTOFF_MS = NOW - DAY_MS;

    private final KvSnapshotFileRule rule = new KvSnapshotFileRule();

    @Test
    void deletesExpiredSnapshotFileOutsideBucketActiveRefs() {
        FileMeta file = file("/kv/db/t-1/0/snap-5/001.sst", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, kvActiveSnapDirs("snap-7", "snap-9"), CUTOFF_MS))
                .isEqualTo(Decision.DELETE);
    }

    @Test
    void keepsActiveSnapshotFile() {
        FileMeta file = file("/kv/db/t-1/0/snap-5/001.sst", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, kvActiveSnapDirs("snap-5"), CUTOFF_MS))
                .isEqualTo(Decision.KEEP_ACTIVE);
    }

    @Test
    void defersSnapshotWhenMtimeAtOrAfterCutoff() {
        FileMeta file = file("/kv/db/t-1/0/snap-5/001.sst", NOW - DAY_MS / 2);

        assertThat(rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.DEFER);
    }

    @Test
    void skipsUnknownFileNameInsideSnapshotDirectory() {
        FileMeta file = file("/kv/db/t-1/0/snap-5/data.bloom", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.SKIP_UNKNOWN);
    }

    @Test
    void skipsUnknownWhenParentIsNotSnapshotDirectory() {
        FileMeta file = file("/kv/db/t-1/0/random/001.sst", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.SKIP_UNKNOWN);
    }

    @Test
    void recognizesExactPrefixAndSuffixBasedSnapshotFiles() {
        String[] fileNames = {
            "_METADATA", "MANIFEST-001", "OPTIONS-002", "CURRENT", "LOG", "IDENTITY", "001.log"
        };

        for (String fileName : fileNames) {
            FileMeta file = file("/kv/db/t-1/0/snap-5/" + fileName, NOW - 2 * DAY_MS);
            assertThat(rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                    .as("file=%s", fileName)
                    .isEqualTo(Decision.DELETE);
        }
    }

    @Test
    void retainedNonLatestSnapshotIsActive() {
        // Simulates kv.snapshot.num-retained=2, latest snapId=10, retained={9,10}: the active set
        // is the full retained set (server emits RETAINED ∪ STILL_IN_USE), so a file under snap-9
        // MUST be classified as KEEP_ACTIVE even if it's old enough to clear the cutoff. Cutoff is
        // set to NOW (an aggressive value) to prove the active-set check short-circuits before the
        // age check.
        FileMeta file =
                new FileMeta(new FsPath("oss://b/kv/db/t-7/0/snap-9/_METADATA"), 1024L, 200L);

        Decision decision = rule.evaluate(file, kvActiveSnapDirs("snap-9", "snap-10"), NOW);

        assertThat(decision).isEqualTo(Decision.KEEP_ACTIVE);
    }

    private static BucketActiveRefs kvActiveSnapDirs(String... snapDirs) {
        Set<String> activeDirs = new HashSet<String>(Arrays.asList(snapDirs));
        return new BucketActiveRefs(
                Collections.<String>emptySet(), activeDirs, Collections.<String>emptySet());
    }

    private static FileMeta file(String path, long modificationTime) {
        return new FileMeta(new FsPath(path), 1L, modificationTime);
    }
}
