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

/** Tests for {@link KvSharedSstRule}. */
class KvSharedSstRuleTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final long DAY_MS = 24L * 60L * 60L * 1000L;
    private static final long CUTOFF_MS = NOW - DAY_MS;
    private static final String SHARED_UUID = "83cc543d-5050-4f75-b15d-3f8a466cf107";

    private final KvSharedSstRule rule = new KvSharedSstRule();

    @Test
    void deletesExpiredUnreferencedSharedSst() {
        FileMeta file = file("/kv/db/t-1/0/shared/abc-001.sst", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, sharedSstActiveRefs("other-file.sst"), CUTOFF_MS))
                .isEqualTo(Decision.DELETE);
    }

    @Test
    void defersYoungUnreferencedSharedSst() {
        FileMeta file = file("/kv/db/t-1/0/shared/abc-001.sst", NOW - DAY_MS / 2);

        assertThat(rule.evaluate(file, sharedSstActiveRefs("other-file.sst"), CUTOFF_MS))
                .isEqualTo(Decision.DEFER);
    }

    @Test
    void keepsExpiredSharedSstWhenActiveSetIsUnknown() {
        FileMeta file = file("/kv/db/t-1/0/shared/abc-001.sst", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.KEEP_ACTIVE);
    }

    @Test
    void deletesExpiredSharedSstWhenActiveSetIsKnownEmpty() {
        FileMeta file = file("/kv/db/t-1/0/shared/abc-001.sst", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, BucketActiveRefs.knownEmpty(), CUTOFF_MS))
                .isEqualTo(Decision.DELETE);
    }

    @Test
    void defersYoungSharedSstWhenActiveSetIsKnownEmpty() {
        FileMeta file = file("/kv/db/t-1/0/shared/abc-001.sst", NOW - DAY_MS / 2);

        assertThat(rule.evaluate(file, BucketActiveRefs.knownEmpty(), CUTOFF_MS))
                .isEqualTo(Decision.DEFER);
    }

    @Test
    void keepsReferencedSharedSst() {
        FileMeta file = file("/kv/db/t-1/0/shared/abc-001.sst", NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, sharedSstActiveRefs("abc-001.sst"), CUTOFF_MS))
                .isEqualTo(Decision.KEEP_ACTIVE);
    }

    @Test
    void deletesExpiredUnreferencedSharedUuid() {
        FileMeta file = file("/kv/db/t-1/0/shared/" + SHARED_UUID, NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, sharedSstActiveRefs("other-file"), CUTOFF_MS))
                .isEqualTo(Decision.DELETE);
    }

    @Test
    void defersYoungUnreferencedSharedUuid() {
        FileMeta file = file("/kv/db/t-1/0/shared/" + SHARED_UUID, NOW - DAY_MS / 2);

        assertThat(rule.evaluate(file, sharedSstActiveRefs("other-file"), CUTOFF_MS))
                .isEqualTo(Decision.DEFER);
    }

    @Test
    void keepsReferencedSharedUuid() {
        FileMeta file = file("/kv/db/t-1/0/shared/" + SHARED_UUID, NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, sharedSstActiveRefs(SHARED_UUID), CUTOFF_MS))
                .isEqualTo(Decision.KEEP_ACTIVE);
    }

    @Test
    void keepsSharedUuidWhenActiveSetIsEmpty() {
        FileMeta file = file("/kv/db/t-1/0/shared/" + SHARED_UUID, NOW - 2 * DAY_MS);

        assertThat(rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS))
                .isEqualTo(Decision.KEEP_ACTIVE);
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

    private static BucketActiveRefs sharedSstActiveRefs(String... fileNames) {
        Set<String> sharedFiles = new HashSet<>(Arrays.asList(fileNames));
        return new BucketActiveRefs(
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                sharedFiles);
    }

    private static FileMeta file(String path, long modificationTime) {
        return new FileMeta(new FsPath(path), 1L, modificationTime);
    }
}
