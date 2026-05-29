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

/** Tests for {@link LogSegmentRule}. */
class LogSegmentRuleTest {

    private static final String SEGMENT_ID = "11111111-1111-1111-1111-111111111111";
    private static final long NOW = 1_700_000_000_000L;
    private static final long DAY_MS = 24L * 60L * 60L * 1000L;

    /**
     * Absolute cutoff = NOW - 1d. Files with mtime strictly less than this are deletion-eligible.
     */
    private static final long CUTOFF_MS = NOW - DAY_MS;

    private final LogSegmentRule rule = new LogSegmentRule();

    @Test
    void deleteWhenKnownExpiredAndNotInBucketActiveRefs() {
        FileMeta file =
                file("/log/db/t-1/0/" + SEGMENT_ID + "/00000000000000000000.log", NOW - 2 * DAY_MS);

        Decision decision = rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS);

        assertThat(decision).isEqualTo(Decision.DELETE);
    }

    @Test
    void keepActiveWhenInBucketActiveRefs() {
        FileMeta file =
                file("/log/db/t-1/0/" + SEGMENT_ID + "/00000000000000000000.log", NOW - 2 * DAY_MS);
        Set<String> liveFiles = new HashSet<String>();
        liveFiles.add(SEGMENT_ID + "/00000000000000000000.log");
        BucketActiveRefs activeRefs =
                new BucketActiveRefs(
                        liveFiles, Collections.<String>emptySet(), Collections.<String>emptySet());

        Decision decision = rule.evaluate(file, activeRefs, CUTOFF_MS);

        assertThat(decision).isEqualTo(Decision.KEEP_ACTIVE);
    }

    @Test
    void deferWhenMtimeAtOrAfterCutoff() {
        FileMeta file =
                file("/log/db/t-1/0/" + SEGMENT_ID + "/00000000000000000000.log", NOW - DAY_MS / 2);

        Decision decision = rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS);

        assertThat(decision).isEqualTo(Decision.DEFER);
    }

    @Test
    void skipUnknownExtension() {
        FileMeta file =
                file(
                        "/log/db/t-1/0/" + SEGMENT_ID + "/00000000000000000000.bloom",
                        NOW - 2 * DAY_MS);

        Decision decision = rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS);

        assertThat(decision).isEqualTo(Decision.SKIP_UNKNOWN);
    }

    @Test
    void skipUnknownWhenParentIsNotSegmentUuid() {
        FileMeta file = file("/log/db/t-1/0/not-a-uuid/00000000000000000000.log", NOW - 2 * DAY_MS);

        Decision decision = rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS);

        assertThat(decision).isEqualTo(Decision.SKIP_UNKNOWN);
    }

    @Test
    void deletedSuffixIsRecognizedAsKnownType() {
        FileMeta file =
                file(
                        "/log/db/t-1/0/" + SEGMENT_ID + "/00000000000000000000.log.deleted",
                        NOW - 2 * DAY_MS);

        Decision decision = rule.evaluate(file, BucketActiveRefs.empty(), CUTOFF_MS);

        assertThat(decision).isEqualTo(Decision.DELETE);
    }

    private static FileMeta file(String path, long modificationTime) {
        return new FileMeta(new FsPath(path), 100L, modificationTime);
    }
}
