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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RuleDispatcher}. */
class RuleDispatcherTest {

    private static final String SEGMENT_ID = "11111111-1111-1111-1111-111111111111";

    private final RuleDispatcher dispatcher = new RuleDispatcher();

    @Test
    void dispatchesLogSegmentRule() {
        assertThat(dispatcher.dispatch(file("/log/db/t-1/0/" + SEGMENT_ID + "/000.log")).id())
                .isEqualTo(RuleId.LOG_SEGMENT);
    }

    @Test
    void dispatchesLogManifestRule() {
        assertThat(dispatcher.dispatch(file("/log/db/t-1/0/metadata/current.manifest")).id())
                .isEqualTo(RuleId.LOG_MANIFEST);
    }

    @Test
    void dispatchesKvSnapshotFileRule() {
        assertThat(dispatcher.dispatch(file("/kv/db/t-1/0/snap-5/001.sst")).id())
                .isEqualTo(RuleId.KV_SNAPSHOT_FILE);
    }

    @Test
    void dispatchesKvSharedSstRule() {
        assertThat(dispatcher.dispatch(file("/kv/db/t-1/0/shared/abc-001.sst")).id())
                .isEqualTo(RuleId.KV_SHARED_SST);
    }

    @Test
    void fallsBackToUnknownRule() {
        assertThat(dispatcher.dispatch(file("/random/path/file.bin")).id())
                .isEqualTo(RuleId.UNKNOWN);
    }

    @Test
    void unknownRuleSkipsConservatively() {
        FileRule rule = dispatcher.dispatch(file("/random/path/file.bin"));
        assertThat(rule.evaluate(file("/random/path/file.bin"), BucketActiveRefs.empty(), 0L))
                .isEqualTo(Decision.SKIP_UNKNOWN);
    }

    private static FileMeta file(String path) {
        return new FileMeta(new FsPath(path), 0L, 0L);
    }
}
