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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the deletion cutoff for missing or invalid modification times. */
class MtimePolicyTest {
    @Test
    void retainsFilesWithUnavailableModificationTime() {
        for (long time : new long[] {Long.MIN_VALUE, -1, 0, Long.MAX_VALUE}) {
            assertThat(MtimePolicy.evaluateInactiveFile(time, 100))
                    .isEqualTo(Decision.MTIME_UNAVAILABLE);
        }
    }

    @Test
    void onlyDeletesFilesStrictlyBeforeCutoff() {
        assertThat(MtimePolicy.evaluateInactiveFile(99, 100)).isEqualTo(Decision.DELETE);
        assertThat(MtimePolicy.evaluateInactiveFile(100, 100)).isEqualTo(Decision.DEFER);
        assertThat(MtimePolicy.evaluateInactiveFile(101, 100)).isEqualTo(Decision.DEFER);
    }
}
