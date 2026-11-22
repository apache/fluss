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

package org.apache.fluss.lake.paimon.tiering.markdone;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests idle-time boundaries independently of lake storage and wall-clock time. */
class PartitionMarkDoneTriggerTest {

    @Test
    void testIdleWindowAndLateData() {
        Map<String, Long> restored = new HashMap<>();
        restored.put("updated", 2000L);
        restored.put("open", 0L);
        PartitionMarkDoneTrigger trigger =
                new PartitionMarkDoneTrigger(
                        restored, partition -> partition.equals("open") ? 3000L : 0L, 1000L);
        assertThat(trigger.donePartitions(3000L)).isEmpty();
        assertThat(trigger.donePartitions(3001L)).containsExactly("updated");
        assertThat(restored).containsKeys("updated", "open");
        trigger.notifyPartition("updated", 3500L);
        assertThat(trigger.donePartitions(4000L)).isEmpty();
        assertThat(trigger.donePartitions(4001L)).containsExactly("open");
        assertThat(trigger.donePartitions(4501L)).containsExactly("updated");
        assertThat(trigger.trackedPartitionLastUpdateTimes()).isEmpty();
    }
}
