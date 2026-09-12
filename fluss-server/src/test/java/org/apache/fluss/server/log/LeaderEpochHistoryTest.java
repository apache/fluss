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

package org.apache.fluss.server.log;

import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.metadata.LeaderEpochOffset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests persistent log history used to find a common prefix after leader changes. */
class LeaderEpochHistoryTest {
    @TempDir private File directory;

    @Test
    void testUnknownPrefixAndSkippedEpochsSurviveRestart() throws Exception {
        File file = new File(directory, "leader-epoch-checkpoint");
        LeaderEpochHistory history = new LeaderEpochHistory(file);
        history.assign(41, 100);
        history.assign(45, 160);
        history = new LeaderEpochHistory(file);

        assertThat(history.epochForOffset(99, 200)).isEqualTo(-1);
        assertThat(history.epochForOffset(159, 200)).isEqualTo(41);
        assertThat(history.epochForOffset(160, 200)).isEqualTo(45);
        assertThat(history.endOffsetFor(40, 200)).isEmpty();
        LeaderEpochOffset preceding = history.endOffsetFor(44, 200).get();
        assertThat(preceding.epoch()).isEqualTo(41);
        assertThat(preceding.offset()).isEqualTo(160);
        assertThat(history.endOffsetFor(45, 200).get().offset()).isEqualTo(200);
    }

    @Test
    void testResumingLatestEpochPreservesItsStartAcrossRestart() throws Exception {
        File file = new File(directory, "epochs");
        LeaderEpochHistory history = new LeaderEpochHistory(file);
        history.assign(1, 0);
        history.assign(2, 10);
        history = new LeaderEpochHistory(file);
        history.assign(2, 20);
        history = new LeaderEpochHistory(file);
        assertThat(history.epochForOffset(9, 20)).isEqualTo(1);
        assertThat(history.epochForOffset(10, 20)).isEqualTo(2);
        assertThat(history.endOffsetFor(1, 20).get().offset()).isEqualTo(10);
        LeaderEpochHistory recovered = history;
        assertThatThrownBy(() -> recovered.assign(1, 20))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> recovered.assign(2, 9))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEmptyEpochDoesNotChangeLastRecordEpoch() throws Exception {
        LeaderEpochHistory history = new LeaderEpochHistory(new File(directory, "epochs"));
        history.assign(1, 0);
        history.assign(2, 10);
        history.assign(3, 10);
        assertThat(history.epochForOffset(9, 10)).isEqualTo(1);
        assertThat(history.epochForOffset(10, 10)).isEqualTo(-1);
        assertThat(history.endOffsetFor(2, 10).get().offset()).isEqualTo(10);
        assertThat(history.epochForOffset(10, 11)).isEqualTo(3);
    }

    @Test
    void testTruncateAndReplaceTailSurvivesRestart() throws Exception {
        File file = new File(directory, "epochs");
        LeaderEpochHistory history = new LeaderEpochHistory(file);
        history.assign(1, 0);
        history.assign(2, 10);
        history.assign(3, 20);
        history.truncateFromEnd(10);
        history.assign(4, 10);
        history = new LeaderEpochHistory(file);
        assertThat(history.epochForOffset(9, 15)).isEqualTo(1);
        assertThat(history.epochForOffset(10, 15)).isEqualTo(4);
        assertThat(history.endOffsetFor(3, 15).get().epoch()).isEqualTo(1);
        assertThat(history.endOffsetFor(3, 15).get().offset()).isEqualTo(10);
    }

    @Test
    void testRetentionPreservesEpochCoveringFirstRecord() throws Exception {
        File file = new File(directory, "epochs");
        LeaderEpochHistory history = new LeaderEpochHistory(file);
        history.assign(1, 0);
        history.assign(2, 10);
        history.assign(3, 20);
        history.truncateFromStart(15);
        history = new LeaderEpochHistory(file);
        assertThat(history.endOffsetFor(1, 30)).isEmpty();
        assertThat(history.epochForOffset(15, 30)).isEqualTo(2);
        assertThat(history.endOffsetFor(2, 30).get().offset()).isEqualTo(20);
    }

    @Test
    void testUntrackedAppendInvalidatesHistoryAcrossRestart() throws Exception {
        File file = new File(directory, "epochs");
        LeaderEpochHistory history = new LeaderEpochHistory(file);
        history.assign(41, 0);
        assertThat(history.epochForOffset(9, 10)).isEqualTo(41);
        history.invalidate();
        history = new LeaderEpochHistory(file);
        assertThat(history.epochForOffset(19, 20)).isEqualTo(-1);
        assertThat(history.endOffsetFor(41, 20)).isEmpty();
        history.assign(45, 20);
        assertThat(history.epochForOffset(19, 25)).isEqualTo(-1);
        assertThat(history.epochForOffset(20, 25)).isEqualTo(45);
    }

    @Test
    void testFetchedBoundaryDoesNotIdentifyUncopiedPrefix() throws Exception {
        LeaderEpochHistory history = new LeaderEpochHistory(new File(directory, "epochs"));
        history.append(Collections.singletonList(new LeaderEpochOffset(41, 0)), 10, 10);
        assertThat(history.epochForOffset(9, 10)).isEqualTo(-1);
        history.append(
                Arrays.asList(new LeaderEpochOffset(41, 0), new LeaderEpochOffset(45, 15)), 10, 20);
        assertThat(history.epochForOffset(9, 20)).isEqualTo(-1);
        assertThat(history.epochForOffset(14, 20)).isEqualTo(-1);
        assertThat(history.epochForOffset(15, 20)).isEqualTo(45);
        assertThat(history.entries(20, 20)).isEmpty();
    }

    @Test
    void testFailedCheckpointStopsHistoryUntilReload() throws Exception {
        File file = new File(directory, "epochs");
        LeaderEpochHistory history = new LeaderEpochHistory(file);
        history.assign(1, 0);
        Files.createDirectory(new File(directory, "epochs.tmp").toPath());
        assertThatThrownBy(() -> history.assign(2, 10)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> history.epochForOffset(10, 15))
                .isInstanceOf(LogStorageException.class);
        assertThatThrownBy(() -> history.assign(1, 0)).isInstanceOf(LogStorageException.class);
        assertThat(new LeaderEpochHistory(file).epochForOffset(10, 15)).isEqualTo(1);
    }
}
