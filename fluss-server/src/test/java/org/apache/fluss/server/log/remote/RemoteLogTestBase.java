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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaTestBase;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH_PA_2024;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.assertj.core.api.Assertions.assertThat;

/** Test base for remote log. */
public class RemoteLogTestBase extends ReplicaTestBase {
    @Override
    public Configuration getServerConf() {
        Configuration conf = new Configuration();
        // set index interval size to 1 byte to make sure the offset index file always update
        // immediately.
        conf.set(ConfigOptions.LOG_INDEX_INTERVAL_SIZE, MemorySize.parse("1b"));

        conf.set(ConfigOptions.REMOTE_LOG_INDEX_FILE_CACHE_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.REMOTE_FS_WRITE_BUFFER_SIZE, MemorySize.parse("10b"));
        return conf;
    }

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
    }

    protected LogTablet makeLogTabletAndAddSegments(boolean partitionTable) throws Exception {
        if (partitionTable) {
            return makeReplicaAndAddSegments(
                            DATA1_PHYSICAL_TABLE_PATH_PA_2024,
                            new TableBucket(DATA1_TABLE_ID, (long) 0, 0),
                            5)
                    .getLogTablet();
        } else {
            return makeReplicaAndAddSegments(
                            DATA1_PHYSICAL_TABLE_PATH, new TableBucket(DATA1_TABLE_ID, 0), 5)
                    .getLogTablet();
        }
    }

    private Replica makeReplicaAndAddSegments(
            PhysicalTablePath physicalTablePath, TableBucket tb, int segmentSize) throws Exception {
        Replica replica = makeLogReplica(physicalTablePath, tb);
        replica.makeLeader(
                new NotifyLeaderAndIsrData(
                        physicalTablePath,
                        tb,
                        Collections.singletonList(0),
                        new LeaderAndIsr(0, 0, Collections.singletonList(0), 0, 0)));
        addMultiSegmentsToLogTablet(replica.getLogTablet(), segmentSize);
        return replica;
    }

    protected static RemoteLogSegment copyLogSegmentToRemote(
            LogTablet logTablet, RemoteLogStorage remoteLogStorage, int segmentIndex)
            throws Exception {
        PhysicalTablePath tp = logTablet.getPhysicalTablePath();
        TableBucket tb = logTablet.getTableBucket();
        List<LogSegment> segments = logTablet.getSegments();
        LogSegment segment = segments.get(segmentIndex);
        assertThat(segments.size()).isGreaterThan(segmentIndex);
        long nextOffset = segments.get(segmentIndex + 1).getBaseOffset();
        File writerIdSnapshotFile =
                logTablet.writerStateManager().fetchSnapshot(nextOffset).orElse(null);
        LogSegmentFiles logSegmentFiles =
                new LogSegmentFiles(
                        segment.getFileLogRecords().file().toPath(),
                        segment.offsetIndex().file().toPath(),
                        segment.timeIndex().file().toPath(),
                        writerIdSnapshotFile.toPath());

        UUID remoteLogSegmentId = UUID.randomUUID();
        RemoteLogSegment remoteLogSegment =
                RemoteLogSegment.Builder.builder()
                        .remoteLogSegmentId(remoteLogSegmentId)
                        .remoteLogStartOffset(segment.getBaseOffset())
                        .remoteLogEndOffset(nextOffset)
                        .maxTimestamp(segment.maxTimestampSoFar())
                        .segmentSizeInBytes(segment.getFileLogRecords().sizeInBytes())
                        .tableBucket(tb)
                        .physicalTablePath(tp)
                        .build();

        remoteLogStorage.copyLogSegmentFiles(remoteLogSegment, logSegmentFiles);
        return remoteLogSegment;
    }

    protected RemoteLogTablet buildRemoteLogTablet(LogTablet logTablet) {
        return new RemoteLogTablet(
                logTablet.getPhysicalTablePath(),
                logTablet.getTableBucket(),
                conf.get(ConfigOptions.TABLE_LOG_TTL).toMillis());
    }

    protected static List<RemoteLogSegment> createRemoteLogSegmentList(LogTablet logTablet) {
        return logTablet.getSegments().stream()
                .map(
                        segment -> {
                            try {
                                return RemoteLogSegment.Builder.builder()
                                        .remoteLogSegmentId(UUID.randomUUID())
                                        .remoteLogStartOffset(segment.getBaseOffset())
                                        .remoteLogEndOffset(segment.getBaseOffset() + DATA1.size())
                                        .maxTimestamp(segment.maxTimestampSoFar())
                                        .segmentSizeInBytes(
                                                segment.getFileLogRecords().sizeInBytes())
                                        .tableBucket(logTablet.getTableBucket())
                                        .physicalTablePath(logTablet.getPhysicalTablePath())
                                        .build();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .collect(Collectors.toList());
    }
}
