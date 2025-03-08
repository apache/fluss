/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.log.remote;

import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.server.log.LogTablet;
import com.alibaba.fluss.server.log.OffsetIndex;
import com.alibaba.fluss.server.log.OffsetPosition;
import com.alibaba.fluss.server.log.TimeIndex;
import com.alibaba.fluss.server.log.TimestampOffset;
import com.alibaba.fluss.server.log.remote.RemoteLogIndexCache.Entry;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.IOUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RemoteLogIndexCache}. */
class RemoteLogIndexCacheTest extends RemoteLogTestBase {
    private RemoteLogIndexCache rlIndexCache;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        rlIndexCache = remoteLogManager.getRemoteLogIndexCache();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIndexFileNameAndLocationOnDisk(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // 1. first upload one segment to remote.
        RemoteLogSegment remoteLogSegment = copyLogSegmentToRemote(logTablet, remoteLogStorage, 0);

        Entry entry = rlIndexCache.getIndexEntry(remoteLogSegment);
        Path offsetIndexPath = entry.offsetIndex().file().toPath();
        Path timeIndexPath = entry.timeIndex().file().toPath();
        String expectedOffsetIndexFileName =
                remoteLogSegment.remoteLogStartOffset()
                        + "_"
                        + remoteLogSegment.remoteLogSegmentId()
                        + ".index";
        String expectedTimestampFileName =
                remoteLogSegment.remoteLogStartOffset()
                        + "_"
                        + remoteLogSegment.remoteLogSegmentId()
                        + ".timeindex";
        assertThat(offsetIndexPath.getFileName().toString()).isEqualTo(expectedOffsetIndexFileName);
        assertThat(timeIndexPath.getFileName().toString()).isEqualTo(expectedTimestampFileName);
        assertThat(offsetIndexPath.getParent().getFileName().toString())
                .isEqualTo(FlussPaths.REMOTE_LOG_INDEX_LOCAL_CACHE);
        assertThat(timeIndexPath.getParent().getFileName().toString())
                .isEqualTo(RemoteLogIndexCache.DIR_NAME);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFetchIndexFromRemoteLogStorage(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // 1. first upload one segment to remote.
        RemoteLogSegment remoteLogSegment = copyLogSegmentToRemote(logTablet, remoteLogStorage, 0);

        OffsetIndex offsetIndex = rlIndexCache.getIndexEntry(remoteLogSegment).offsetIndex();
        OffsetPosition offsetPosition1 = offsetIndex.entry(1);
        int resultPosition =
                rlIndexCache.lookupPosition(remoteLogSegment, offsetPosition1.getOffset());
        assertThat(offsetPosition1.getPosition()).isEqualTo(resultPosition);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFetchTimeIndexFromRemoteLogStorage(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // 1. first upload one segment to remote.
        RemoteLogSegment remoteLogSegment = copyLogSegmentToRemote(logTablet, remoteLogStorage, 0);
        TimeIndex timeIndex = rlIndexCache.getIndexEntry(remoteLogSegment).timeIndex();
        TimestampOffset timestampOffset = timeIndex.entry(0);

        OffsetIndex offsetIndex = rlIndexCache.getIndexEntry(remoteLogSegment).offsetIndex();
        OffsetPosition offsetPosition = offsetIndex.lookup(timestampOffset.offset);
        long resultOffset =
                rlIndexCache.lookupOffsetForTimestamp(remoteLogSegment, timestampOffset.timestamp);
        assertThat(offsetPosition.getOffset()).isEqualTo(resultOffset);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPositionForNonExistingIndexFromRemoteStorage(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // 1. first upload one segment to remote.
        RemoteLogSegment remoteLogSegment = copyLogSegmentToRemote(logTablet, remoteLogStorage, 0);

        OffsetIndex offsetIndex = rlIndexCache.getIndexEntry(remoteLogSegment).offsetIndex();
        int lastOffsetIndex =
                rlIndexCache.lookupPosition(remoteLogSegment, offsetIndex.lastOffset());
        long greaterOffsetThatLastOffset = offsetIndex.lastOffset() + 1;
        assertThat(rlIndexCache.lookupPosition(remoteLogSegment, greaterOffsetThatLastOffset))
                .isEqualTo(lastOffsetIndex);

        // offsetIndex.lookup() returns OffsetPosition(baseOffset, 0) for offsets smaller
        // than the least entry in the offset index.
        OffsetPosition nonExistentOffsetPosition = new OffsetPosition(0, 0);
        long lowerOffsetThatBaseOffset = offsetIndex.baseOffset() - 1;
        assertThat(rlIndexCache.lookupPosition(remoteLogSegment, lowerOffsetThatBaseOffset))
                .isEqualTo(nonExistentOffsetPosition.getPosition());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testGetIndexAfterCacheClose(boolean partitionTable) throws Exception {
        // close existing cache created in test setup before creating a new one.
        IOUtils.closeQuietly(rlIndexCache, "RemoteLogIndexCache created for unit test");

        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        rlIndexCache = new RemoteLogIndexCache(1024 * 1024L, remoteLogStorage, tempDir);
        RemoteLogSegment remoteLogSegment = copyLogSegmentToRemote(logTablet, remoteLogStorage, 0);
        assertThat(rlIndexCache.getInternalCache().asMap().size()).isEqualTo(0);
        rlIndexCache.getIndexEntry(remoteLogSegment);
        assertThat(rlIndexCache.getInternalCache().asMap().size()).isEqualTo(1);

        rlIndexCache.close();
        assertThatThrownBy(() -> rlIndexCache.getIndexEntry(remoteLogSegment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Unable to fetch index for remote-segment-id = "
                                + remoteLogSegment.remoteLogSegmentId()
                                + ". Instance is already closed.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCacheEntryIsDeletedOnRemoval(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // 1. first upload two segment to remote.
        RemoteLogSegment remoteLogSegment1 = copyLogSegmentToRemote(logTablet, remoteLogStorage, 0);
        RemoteLogSegment remoteLogSegment2 = copyLogSegmentToRemote(logTablet, remoteLogStorage, 1);
        rlIndexCache.getIndexEntry(remoteLogSegment1);
        rlIndexCache.getIndexEntry(remoteLogSegment2);

        File file = rlIndexCache.cacheDir();
        File[] files = file.listFiles();
        assertThat(files.length).isEqualTo(4);
        rlIndexCache.remove(remoteLogSegment1.remoteLogSegmentId());
        files = file.listFiles();
        assertThat(files.length).isEqualTo(2);
        rlIndexCache.remove(remoteLogSegment2.remoteLogSegmentId());
        files = file.listFiles();
        assertThat(files.length).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRemoveNonExistentItem(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // 1. first upload one segment to remote.
        RemoteLogSegment remoteLogSegment = copyLogSegmentToRemote(logTablet, remoteLogStorage, 0);
        rlIndexCache.getIndexEntry(remoteLogSegment);

        assertThat(rlIndexCache.getInternalCache().asMap().size()).isEqualTo(1);
        rlIndexCache.remove(UUID.randomUUID());
        assertThat(rlIndexCache.getInternalCache().asMap().size()).isEqualTo(1);
        assertThat(
                        rlIndexCache
                                .getInternalCache()
                                .asMap()
                                .containsKey(remoteLogSegment.remoteLogSegmentId()))
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRemoveMultiItems(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // 1. first upload two segment to remote.
        RemoteLogSegment remoteLogSegment1 = copyLogSegmentToRemote(logTablet, remoteLogStorage, 0);
        RemoteLogSegment remoteLogSegment2 = copyLogSegmentToRemote(logTablet, remoteLogStorage, 1);
        rlIndexCache.getIndexEntry(remoteLogSegment1);
        rlIndexCache.getIndexEntry(remoteLogSegment2);

        assertThat(rlIndexCache.getInternalCache().asMap().size()).isEqualTo(2);
        rlIndexCache.removeAll(
                Arrays.asList(
                        remoteLogSegment1.remoteLogSegmentId(),
                        remoteLogSegment2.remoteLogSegmentId(),
                        UUID.randomUUID()));
        assertThat(rlIndexCache.getInternalCache().asMap().size()).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCleanerThreadShutdown(boolean partitionTable) throws Exception {
        // cache is empty at beginning.
        assertThat(rlIndexCache.getInternalCache().asMap().isEmpty()).isTrue();
        assertThat(rlIndexCache.cleanerThread().isDaemon()).isTrue();
        assertThat(rlIndexCache.cleanerThread().isAlive()).isTrue();

        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        // upload one segment to remote.
        RemoteLogSegment remoteLogSegment = copyLogSegmentToRemote(logTablet, remoteLogStorage, 0);
        Entry entry = rlIndexCache.getIndexEntry(remoteLogSegment);
        assertThat(entry.offsetIndex().file().exists()).isTrue();
        // trigger cleanup.
        rlIndexCache.remove(remoteLogSegment.remoteLogSegmentId());
        assertThat(entry.isCleanStarted()).isTrue();
        assertThat(entry.offsetIndex().file().exists()).isFalse();

        // close the cache properly.
        rlIndexCache.close();
        assertThat(rlIndexCache.cleanerThread().isRunning()).isFalse();
        // it's possible the thread isn't dead when isRunning=false, so wait the thread to die here
        rlIndexCache.cleanerThread().join();
        assertThat(rlIndexCache.cleanerThread().isAlive()).isFalse();
    }
}
