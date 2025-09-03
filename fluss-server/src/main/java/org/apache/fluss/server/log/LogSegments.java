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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class encapsulates a thread-safe navigable map of LogSegment instances and provides the
 * required read and write behavior on the map.
 */
@ThreadSafe
@Internal
public final class LogSegments {

    private final TableBucket tableBucket;

    private final ConcurrentNavigableMap<Long, LogSegment> segments;

    public LogSegments(TableBucket tableBucket) {
        this.tableBucket = tableBucket;
        segments = new ConcurrentSkipListMap<>();
    }

    public boolean isEmpty() {
        return segments.isEmpty();
    }

    public LogSegment add(LogSegment logSegment) {
        return segments.put(logSegment.getBaseOffset(), logSegment);
    }

    public void remove(long offset) {
        segments.remove(offset);
    }

    public void clear() {
        segments.clear();
    }

    public void close() {
        for (LogSegment logSegment : segments.values()) {
            logSegment.close();
        }
    }

    /** Close the handlers for all segments. */
    public void closeHandlers() {
        for (LogSegment logSegment : segments.values()) {
            logSegment.closeHandlers();
        }
    }

    /**
     * Update the directory reference for the log and indices of all segments.
     *
     * @param dir the renamed directory
     */
    public void updateParentDir(File dir) {
        segments.values().forEach(logSegment -> logSegment.updateParentDir(dir));
    }

    public int numberOfSegments() {
        return segments.size();
    }

    public List<Long> baseOffsets() {
        return segments.values().stream()
                .map(LogSegment::getBaseOffset)
                .collect(Collectors.toList());
    }

    /**
     * @param offset the segment to be checked
     * @return true if a segment exists at the provided offset, false otherwise.
     */
    public boolean contains(long offset) {
        return segments.containsKey(offset);
    }

    /**
     * Retrieves a segment at the specified offset.
     *
     * @param offset the segment to be retrieved
     * @return the segment if it exists, otherwise None.
     */
    public Optional<LogSegment> get(long offset) {
        return Optional.ofNullable(segments.get(offset));
    }

    /** Returns all the log segments in base offset ascending order. */
    public List<LogSegment> values() {
        return new ArrayList<>(segments.values());
    }

    /**
     * Get log segments in the range [from, to).
     *
     * @param from the start offset
     * @param to the end offset
     * @return the log segments in the range [from, to)
     */
    public List<LogSegment> values(long from, long to) {
        if (from == to) {
            return new ArrayList<>();
        } else if (from > to) {
            throw new IllegalArgumentException(
                    "Invalid log segment range: requested segments in "
                            + tableBucket
                            + " from offset "
                            + from
                            + " which is greater than limit offset "
                            + to);
        } else {
            ConcurrentNavigableMap<Long, LogSegment> fromToMap =
                    Optional.ofNullable(segments.floorKey(from))
                            .map(floor -> segments.subMap(floor, to))
                            .orElse(segments.headMap(to));
            return new ArrayList<>(fromToMap.values());
        }
    }

    public List<LogSegment> nonActiveLogSegmentsFrom(long from) {
        LogSegment activeSegment = activeSegment();
        if (from > activeSegment.getBaseOffset()) {
            return new ArrayList<>();
        } else {
            return values(from, activeSegment.getBaseOffset());
        }
    }

    /**
     * @return the entry associated with the greatest offset less than or equal to the given offset,
     *     if it exists.
     */
    private Optional<Map.Entry<Long, LogSegment>> floorEntry(long offset) {
        return Optional.ofNullable(segments.floorEntry(offset));
    }

    /**
     * @return the log segment with the greatest offset less than or equal to the given offset, if
     *     it exists.
     */
    public Optional<LogSegment> floorSegment(long offset) {
        return floorEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * @return the entry associated with the greatest offset, if it exists.
     */
    public Optional<Map.Entry<Long, LogSegment>> lastEntry() {
        return Optional.ofNullable(segments.lastEntry());
    }

    /**
     * @return the log segment with the greatest offset, if it exists.
     */
    public Optional<LogSegment> lastSegment() {
        return Optional.ofNullable(segments.lastEntry()).map(Map.Entry::getValue);
    }

    /**
     * @return the entry associated with the greatest offset, if it exists.
     */
    public Optional<Map.Entry<Long, LogSegment>> firstEntry() {
        return Optional.ofNullable(segments.firstEntry());
    }

    /**
     * @return the log segment with the greatest offset, if it exists.
     */
    public Optional<LogSegment> firstSegment() {
        return Optional.ofNullable(segments.firstEntry()).map(Map.Entry::getValue);
    }

    /**
     * @return the base offset of the log segment associated with the smallest offset, if it exists.
     */
    Optional<Long> firstSegmentBaseOffset() {
        return firstSegment().map(LogSegment::getBaseOffset);
    }

    /**
     * @return the entry associated with the greatest offset strictly less than the given offset, if
     *     it exists.
     */
    private Optional<Map.Entry<Long, LogSegment>> lowerEntry(long offset) {
        return Optional.ofNullable(segments.lowerEntry(offset));
    }

    /**
     * @return the log segment with the greatest offset strictly less than the given offset, if it
     *     exists.
     */
    public Optional<LogSegment> lowerSegment(long offset) {
        return lowerEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * @return the entry associated with the smallest offset strictly greater than the given offset,
     *     if it exists.
     */
    public Optional<Map.Entry<Long, LogSegment>> higherEntry(long offset) {
        return Optional.ofNullable(segments.higherEntry(offset));
    }

    /**
     * @return the log segment with the smallest offset strictly greater than the given offset, if
     *     it exists.
     */
    public Optional<LogSegment> higherSegment(long offset) {
        return higherEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * @return an iterable with log segments ordered from the lowest base offset to highest, each
     *     segment returned has a base offset strictly greater than the provided baseOffset.
     */
    public List<LogSegment> higherSegments(long baseOffset) {
        if (segments.higherKey(baseOffset) == null) {
            return new ArrayList<>();
        }
        long higherOffset = segments.higherKey(baseOffset);
        ConcurrentNavigableMap<Long, LogSegment> higherMap = segments.tailMap(higherOffset);
        return new ArrayList<>(higherMap.values());
    }

    /** The active segment that is currently taking appends. */
    public LogSegment activeSegment() {
        return lastSegment().get();
    }

    public long sizeInBytes() {
        return segments.values().stream().mapToLong(LogSegment::getSizeInBytes).sum();
    }
}
