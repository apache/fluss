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

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.List;

/**
 * The handler that decides which partitions can be marked done and executes the lake-specific
 * mark-done action (e.g. writing a {@code _SUCCESS} file for Paimon).
 *
 * <p>This is a lake-specific SPI created by {@link
 * org.apache.fluss.lake.writer.LakeTieringFactory#createPartitionDoneHandler}. The tiering commit
 * operator only feeds partitions that are not yet done (those still tracked in the tiering state,
 * since done partitions are removed under delete-on-done) and relies on this handler to perform the
 * idle judgement and the actual action.
 *
 * <p>Implementations MUST be idempotent: the same partition may be marked done more than once in
 * rare failure windows (e.g. action executed but the new tiering state not yet persisted before a
 * crash), so repeatedly executing the action for an already-done partition must be safe.
 *
 * @since 0.9
 */
@PublicEvolving
public interface PartitionDoneHandler extends AutoCloseable {

    /**
     * Judges which partitions can be marked done and executes the mark-done action (idempotent),
     * returning the names of the partitions that were newly marked done in this round.
     *
     * @param candidates the candidate partitions that are not yet done
     * @param currentTime the current time (System.currentTimeMillis() in processing-time mode)
     * @return the names of partitions newly marked done in this round
     * @throws Exception if the mark-done action fails
     */
    List<String> markDoneIfReady(List<PartitionDoneCandidate> candidates, long currentTime)
            throws Exception;

    @Override
    default void close() throws Exception {}
}
