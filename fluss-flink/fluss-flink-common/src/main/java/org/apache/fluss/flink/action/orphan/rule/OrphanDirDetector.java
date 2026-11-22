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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;

import javax.annotation.Nullable;

import java.util.Set;

/**
 * Detects orphan table and partition directories by ID guard.
 *
 * <p>A directory is an orphan candidate iff its parsed ID is not in the active set and does not
 * exceed the last-known maximum (conservatively treating IDs above the max as freshly allocated).
 * Unrecognizable directory names are never flagged.
 */
@Internal
public final class OrphanDirDetector {

    private OrphanDirDetector() {}

    /**
     * Returns {@code true} if the directory name matches {@code {name}-{tableId}} and the parsed ID
     * is not in {@code activeTableIds} and is {@code <= maxKnownTableId}.
     */
    public static boolean isOrphanTable(
            String dirName, Set<Long> activeTableIds, long maxKnownTableId) {
        Long parsed = parseTableId(dirName);
        if (parsed == null) {
            return false;
        }
        if (activeTableIds.contains(parsed)) {
            return false;
        }
        return parsed <= maxKnownTableId;
    }

    /**
     * Returns {@code true} if the directory name matches {@code {name}-p{partitionId}} and the
     * parsed ID is not in {@code activePartitionIds} and is {@code <= maxKnownPartitionId}.
     */
    public static boolean isOrphanPartition(
            String dirName, Set<Long> activePartitionIds, long maxKnownPartitionId) {
        Long parsed = parsePartitionId(dirName);
        if (parsed == null) {
            return false;
        }
        if (activePartitionIds.contains(parsed)) {
            return false;
        }
        return parsed <= maxKnownPartitionId;
    }

    @VisibleForTesting
    @Nullable
    static Long parseTableId(String dirName) {
        int dash = dirName.lastIndexOf('-');
        if (dash <= 0 || dash == dirName.length() - 1) {
            return null;
        }
        String idPart = dirName.substring(dash + 1);
        for (int i = 0; i < idPart.length(); i++) {
            if (!Character.isDigit(idPart.charAt(i))) {
                return null;
            }
        }
        try {
            return Long.parseLong(idPart);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @VisibleForTesting
    @Nullable
    static Long parsePartitionId(String dirName) {
        int dashP = dirName.lastIndexOf("-p");
        if (dashP <= 0 || dashP == dirName.length() - 2) {
            return null;
        }
        String idPart = dirName.substring(dashP + 2);
        for (int i = 0; i < idPart.length(); i++) {
            if (!Character.isDigit(idPart.charAt(i))) {
                return null;
            }
        }
        try {
            return Long.parseLong(idPart);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
