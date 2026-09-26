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

package org.apache.fluss.flink.action.orphan.job;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.rule.Decision;
import org.apache.fluss.flink.action.orphan.rule.RuleId;

import java.io.Serializable;

/** Fixed-size file decisions and scan coverage for one task. */
@Internal
public final class RuleSummary implements Serializable {
    public static final int SCANNED_FILES = 0;
    public static final int SCANNED_BYTES = 1;
    public static final int KEEP_ACTIVE = 2;
    public static final int NEWER_THAN_CUTOFF = 3;
    public static final int UNKNOWN_TYPE = 4;
    public static final int CANDIDATE_FILES = 5;
    public static final int UNAVAILABLE_MTIME = 6;

    private static final long serialVersionUID = 1L;
    private final long[][] rules = new long[RuleId.values().length][7];
    private long unavailableDirectories;
    private long missingDirectories;

    public void record(RuleId rule, Decision decision, long bytes) {
        long[] row = rules[rule.ordinal()];
        row[SCANNED_FILES]++;
        row[SCANNED_BYTES] += bytes;
        switch (decision) {
            case KEEP_ACTIVE:
                row[KEEP_ACTIVE]++;
                break;
            case DEFER:
                row[NEWER_THAN_CUTOFF]++;
                break;
            case SKIP_UNKNOWN:
                row[UNKNOWN_TYPE]++;
                break;
            case DELETE:
                row[CANDIDATE_FILES]++;
                break;
            case MTIME_UNAVAILABLE:
                row[UNAVAILABLE_MTIME]++;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized cleanup decision");
        }
    }

    public void recordDirectoryMtime(long mtime) {
        if (mtime <= 0 || mtime == Long.MAX_VALUE) {
            unavailableDirectories++;
        }
    }

    public void recordMissingDirectory() {
        missingDirectories++;
    }

    public void add(RuleSummary other) {
        for (int i = 0; i < rules.length; i++) {
            for (int j = 0; j < rules[i].length; j++) {
                rules[i][j] += other.rules[i][j];
            }
        }
        unavailableDirectories += other.unavailableDirectories;
        missingDirectories += other.missingDirectories;
    }

    public long value(RuleId rule, int column) {
        return rules[rule.ordinal()][column];
    }

    public long total(int column) {
        long sum = 0;
        for (long[] row : rules) {
            sum += row[column];
        }
        return sum;
    }

    public long unavailableDirectories() {
        return unavailableDirectories;
    }

    public long missingDirectories() {
        return missingDirectories;
    }

    public boolean consistent(CleanupCounters counters) {
        for (long[] row : rules) {
            if (row[SCANNED_FILES]
                    != row[KEEP_ACTIVE]
                            + row[NEWER_THAN_CUTOFF]
                            + row[UNKNOWN_TYPE]
                            + row[CANDIDATE_FILES]
                            + row[UNAVAILABLE_MTIME]) {
                return false;
            }
        }
        return total(SCANNED_FILES) == counters.scannedFiles()
                && total(CANDIDATE_FILES) == counters.plannedFiles();
    }
}
