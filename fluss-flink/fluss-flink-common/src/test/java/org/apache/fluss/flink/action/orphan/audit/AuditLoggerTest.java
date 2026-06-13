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

package org.apache.fluss.flink.action.orphan.audit;

import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Smoke tests covering every {@link AuditLogger} entry point. The audit logger has no return
 * values, no in-memory state, and only forwards structured key/value lines to SLF4J — these tests
 * therefore only verify each emit path is reachable without throwing, which is exactly the
 * production contract callers rely on.
 */
class AuditLoggerTest {

    private final AuditLogger audit = new AuditLogger();
    private final FsPath path = new FsPath("file:///tmp/orphan/file");
    private final FsPath dir = new FsPath("file:///tmp/orphan/dir");

    @Test
    void allFileAndDirEventsEmitWithoutThrowing() {
        assertThatCode(
                        () -> {
                            audit.logCutoff(1700000000000L);
                            audit.logDeleted(path, RuleId.LOG_SEGMENT, true);
                            audit.logDeleted(path, RuleId.LOG_SEGMENT, false);
                            audit.logWouldDelete(path, RuleId.KV_SNAPSHOT_FILE);
                            audit.logDirDeleted(dir);
                            audit.logWouldDeleteDir(dir);
                            audit.logSkipUnknown(path, RuleId.UNKNOWN);
                        })
                .doesNotThrowAnyException();
    }

    @Test
    void allScopeAndBucketSkipEventsEmitWithoutThrowing() {
        assertThatCode(
                        () -> {
                            audit.logBucketAborted("tb=1,bucket=0", "manifest-parse-failed");
                            audit.logSkipDb("db", "list-tables-failed");
                            audit.logSkipTable("db", "t", "get-table-info-failed");
                            audit.logSkipPartitionList("db", "t", "list-partitions-failed");
                            audit.logSkipKvTarget(7L, 11L, "list-kv-snapshots-failed");
                            audit.logSkipKvTarget(7L, null, "list-kv-snapshots-failed");
                            audit.logSkipKvBucket(7L, 11L, 0, "empty-active-set");
                            audit.logSkipKvBucket(7L, null, 0, "empty-active-set");
                            audit.logSkipLogTarget(7L, 11L, "list-remote-manifests-failed");
                            audit.logSkipLogTarget(7L, null, "list-remote-manifests-failed");
                            audit.logSkipLogBucket(7L, 11L, 0, "no-remote-manifest");
                            audit.logSkipLogBucket(7L, null, 0, "no-remote-manifest");
                            audit.logSkipOrphanTable(dir, "opt-in-not-set");
                            audit.logSkipOrphanTableScan("db", "incomplete-table-id-set");
                            audit.logSkipOrphanPartition(dir, "opt-in-not-set");
                            audit.logSkipBucketOutOfScope(7L, 11L, "file:///other/root");
                            audit.logSkipBucketOutOfScope(7L, null, "file:///other/root");
                        })
                .doesNotThrowAnyException();
    }
}
