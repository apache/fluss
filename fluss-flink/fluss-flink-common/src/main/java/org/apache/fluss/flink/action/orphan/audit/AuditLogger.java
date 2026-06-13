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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.flink.action.orphan.rule.RuleId;
import org.apache.fluss.fs.FsPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Structured audit log writer for the orphan files cleanup action.
 *
 * <p>The dedicated logger name {@code fluss.orphan.audit} can be routed to a separate sink (e.g.
 * SLS) by deployment-specific log4j configuration.
 */
@Internal
public final class AuditLogger {

    private static final Logger AUDIT = LoggerFactory.getLogger("fluss.orphan.audit");

    /**
     * Formats cutoff epoch-ms back to the {@code yyyy-MM-dd HH:mm:ss} CLI grammar in the server's
     * local zone, so the audit line and the original {@code --older-than} value can be compared
     * verbatim.
     */
    private static final DateTimeFormatter CUTOFF_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    /**
     * One-shot startup event recording the frozen file cutoff that drives this run's deletion
     * decisions. Emitted before any other audit line so log readers can recover the exact threshold
     * without having to re-parse the original CLI arguments.
     */
    public void logCutoff(long olderThanMillis) {
        AUDIT.info(
                "action=cutoff older_than_iso={} older_than_ms={} ts={}",
                CUTOFF_FORMATTER.format(Instant.ofEpochMilli(olderThanMillis)),
                olderThanMillis,
                Instant.now());
    }

    public void logDeleted(FsPath path, RuleId ruleId, boolean ok) {
        AUDIT.info("action=deleted rule={} path={} ok={} ts={}", ruleId, path, ok, Instant.now());
    }

    public void logWouldDelete(FsPath path, RuleId ruleId) {
        AUDIT.info("action=would_delete rule={} path={} ts={}", ruleId, path, Instant.now());
    }

    public void logDirDeleted(FsPath dir) {
        AUDIT.info("action=dir_deleted path={} ts={}", dir, Instant.now());
    }

    public void logWouldDeleteDir(FsPath dir) {
        AUDIT.info("action=would_delete_dir path={} ts={}", dir, Instant.now());
    }

    public void logSkipUnknown(FsPath path, RuleId ruleId) {
        AUDIT.warn("action=skip_unknown rule={} path={} ts={}", ruleId, path, Instant.now());
    }

    public void logBucketAborted(String bucketStr, String reason) {
        AUDIT.error(
                "action=bucket_aborted bucket={} reason={} ts={}",
                bucketStr,
                reason,
                Instant.now());
    }

    /** Skip an entire database during scope enumeration due to listTables failure. */
    public void logSkipDb(String dbName, String reason) {
        AUDIT.warn("action=skip_db reason={} db={} ts={}", reason, dbName, Instant.now());
    }

    /** Skip a single table during scope enumeration due to getTableInfo or RPC failure. */
    public void logSkipTable(String dbName, String tableName, String reason) {
        AUDIT.warn(
                "action=skip_table reason={} db={} table={} ts={}",
                reason,
                dbName,
                tableName,
                Instant.now());
    }

    /**
     * Skip listPartitionInfos for a table due to RPC failure (both active-partition cleanup and
     * orphan-partition scan are suppressed for this table).
     */
    public void logSkipPartitionList(String dbName, String tableName, String reason) {
        AUDIT.warn(
                "action=skip_partition_list reason={} db={} table={} ts={}",
                reason,
                dbName,
                tableName,
                Instant.now());
    }

    /**
     * Skip KV cleanup for one (tableId, partitionId) target — emitted when {@code ListKvSnapshots}
     * fails after retries. {@code partitionId} is null for non-partitioned tables.
     */
    public void logSkipKvTarget(long tableId, Long partitionId, String reason) {
        AUDIT.warn(
                "action=skip_kv_target reason={} table_id={} partition_id={} ts={}",
                reason,
                tableId,
                partitionId,
                Instant.now());
    }

    /**
     * Skip KV cleanup for a single bucket whose {@code ListKvSnapshots} response carried no
     * active-snapshot entries. Empty per-bucket active set is treated as "cannot prove what is
     * active" and the bucket is skipped to avoid mis-deletion.
     */
    public void logSkipKvBucket(long tableId, Long partitionId, int bucketId, String reason) {
        AUDIT.warn(
                "action=skip_kv_bucket reason={} table_id={} partition_id={} bucket_id={} ts={}",
                reason,
                tableId,
                partitionId,
                bucketId,
                Instant.now());
    }

    /**
     * Skip log cleanup for one (tableId, partitionId) target — emitted when {@code
     * ListRemoteLogManifests} fails after retries. {@code partitionId} is null for non-partitioned
     * tables.
     */
    public void logSkipLogTarget(long tableId, Long partitionId, String reason) {
        AUDIT.warn(
                "action=skip_log_target reason={} table_id={} partition_id={} ts={}",
                reason,
                tableId,
                partitionId,
                Instant.now());
    }

    /**
     * Skip log cleanup for a single bucket whose remote manifest was not returned by the {@code
     * ListRemoteLogManifests} RPC (the bucket has not yet committed any remote manifest).
     */
    public void logSkipLogBucket(long tableId, Long partitionId, int bucketId, String reason) {
        AUDIT.warn(
                "action=skip_log_bucket reason={} table_id={} partition_id={} bucket_id={} ts={}",
                reason,
                tableId,
                partitionId,
                bucketId,
                Instant.now());
    }

    /** Default-conservative skip of an orphan-table dir (opt-in flag not set). */
    public void logSkipOrphanTable(FsPath dir, String reason) {
        AUDIT.info("action=skip_orphan_table reason={} path={} ts={}", reason, dir, Instant.now());
    }

    /**
     * Skip the orphan-table scan for a database whose table-info set is incomplete (e.g. {@code
     * --table} single-table mode, or {@code listTables}/{@code getTableInfo} failures left holes in
     * the active table id set). Distinct from {@link #logSkipDb}, which means the whole database
     * scope is dropped.
     */
    public void logSkipOrphanTableScan(String dbName, String reason) {
        AUDIT.warn(
                "action=skip_orphan_table_scan reason={} db={} ts={}",
                reason,
                dbName,
                Instant.now());
    }

    /** Default-conservative skip of an orphan-partition dir (opt-in flag not set). */
    public void logSkipOrphanPartition(FsPath dir, String reason) {
        AUDIT.info(
                "action=skip_orphan_partition reason={} path={} ts={}", reason, dir, Instant.now());
    }

    /** Skip a bucket target because its metadata-resolved root is outside cluster config. */
    public void logSkipBucketOutOfScope(long tableId, Long partitionId, String resolvedRoot) {
        AUDIT.info(
                "action=skip_bucket_target reason=out-of-scope-root table_id={} partition_id={}"
                        + " resolved_root={} ts={}",
                tableId,
                partitionId,
                resolvedRoot,
                Instant.now());
    }
}
