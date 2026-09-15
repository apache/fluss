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
import org.apache.fluss.flink.action.orphan.job.CleanupCounters;
import org.apache.fluss.flink.action.orphan.job.RuleSummary;
import org.apache.fluss.flink.action.orphan.job.ScopeCoverageStats;
import org.apache.fluss.flink.action.orphan.rule.RuleId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/** Versioned text results shared by the existing cleanup operators. */
@Internal
public final class ResultAuditLogger implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger("fluss.orphan.audit");
    private final String runId;
    private final String clusterId;

    public ResultAuditLogger(Map<String, String> options) {
        runId =
                validate(
                        options.getOrDefault("audit.run-id", UUID.randomUUID().toString()),
                        "audit.run-id");
        clusterId =
                options.containsKey("audit.cluster-id")
                        ? validate(options.get("audit.cluster-id"), "audit.cluster-id")
                        : null;
    }

    private static String validate(String value, String key) {
        if (value == null || !value.matches("[A-Za-z0-9_.:-]{1,256}")) {
            throw new IllegalArgumentException(
                    key + " must be a non-empty identifier of at most 256 characters");
        }
        return value;
    }

    private void emit(String action, String fields) {
        LOG.info(
                "audit_version=1 run_id={}{} action={} {}",
                runId,
                clusterId == null ? "" : " cluster_id=" + clusterId,
                action,
                fields);
    }

    public void scopePlan(ScopeCoverageStats scope) {
        emit(
                "scope_plan",
                "bucket_tasks="
                        + scope.bucketTasks()
                        + " orphan_dir_tasks="
                        + scope.orphanDirTasks()
                        + " metadata_failures="
                        + scope.metadataFailures()
                        + " scope_targets="
                        + scope.expectedTargets()
                        + " incomplete_targets="
                        + scope.incompleteTargets()
                        + " coverage_complete="
                        + scope.coverageComplete());
    }

    public void subtask(
            int index,
            int attempt,
            long tasks,
            CleanupCounters counters,
            RuleSummary rules,
            boolean dryRun) {
        emit(
                "scan_subtask_summary",
                "subtask="
                        + index
                        + " attempt="
                        + attempt
                        + " tasks_completed="
                        + tasks
                        + " scanned_files="
                        + counters.scannedFiles()
                        + " scanned_bytes="
                        + rules.total(RuleSummary.SCANNED_BYTES)
                        + " planned_files="
                        + counters.plannedFiles()
                        + " planned_dirs="
                        + counters.plannedDirs()
                        + " planned_bytes="
                        + counters.plannedBytes()
                        + " deleted_files="
                        + counters.deletedFiles()
                        + " empty_dirs_removed="
                        + counters.emptyDirsRemoved()
                        + " delete_failures="
                        + counters.deleteFailures()
                        + " bytes_reclaimed="
                        + counters.bytesReclaimed()
                        + " dry_run="
                        + dryRun);
    }

    public void summary(
            CleanupCounters counters,
            ScopeCoverageStats scope,
            RuleSummary rules,
            long completedTasks,
            boolean dryRun) {
        boolean scopeConsistent =
                scope.countersConsistent()
                        && completedTasks == scope.bucketTasks() + scope.orphanDirTasks();
        boolean rulesConsistent = rules.consistent(counters);
        boolean dryRunConsistent =
                !dryRun
                        || (counters.deletedFiles() == 0
                                && counters.emptyDirsRemoved() == 0
                                && counters.bytesReclaimed() == 0
                                && counters.deleteFailures() == 0);
        boolean complete =
                scope.coverageComplete()
                        && rules.missingDirectories() == 0
                        && rules.total(RuleSummary.UNAVAILABLE_MTIME) == 0
                        && rules.unavailableDirectories() == 0;
        // These fields count targets blocked by missing active references. Confirmed empty
        // reference sets are scanned, while unresolved metadata is reported separately below.
        emit(
                "coverage_summary",
                "no_remote_manifest_targets=0 empty_active_set_targets=0"
                        + " metadata_read_failed_targets="
                        + (scope.logReadFailedBuckets() + scope.snapshotReadFailures())
                        + " directory_list_failed_targets="
                        + rules.missingDirectories()
                        + " rpc_failed_targets="
                        + scope.rpcFailures()
                        + " mtime_unavailable_files="
                        + rules.total(RuleSummary.UNAVAILABLE_MTIME)
                        + " mtime_unavailable_dirs="
                        + rules.unavailableDirectories()
                        + " complete="
                        + complete
                        + " dry_run="
                        + dryRun);
        emit(
                "audit_integrity",
                "scope_counters_consistent="
                        + scopeConsistent
                        + " rule_counters_consistent="
                        + rulesConsistent
                        + " counters_consistent="
                        + (scopeConsistent && rulesConsistent)
                        + " coverage_complete="
                        + complete
                        + " dry_run_counters_consistent="
                        + dryRunConsistent
                        + " dry_run="
                        + dryRun);
        for (RuleId rule : RuleId.values()) {
            emit(
                    "summary_by_rule",
                    "object_type="
                            + rule.toString().replace('-', '_')
                            + " scanned_files="
                            + rules.value(rule, RuleSummary.SCANNED_FILES)
                            + " scanned_bytes="
                            + rules.value(rule, RuleSummary.SCANNED_BYTES)
                            + " keep_active_files="
                            + rules.value(rule, RuleSummary.KEEP_ACTIVE)
                            + " newer_than_cutoff_files="
                            + rules.value(rule, RuleSummary.NEWER_THAN_CUTOFF)
                            + " unknown_file_type_files="
                            + rules.value(rule, RuleSummary.UNKNOWN_TYPE)
                            + " candidate_files="
                            + rules.value(rule, RuleSummary.CANDIDATE_FILES)
                            + " dry_run="
                            + dryRun);
        }
        if (!scopeConsistent || !rulesConsistent || !dryRunConsistent) {
            throw new IllegalStateException("Cleanup result counters are inconsistent");
        }
        emit(
                "summary",
                "scanned="
                        + counters.scannedFiles()
                        + " deleted_files="
                        + counters.deletedFiles()
                        + " empty_dirs_removed="
                        + counters.emptyDirsRemoved()
                        + " delete_failures="
                        + counters.deleteFailures()
                        + " bytes_reclaimed="
                        + counters.bytesReclaimed()
                        + " dry_run="
                        + dryRun);
    }
}
