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

package org.apache.fluss.flink.action.orphan.config;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.utils.StringUtils;

import org.apache.flink.api.java.utils.MultipleParameterTool;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Parsed command-line options for the orphan files cleanup action. */
@Internal
public final class OrphanCleanConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Minimum gap between any user-supplied cutoff and {@code now}. A cutoff closer to {@code now}
     * would risk classifying files that are mid-write (committed file written, snapshot/manifest
     * not yet visible to {@code ListRemoteLogManifests} / {@code ListKvSnapshots}) as orphan and
     * deleting them.
     */
    private static final Duration HARD_LOWER_BOUND = Duration.ofDays(1);

    /** Default file-level cutoff: files written before {@code now - 3d} are deletion-eligible. */
    private static final Duration DEFAULT_OLDER_THAN = Duration.ofDays(3);

    private static final long DEFAULT_DELETE_RATE_LIMIT_PER_SECOND = 100L;

    /**
     * Wall-clock timestamp format accepted on the CLI ({@code yyyy-MM-dd HH:mm:ss}, interpreted in
     * the server's local time zone). Matches Apache Paimon's {@code orphan_files_clean older_than}
     * grammar to minimize operator context-switching between systems.
     */
    private static final DateTimeFormatter CUTOFF_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final String bootstrapServer;
    private final boolean allDatabases;
    private final @Nullable String database;
    private final @Nullable String table;
    private final long olderThanMillis;
    private final boolean dryRun;
    private final long deleteRateLimitPerSecond;
    private final @Nullable Integer parallelism;
    private final List<String> scanRoots;
    private final boolean allowDeleteManifest;
    private final boolean allowCleanOrphanTables;
    private final boolean allowCleanOrphanPartitions;
    private final Map<String, String> extraConfigs;

    private OrphanCleanConfig(
            String bootstrapServer,
            boolean allDatabases,
            @Nullable String database,
            @Nullable String table,
            long olderThanMillis,
            boolean dryRun,
            long deleteRateLimitPerSecond,
            @Nullable Integer parallelism,
            List<String> scanRoots,
            boolean allowDeleteManifest,
            boolean allowCleanOrphanTables,
            boolean allowCleanOrphanPartitions,
            Map<String, String> extraConfigs) {
        this.bootstrapServer = bootstrapServer;
        this.allDatabases = allDatabases;
        this.database = database;
        this.table = table;
        this.olderThanMillis = olderThanMillis;
        this.dryRun = dryRun;
        this.deleteRateLimitPerSecond = deleteRateLimitPerSecond;
        this.parallelism = parallelism;
        this.scanRoots = Collections.unmodifiableList(new ArrayList<String>(scanRoots));
        this.allowDeleteManifest = allowDeleteManifest;
        this.allowCleanOrphanTables = allowCleanOrphanTables;
        this.allowCleanOrphanPartitions = allowCleanOrphanPartitions;
        this.extraConfigs = Collections.unmodifiableMap(new HashMap<>(extraConfigs));
    }

    /** Parses a cleanup config from CLI parameters. */
    public static OrphanCleanConfig fromParams(MultipleParameterTool params) {
        String bootstrapServer = params.get("bootstrap-server");
        if (StringUtils.isNullOrWhitespaceOnly(bootstrapServer)) {
            throw new IllegalArgumentException("--bootstrap-server is required");
        }

        boolean allDatabases = params.has("all-databases");
        String database = params.get("database");
        if (allDatabases && !StringUtils.isNullOrWhitespaceOnly(database)) {
            throw new IllegalArgumentException(
                    "--database and --all-databases are mutually exclusive");
        }
        if (!allDatabases && StringUtils.isNullOrWhitespaceOnly(database)) {
            throw new IllegalArgumentException(
                    "Either --database or --all-databases must be provided");
        }
        if (allDatabases && !StringUtils.isNullOrWhitespaceOnly(params.get("table"))) {
            throw new IllegalArgumentException(
                    "--table requires --database and cannot be used with --all-databases");
        }

        long now = System.currentTimeMillis();
        long olderThanMillis =
                parseCutoff("--older-than", params.get("older-than"), now, DEFAULT_OLDER_THAN);
        long deleteRateLimitPerSecond =
                parseDeleteRateLimit(params.get("delete-rate-limit-per-second"));
        Integer parallelism = parseParallelism(params.get("parallelism"));
        boolean allowDeleteManifest = params.has("allow-delete-manifest");
        boolean allowCleanOrphanTables = params.has("allow-clean-orphan-tables");
        boolean allowCleanOrphanPartitions = params.has("allow-clean-orphan-partitions");

        return new OrphanCleanConfig(
                bootstrapServer,
                allDatabases,
                database,
                params.get("table"),
                olderThanMillis,
                params.has("dry-run"),
                deleteRateLimitPerSecond,
                parallelism,
                parseScanRoots(params.getMultiParameter("scan-root")),
                allowDeleteManifest,
                allowCleanOrphanTables,
                allowCleanOrphanPartitions,
                parseExtraConfigs(params.getMultiParameter("conf")));
    }

    /**
     * Parses a CLI cutoff value into an absolute epoch-ms timestamp. Empty input falls back to
     * {@code now - defaultGap}. Explicit input must parse as {@code yyyy-MM-dd HH:mm:ss} in the
     * server's local time zone and must be at least {@link #HARD_LOWER_BOUND} earlier than {@code
     * now} — closer-to-now cutoffs would race with active writes (see {@code HARD_LOWER_BOUND}
     * javadoc).
     */
    private static long parseCutoff(
            String flag, @Nullable String value, long now, Duration defaultGap) {
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            return now - defaultGap.toMillis();
        }
        LocalDateTime parsed;
        try {
            parsed = LocalDateTime.parse(value, CUTOFF_FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    flag
                            + " must be a timestamp in 'yyyy-MM-dd HH:mm:ss' (server local TZ), got: "
                            + value,
                    e);
        }
        long parsedMillis = parsed.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long maxAllowed = now - HARD_LOWER_BOUND.toMillis();
        if (parsedMillis > maxAllowed) {
            throw new IllegalArgumentException(
                    flag
                            + " must be at least 1d before now (got "
                            + Instant.ofEpochMilli(parsedMillis)
                            + ", now is "
                            + Instant.ofEpochMilli(now)
                            + "); a closer cutoff would race with mid-write files");
        }
        return parsedMillis;
    }

    private static long parseDeleteRateLimit(@Nullable String value) {
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            return DEFAULT_DELETE_RATE_LIMIT_PER_SECOND;
        }
        long rate = Long.parseLong(value);
        if (rate <= 0) {
            throw new IllegalArgumentException("--delete-rate-limit-per-second must be positive");
        }
        return rate;
    }

    @Nullable
    private static Integer parseParallelism(@Nullable String value) {
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            return null;
        }
        int p = Integer.parseInt(value);
        if (p <= 0) {
            throw new IllegalArgumentException("--parallelism must be positive");
        }
        return p;
    }

    private static List<String> parseScanRoots(@Nullable Collection<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> scanRoots = new ArrayList<String>(values.size());
        for (String value : values) {
            if (StringUtils.isNullOrWhitespaceOnly(value)) {
                throw new IllegalArgumentException("--scan-root must not be blank");
            }
            scanRoots.add(value);
        }
        return scanRoots;
    }

    private static Map<String, String> parseExtraConfigs(@Nullable Collection<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> configs = new HashMap<String, String>();
        for (String kv : values) {
            int eqIdx = kv.indexOf('=');
            if (eqIdx <= 0) {
                throw new IllegalArgumentException(
                        "--conf must be in key=value format, got: " + kv);
            }
            configs.put(kv.substring(0, eqIdx), kv.substring(eqIdx + 1));
        }
        return configs;
    }

    /** Returns the bootstrap server list used to connect to Fluss. */
    public String bootstrapServer() {
        return bootstrapServer;
    }

    /** Returns whether the cleanup targets all databases. */
    public boolean allDatabases() {
        return allDatabases;
    }

    /** Returns the single targeted database when the action is not scoped to all databases. */
    public Optional<String> database() {
        return Optional.ofNullable(database);
    }

    /** Returns the optional targeted table name. */
    public Optional<String> table() {
        return Optional.ofNullable(table);
    }

    /**
     * Returns the file-level cutoff as an absolute epoch-millis timestamp, frozen at action
     * startup. A candidate file is deletion-eligible iff its mtime is strictly less than this
     * value. The cutoff does not slide during the run — long scans cannot accidentally pull in
     * files written after startup.
     */
    public long olderThanMillis() {
        return olderThanMillis;
    }

    /** Returns whether the action runs in dry-run mode. */
    public boolean dryRun() {
        return dryRun;
    }

    /** Returns the maximum number of actual delete calls per second. */
    public long deleteRateLimitPerSecond() {
        return deleteRateLimitPerSecond;
    }

    /** Returns the optional parallelism for the ScanAndClean stage. */
    public Optional<Integer> parallelism() {
        return Optional.ofNullable(parallelism);
    }

    /** Returns additional remote.data.dir roots to scan. */
    public List<String> scanRoots() {
        return scanRoots;
    }

    /**
     * Opt-in to delete {@code .manifest} files. Default {@code false}: mis-deleting an active
     * manifest leaves the coordinator's manifest pointer dangling and breaks the bucket's metadata
     * chain — the failure mode is catastrophic and asymmetric vs the trivial space cost of keeping
     * orphan manifests (KB-sized files), so deletion is gated behind an explicit operator flag.
     */
    public boolean allowDeleteManifest() {
        return allowDeleteManifest;
    }

    /**
     * Opt-in to recursively clean files inside an orphan-table directory. Default {@code false}:
     * the action only audits the detected orphan dir and leaves its contents untouched, because an
     * id-based misclassification of a freshly-created table as orphan would otherwise be
     * unrecoverable. Operators flip this on once they have reviewed the audit log.
     */
    public boolean allowCleanOrphanTables() {
        return allowCleanOrphanTables;
    }

    /**
     * Opt-in to recursively clean files inside an orphan-partition directory. Same default-audit
     * rationale as {@link #allowCleanOrphanTables()}.
     */
    public boolean allowCleanOrphanPartitions() {
        return allowCleanOrphanPartitions;
    }

    /**
     * Returns extra configuration entries passed via {@code --conf key=value}. These are propagated
     * to {@link org.apache.fluss.fs.FileSystem#initialize} for remote filesystem authentication
     * (e.g. {@code fs.oss.accessKeyId}, {@code fs.oss.accessKeySecret}).
     */
    public Map<String, String> extraConfigs() {
        return extraConfigs;
    }
}
