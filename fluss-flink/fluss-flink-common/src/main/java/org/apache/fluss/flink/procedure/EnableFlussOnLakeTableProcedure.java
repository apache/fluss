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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.flink.action.load.LoadLakeDataToFlussAction;
import org.apache.fluss.flink.utils.DataLakeUtils;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.TemporaryClassLoaderContext;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.utils.Preconditions.checkState;
import static org.apache.fluss.utils.PropertiesUtils.extractAndRemovePrefix;

/** Procedure for promoting an existing Paimon table to a datalake-enabled Fluss table. */
public class EnableFlussOnLakeTableProcedure extends ProcedureBase {

    private static final String SUCCESS_MESSAGE = "Successfully enabled Fluss on %s table '%s'.";

    static Class<? extends EnableFlussOnLakeTableProcedure> getProcedureClass() {
        try {
            ProcedureHint.class.getMethod("argument");
            return EnableFlussOnLakeTableProcedure.class;
        } catch (NoSuchMethodException e) {
            // Only Flink 1.18 needs overloads. Keeping them on newer versions would prevent
            // named arguments from resolving.
            return Flink118.class;
        }
    }

    /** Promotes a lake table, optionally overriding its table options. */
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(ProcedureContext context, String table, @Nullable String options)
            throws Exception {
        TablePath tablePath = parseTablePath(table, flussProcedureContext.getDefaultDatabase());
        TableInfo tableInfo;
        boolean flussTableCreated = false;
        try {
            tableInfo =
                    admin.createTableFromLake(
                                    tablePath,
                                    options == null
                                            ? Collections.emptyMap()
                                            : parseOptions(options))
                            .get();
            flussTableCreated = true;
        } catch (ExecutionException e) {
            if (!ExceptionUtils.findThrowable(e, TableAlreadyExistException.class).isPresent()) {
                throw e;
            }
            tableInfo = admin.getTableInfo(tablePath).get();
        }

        if (tableInfo.getTableConfig().isDataLakeEnabled()) {
            return success(tableInfo);
        }
        if (!flussTableCreated && hasRegisteredLakeSnapshot(tablePath)) {
            // Preserve the snapshot registered by an earlier attempt for the server's re-enable
            // consistency check. Retrying must not load or register a newer source snapshot.
            enableLake(tableInfo);
            return success(tableInfo);
        }

        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(flussProcedureContext.getClassLoader())) {
            LakeStorage lakeStorage = createLakeStorage(tableInfo);
            Optional<Long> snapshotId;
            try (LakeCatalog lakeCatalog = lakeStorage.createLakeCatalog()) {
                snapshotId = lakeCatalog.getLatestSnapshotId(tableInfo.getLakeTablePath());
            }
            if (snapshotId.isPresent() && tableInfo.hasPrimaryKey()) {
                checkState(
                        flussTableCreated,
                        "Cannot load an existing lake-disabled primary-key table without a"
                                + " registered snapshot. Resolve any previous Bulk Load and recreate"
                                + " the Fluss table before retrying.");
                return loadAndEnable(context, tableInfo, snapshotId.get());
            }
            return enableWithoutLoad(tableInfo, snapshotId);
        }
    }

    private String[] loadAndEnable(ProcedureContext context, TableInfo tableInfo, long snapshotId)
            throws Exception {
        // The job owns snapshot registration and lake enablement after Bulk Load commit succeeds.
        // Sync mode only changes whether CALL waits; it does not change who enables the table.
        JobClient job =
                new LoadLakeDataToFlussAction(
                                flussProcedureContext.getFlussConfiguration(),
                                Configuration.fromMap(
                                        extractAndRemovePrefix(
                                                flussProcedureContext.getLakeCatalogProperties(),
                                                DataLakeFormat.PAIMON + ".")),
                                tableInfo.getTablePath(),
                                snapshotId,
                                null,
                                UUID.randomUUID().toString(),
                                tableInfo.getTableId())
                        .submit(context.getExecutionEnvironment());
        if (!context.getExecutionEnvironment()
                .getConfiguration()
                .get(TableConfigOptions.TABLE_DML_SYNC)) {
            return new String[] {"JobID=" + job.getJobID()};
        }
        job.getJobExecutionResult().get();
        return success(tableInfo);
    }

    private String[] enableWithoutLoad(TableInfo tableInfo, Optional<Long> snapshotId)
            throws Exception {
        // Log tables and tables without a source snapshot need no KV load job.
        if (snapshotId.isPresent()) {
            try (FlussTableLakeSnapshotCommitter committer =
                    new FlussTableLakeSnapshotCommitter(
                            flussProcedureContext.getFlussConfiguration())) {
                committer.open();
                committer.commitInitialSnapshot(
                        tableInfo.getTableId(), tableInfo.getTablePath(), snapshotId.get());
            }
        }
        enableLake(tableInfo);
        return success(tableInfo);
    }

    private void enableLake(TableInfo tableInfo) throws Exception {
        admin.alterTable(
                        tableInfo.getTablePath(),
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")),
                        false)
                .get();
    }

    private String[] success(TableInfo tableInfo) {
        String lakeFormat =
                tableInfo
                        .getTableConfig()
                        .getDataLakeFormat()
                        .map(DataLakeFormat::toString)
                        .orElse("lake");
        return new String[] {String.format(SUCCESS_MESSAGE, lakeFormat, tableInfo.getTablePath())};
    }

    private boolean hasRegisteredLakeSnapshot(TablePath tablePath) throws Exception {
        try {
            admin.getLatestLakeSnapshot(tablePath).get();
            return true;
        } catch (ExecutionException e) {
            if (ExceptionUtils.findThrowable(e, LakeTableSnapshotNotExistException.class)
                    .isPresent()) {
                return false;
            }
            throw e;
        }
    }

    private LakeStorage createLakeStorage(TableInfo tableInfo) {
        DataLakeFormat dataLakeFormat = tableInfo.getTableConfig().getDataLakeFormat().get();
        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat.toString(), null);
        Map<String, String> lakeProperties =
                new HashMap<>(
                        DataLakeUtils.extractLakeCatalogProperties(tableInfo.getProperties()));
        lakeProperties.putAll(
                extractAndRemovePrefix(
                        flussProcedureContext.getLakeCatalogProperties(), dataLakeFormat + "."));
        Configuration lakeConfiguration = Configuration.fromMap(lakeProperties);
        return lakeStoragePlugin.createLakeStorage(lakeConfiguration);
    }

    static TablePath parseTablePath(String table, String defaultDatabase) {
        if (table == null) {
            throw new IllegalArgumentException("Table identifier must not be null.");
        }
        String[] identifiers = table.trim().split("\\.", -1);
        TablePath tablePath;
        if (identifiers.length == 1 && !identifiers[0].trim().isEmpty()) {
            tablePath = TablePath.of(defaultDatabase, identifiers[0].trim());
        } else if (identifiers.length == 2
                && !identifiers[0].trim().isEmpty()
                && !identifiers[1].trim().isEmpty()) {
            tablePath = TablePath.of(identifiers[0].trim(), identifiers[1].trim());
        } else {
            throw new IllegalArgumentException(
                    "Table identifier must be in the form 'table' or 'database.table', but was '"
                            + table
                            + "'.");
        }
        tablePath.validate();
        return tablePath;
    }

    static Map<String, String> parseOptions(String options) {
        if (options == null) {
            throw new IllegalArgumentException("Options must not be null.");
        }
        if (options.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> result = new LinkedHashMap<>();
        for (String option : options.split(",", -1)) {
            int separator = option.indexOf('=');
            if (separator < 0) {
                throw invalidOption(option, "is missing '='");
            }
            String key = option.substring(0, separator).trim();
            String value = option.substring(separator + 1).trim();
            if (key.isEmpty()) {
                throw invalidOption(option, "has an empty key");
            }
            if (value.isEmpty()) {
                throw invalidOption(option, "has an empty value");
            }
            if (result.put(key, value) != null) {
                throw invalidOption(option, "duplicates option '" + key + "'");
            }
        }
        return result;
    }

    private static IllegalArgumentException invalidOption(String option, String reason) {
        return new IllegalArgumentException(
                "Invalid table option '" + option.trim() + "': " + reason + ".");
    }

    /** Positional overload for Flink 1.18, which does not support optional arguments. */
    public static class Flink118 extends EnableFlussOnLakeTableProcedure {

        /** Promotes a table with default options. */
        public String[] call(ProcedureContext context, String table) throws Exception {
            return call(context, table, null);
        }
    }
}
