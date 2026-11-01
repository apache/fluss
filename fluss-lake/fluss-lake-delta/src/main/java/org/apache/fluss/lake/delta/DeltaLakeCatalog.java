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

package org.apache.fluss.lake.delta;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.delta.utils.FlussDataTypeToDeltaDataType;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.LakeTableUtil;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A Delta Lake implementation of {@link LakeCatalog}.
 *
 * <p>Every Fluss table maps to a path-based Delta table at {@code warehouse/db/table}, which can be
 * registered as an external table in a metastore such as Unity Catalog.
 */
public class DeltaLakeCatalog implements LakeCatalog {

    private static final String DOMAIN_METADATA_FEATURE = "domainMetadata";

    /**
     * Table properties of every created Delta table. Domain metadata keeps the Fluss offsets of
     * each tiering commit, see {@code DeltaLakeCommitter}.
     */
    static final Map<String, String> DEFAULT_TABLE_PROPERTIES =
            Collections.singletonMap("delta.feature." + DOMAIN_METADATA_FEATURE, "supported");

    private static final String DELTA_PROPERTY_PREFIX = "delta.";

    private final Configuration options;

    public DeltaLakeCatalog(Configuration options) {
        this.options = options;
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor, Context context)
            throws TableAlreadyExistException {
        if (tableDescriptor.hasPrimaryKey()) {
            throw new InvalidTableException(
                    "Currently, we don't support tiering a primary key table to Delta Lake.");
        }
        RowType rowType = tableDescriptor.getSchema().getRowType();
        List<String> partitionKeys = tableDescriptor.getPartitionKeys();
        for (String partitionKey : partitionKeys) {
            DataTypeRoot root =
                    rowType.getTypeAt(rowType.getFieldIndex(partitionKey)).getTypeRoot();
            if (root != DataTypeRoot.STRING) {
                throw new InvalidTableException(
                        String.format(
                                "Currently, Delta Lake only supports STRING partition keys, "
                                        + "but partition key '%s' is %s.",
                                partitionKey, root));
            }
        }
        StructType schema;
        try {
            schema = FlussDataTypeToDeltaDataType.toDeltaSchema(rowType);
        } catch (UnsupportedOperationException e) {
            throw new InvalidTableException(e.getMessage());
        }

        String path = DeltaConfig.tablePath(options, tablePath);
        Engine engine = DeltaConfig.createEngine(options);
        try {
            Snapshot existing = TableManager.loadSnapshot(path).build(engine);
            // The table may be left by a previous attempt to create the same Fluss table.
            if (!existing.getSchema().equals(schema)
                    || !existing.getPartitionColumnNames().equals(partitionKeys)) {
                throw new TableAlreadyExistException(
                        String.format(
                                "Delta table %s already exists with schema %s and partition "
                                        + "columns %s, which don't match the Fluss table %s.",
                                path,
                                existing.getSchema(),
                                existing.getPartitionColumnNames(),
                                tablePath));
            }
            if (!existing.getProtocol().getWriterFeatures().contains(DOMAIN_METADATA_FEATURE)) {
                throw new TableAlreadyExistException(
                        String.format(
                                "Delta table %s already exists without the %s table feature, "
                                        + "which tiering the Fluss table %s requires.",
                                path, DOMAIN_METADATA_FEATURE, tablePath));
            }
            return;
        } catch (TableNotFoundException e) {
            // expected, create the table below
        }

        DataLayoutSpec layout =
                partitionKeys.isEmpty()
                        ? DataLayoutSpec.noDataLayout()
                        : DataLayoutSpec.partitioned(
                                partitionKeys.stream()
                                        .map(Column::new)
                                        .collect(Collectors.toList()));
        Transaction transaction =
                TableManager.buildCreateTableTransaction(path, schema, DeltaConfig.ENGINE_INFO)
                        .withDataLayoutSpec(layout)
                        .withTableProperties(tableProperties(tableDescriptor))
                        .build(engine);
        // Column mapping, also enabled implicitly by e.g. delta.enableIcebergCompatV2, annotates
        // the schema. Delta Kernel can't write to such a table, so tiering would always fail.
        if (!transaction.getSchema(engine).equals(schema)) {
            throw new InvalidTableException(
                    String.format(
                            "The delta.* properties of table %s enable column mapping, which "
                                    + "tiering to Delta Lake doesn't support.",
                            tablePath));
        }
        transaction.commit(engine, CloseableIterable.emptyIterable());
    }

    /**
     * Only accepts changes that leave the Delta table as it is: tiering doesn't read Fluss-side
     * options such as {@code table.datalake.freshness} from the Delta table, so they are not
     * written to it.
     */
    @Override
    public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
            throws TableNotExistException {
        for (TableChange change : tableChanges) {
            if (change instanceof TableChange.ModifyBucketCount) {
                throw new UnsupportedOperationException(
                        "Bucket count rescale is not supported by the Delta Lake catalog yet.");
            }
            if (!(change instanceof TableChange.SetOption
                    || change instanceof TableChange.ResetOption)) {
                throw new InvalidAlterTableException(
                        "Altering the schema of a Delta Lake table is not supported yet: "
                                + change);
            }
            if (LakeTableUtil.isLakeTablePathChange(change)) {
                throw new InvalidAlterTableException(
                        "The Delta table path can't be altered: " + change);
            }
        }

        String path = DeltaConfig.tablePath(options, tablePath);
        Map<String, String> deltaProperties;
        try {
            deltaProperties =
                    TableManager.loadSnapshot(path)
                            .build(DeltaConfig.createEngine(options))
                            .getTableProperties();
        } catch (TableNotFoundException e) {
            throw new TableNotExistException("Delta table " + path + " does not exist.", e);
        }
        for (TableChange change : tableChanges) {
            checkDeltaPropertyUnchanged(change, deltaProperties);
        }
    }

    /**
     * Rejects a change of a {@code delta.*} property, which would otherwise be recorded by Fluss
     * but never reach the Delta table. A change that matches the Delta table is accepted, because
     * enabling datalake replays the properties that {@link #createTable} has just applied.
     */
    private static void checkDeltaPropertyUnchanged(
            TableChange change, Map<String, String> deltaProperties) {
        String key;
        String value;
        if (change instanceof TableChange.SetOption) {
            key = ((TableChange.SetOption) change).getKey();
            value = ((TableChange.SetOption) change).getValue();
        } else {
            key = ((TableChange.ResetOption) change).getKey();
            value = null;
        }
        if (key.startsWith(DELTA_PROPERTY_PREFIX)
                && !Objects.equals(value, deltaProperties.get(key))) {
            throw new InvalidAlterTableException(
                    String.format(
                            "Altering the Delta table property '%s' is not supported yet.", key));
        }
    }

    /** Passes the {@code delta.*} custom properties of the Fluss table to the Delta table. */
    private static Map<String, String> tableProperties(TableDescriptor tableDescriptor) {
        Map<String, String> properties = new HashMap<>(DEFAULT_TABLE_PROPERTIES);
        for (Map.Entry<String, String> entry : tableDescriptor.getCustomProperties().entrySet()) {
            if (entry.getKey().startsWith(DELTA_PROPERTY_PREFIX)) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
        return properties;
    }
}
