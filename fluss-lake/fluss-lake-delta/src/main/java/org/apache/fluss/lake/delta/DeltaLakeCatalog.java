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
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.delta.utils.FlussDataTypeToDeltaDataType;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
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
import java.util.stream.Collectors;

/**
 * A Delta Lake implementation of {@link LakeCatalog}.
 *
 * <p>Every Fluss table maps to a path-based Delta table at {@code warehouse/db/table}, which can be
 * registered as an external table in a metastore such as Unity Catalog.
 */
public class DeltaLakeCatalog implements LakeCatalog {

    /**
     * Table properties of every created Delta table. Domain metadata keeps the Fluss offsets of
     * each tiering commit, see {@code DeltaLakeCommitter}.
     */
    static final Map<String, String> DEFAULT_TABLE_PROPERTIES =
            Collections.singletonMap("delta.feature.domainMetadata", "supported");

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
            if (existing.getSchema().equals(schema)
                    && existing.getPartitionColumnNames().equals(partitionKeys)) {
                return;
            }
            throw new TableAlreadyExistException(
                    String.format(
                            "Delta table %s already exists with schema %s and partition columns "
                                    + "%s, which don't match the Fluss table %s.",
                            path,
                            existing.getSchema(),
                            existing.getPartitionColumnNames(),
                            tablePath));
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
        TableManager.buildCreateTableTransaction(path, schema, DeltaConfig.ENGINE_INFO)
                .withDataLayoutSpec(layout)
                .withTableProperties(tableProperties(tableDescriptor))
                .build(engine)
                .commit(engine, CloseableIterable.emptyIterable());
    }

    @Override
    public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
            throws TableNotExistException {
        throw new UnsupportedOperationException(
                "Alter table is not supported for Delta Lake at the moment.");
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
