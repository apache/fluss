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

package org.apache.fluss.lake.lance;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lance.utils.LanceArrowUtils;
import org.apache.fluss.lake.lance.utils.LanceDatasetAdapter;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;

import com.lancedb.lance.WriteParams;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/** A Lance implementation of {@link LakeCatalog}. */
public class LanceLakeCatalog implements LakeCatalog {
    private final Configuration options;

    public LanceLakeCatalog(Configuration config) {
        this.options = config;
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor, Context context) {
        // currently, we don't support primary key table for lance
        if (tableDescriptor.hasPrimaryKey()) {
            throw new InvalidTableException(
                    "Currently, we don't support tiering a primary key table to Lance");
        }

        LanceConfig config =
                LanceConfig.from(
                        options.toMap(),
                        tableDescriptor.getCustomProperties(),
                        tablePath.getDatabaseName(),
                        tablePath.getTableName());
        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);

        List<Field> fields = new ArrayList<>();
        // set schema
        fields.addAll(
                LanceArrowUtils.toArrowSchema(tableDescriptor.getSchema().getRowType())
                        .getFields());
        try {
            LanceDatasetAdapter.createDataset(config.getDatasetUri(), new Schema(fields), params);
        } catch (RuntimeException e) {
            throw new RuntimeException("Table " + tablePath + " creation failed", e);
        }
    }

    @Override
    public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
            throws TableNotExistException {
        throw new UnsupportedOperationException(
                "Alter table is not supported for Lance at the moment");
    }

    @Override
    public void close() throws Exception {
        LakeCatalog.super.close();
    }
}
