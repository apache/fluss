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

package com.alibaba.fluss.lake.lance;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.lakestorage.LakeCatalog;
import com.alibaba.fluss.lake.lance.utils.LanceArrowUtils;
import com.alibaba.fluss.lake.lance.utils.LanceDatasetAdapter;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import com.lancedb.lance.WriteParams;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** A Lance implementation of {@link LakeCatalog}. */
public class LanceLakeCatalog implements LakeCatalog {
    private static final List<Field> SYSTEM_COLUMNS = new ArrayList<>();

    static {
        SYSTEM_COLUMNS.add(Field.nullable(BUCKET_COLUMN_NAME, new ArrowType.Int(32, true)));
        SYSTEM_COLUMNS.add(Field.nullable(OFFSET_COLUMN_NAME, new ArrowType.Int(64, true)));
        SYSTEM_COLUMNS.add(
                Field.nullable(
                        TIMESTAMP_COLUMN_NAME,
                        new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    }

    private final Configuration options;

    public LanceLakeCatalog(Configuration config) {
        this.options = config;
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor) {
        LanceConfig config =
                LanceConfig.from(
                        options.toMap(), tablePath.getDatabaseName(), tablePath.getTableName());
        WriteParams params = LanceConfig.genWriteParamsFromConfig(config);

        List<Field> fields = new ArrayList<>();
        // set schema
        fields.addAll(
                LanceArrowUtils.toArrowSchema(tableDescriptor.getSchema().getRowType())
                        .getFields());
        // add system metadata columns to schema
        fields.addAll(SYSTEM_COLUMNS);
        try {
            LanceDatasetAdapter.createDataset(config.getDatasetUri(), new Schema(fields), params);
        } catch (RuntimeException e) {
            throw new RuntimeException("Table " + tablePath + " creation failed", e);
        }
    }

    @Override
    public void close() throws Exception {
        LakeCatalog.super.close();
    }
}
