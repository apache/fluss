/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.metadata.TableInfo;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;

final class FlussTableScanValidator {

    private FlussTableScanValidator() {}

    static void validateIdentity(FlussTableHandle table, TableInfo info) {
        if (table.getTableId() != info.getTableId()
                || table.getSchemaId() != info.getSchemaId()
                || table.getBucketCount() != info.getNumBuckets()
                || table.getBucketCountEpoch() != info.getBucketCountEpoch()) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Fluss table, schema or bucket layout changed during query planning; retry the query please");
        }
    }

    static void validateSupportedTable(TableInfo info) {
        if (info.getTableConfig().isDataLakeEnabled()) {
            throw new TrinoException(
                    UNSUPPORTED_TABLE_TYPE, "Reading Fluss Lakehouse tables is not supported");
        }
        if (info.hasPrimaryKey()) {
            throw new TrinoException(
                    UNSUPPORTED_TABLE_TYPE, "Reading Fluss primary key tables is not supported");
        }
        if (info.isPartitioned()) {
            throw new TrinoException(
                    UNSUPPORTED_TABLE_TYPE, "Reading partitioned Fluss tables is not supported");
        }
    }
}
