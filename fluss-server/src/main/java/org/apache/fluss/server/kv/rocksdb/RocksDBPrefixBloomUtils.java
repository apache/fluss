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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import io.github.fluss_contrib.rocksdb.PrefixLayout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/** Derives native prefix layouts from the primary-key encoding used by a table. */
public final class RocksDBPrefixBloomUtils {

    private RocksDBPrefixBloomUtils() {}

    /** Derives the bucket-key prefix layout, or returns null when the option is disabled. */
    @Nullable
    public static PrefixLayout createPrefixLayout(TableDescriptor table) {
        TableConfig config = new TableConfig(Configuration.fromMap(table.getProperties()));
        if (!config.isKvPrefixBloomEnabled()) {
            return null;
        }
        List<String> primaryKeys =
                table.getSchema().getPrimaryKey().isPresent()
                        ? new ArrayList<>(table.getSchema().getPrimaryKey().get().getColumnNames())
                        : new ArrayList<>();
        primaryKeys.removeAll(table.getPartitionKeys());
        return createPrefixLayout(
                table.getSchema().getRowType(),
                primaryKeys,
                table.getTableDistribution().isPresent()
                        ? table.getTableDistribution().get().getBucketKeys()
                        : Collections.emptyList(),
                config);
    }

    /** Derives the layout from the persisted key-format and distribution metadata. */
    @Nullable
    public static PrefixLayout createPrefixLayout(TableInfo table) {
        if (!table.getTableConfig().isKvPrefixBloomEnabled()) {
            return null;
        }
        return createPrefixLayout(
                table.getRowType(),
                table.getPhysicalPrimaryKeys(),
                table.getBucketKeys(),
                table.getTableConfig());
    }

    private static PrefixLayout createPrefixLayout(
            RowType rowType,
            List<String> primaryKeys,
            List<String> bucketKeys,
            TableConfig config) {
        if (primaryKeys.isEmpty() || bucketKeys.isEmpty()) {
            throw new InvalidTableException(
                    "Prefix Bloom filters require a primary key table with bucket keys.");
        }
        if (bucketKeys.size() > primaryKeys.size()
                || !primaryKeys.subList(0, bucketKeys.size()).equals(bucketKeys)) {
            throw new InvalidTableException(
                    "Prefix Bloom filters require bucket keys to form a leading prefix of the physical primary key.");
        }
        KeyEncoder encoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType, primaryKeys, config, bucketKeys.equals(primaryKeys));
        if (!(encoder instanceof CompactedKeyEncoder)) {
            throw new InvalidTableException(
                    "Prefix Bloom filters cannot describe primary-key encoder "
                            + encoder.getClass().getSimpleName()
                            + ". Compacted primary-key encoding is required.");
        }
        List<PrefixLayout.Step> steps = new ArrayList<>();
        for (String key : bucketKeys) {
            appendCompactedField(steps, rowType.getTypeAt(rowType.getFieldIndex(key)));
        }
        return PrefixLayout.of(steps.toArray(new PrefixLayout.Step[0]));
    }

    private static void appendCompactedField(List<PrefixLayout.Step> steps, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                steps.add(PrefixLayout.fixed(1));
                break;
            case SMALLINT:
                steps.add(PrefixLayout.fixed(2));
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                steps.add(PrefixLayout.varint32());
                break;
            case BIGINT:
                steps.add(PrefixLayout.varint64());
                break;
            case FLOAT:
                steps.add(PrefixLayout.fixed(4));
                break;
            case DOUBLE:
                steps.add(PrefixLayout.fixed(8));
                break;
            case CHAR:
            case STRING:
            case BINARY:
            case BYTES:
                steps.add(PrefixLayout.bytesVarint32());
                break;
            case DECIMAL:
                steps.add(
                        Decimal.isCompact(getPrecision(type))
                                ? PrefixLayout.varint64()
                                : PrefixLayout.bytesVarint32());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                steps.add(PrefixLayout.varint64());
                if (!TimestampNtz.isCompact(getPrecision(type))) {
                    steps.add(PrefixLayout.varint32());
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                steps.add(PrefixLayout.varint64());
                if (!TimestampLtz.isCompact(getPrecision(type))) {
                    steps.add(PrefixLayout.varint32());
                }
                break;
            default:
                throw new InvalidTableException(
                        "Prefix Bloom filters cannot describe primary-key type " + type + ".");
        }
    }
}
