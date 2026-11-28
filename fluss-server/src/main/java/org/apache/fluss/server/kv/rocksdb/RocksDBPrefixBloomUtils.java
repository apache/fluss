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

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;

import io.github.fluss_contrib.rocksdb.PrefixLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/** Creates RocksDB prefix layouts for compatible compacted primary keys. */
public final class RocksDBPrefixBloomUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBPrefixBloomUtils.class);

    private RocksDBPrefixBloomUtils() {}

    /**
     * Returns a supported bucket-key layout, or null and logs the reason for skipping the table.
     */
    @Nullable
    public static PrefixLayout createPrefixLayout(TableInfo table) {
        List<String> primaryKeys = table.getPhysicalPrimaryKeys();
        List<String> bucketKeys = table.getBucketKeys();
        if (primaryKeys.isEmpty() || bucketKeys.isEmpty()) {
            LOG.info(
                    "Prefix Bloom filters disabled for table {} (id={}): no physical primary key or bucket key.",
                    table.getTablePath(),
                    table.getTableId());
            return null;
        }
        if (bucketKeys.size() > primaryKeys.size()
                || !primaryKeys.subList(0, bucketKeys.size()).equals(bucketKeys)) {
            LOG.info(
                    "Prefix Bloom filters disabled for table {} (id={}): bucket keys {} are not a leading prefix of physical primary keys {}.",
                    table.getTablePath(),
                    table.getTableId(),
                    bucketKeys,
                    primaryKeys);
            return null;
        }
        try {
            KeyEncoder encoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            table.getRowType(),
                            primaryKeys,
                            table.getTableConfig(),
                            table.isDefaultBucketKey());
            if (!(encoder instanceof CompactedKeyEncoder)) {
                LOG.info(
                        "Prefix Bloom filters disabled for table {} (id={}): unsupported key encoder {}.",
                        table.getTablePath(),
                        table.getTableId(),
                        encoder.getClass().getSimpleName());
                return null;
            }
            List<PrefixLayout.Step> steps = new ArrayList<>();
            for (String bucketKey : bucketKeys) {
                DataType type = table.getRowType().getField(bucketKey).getType();
                if (!appendFieldLayout(steps, type)) {
                    LOG.info(
                            "Prefix Bloom filters disabled for table {} (id={}): unsupported key type {}.",
                            table.getTablePath(),
                            table.getTableId(),
                            type);
                    return null;
                }
            }
            return PrefixLayout.of(steps.toArray(new PrefixLayout.Step[0]));
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            LOG.info(
                    "Prefix Bloom filters disabled for table {} (id={}): unsupported key layout: {}",
                    table.getTablePath(),
                    table.getTableId(),
                    e.getMessage());
            return null;
        }
    }

    private static boolean appendFieldLayout(List<PrefixLayout.Step> steps, DataType type) {
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
                return false;
        }
        return true;
    }
}
