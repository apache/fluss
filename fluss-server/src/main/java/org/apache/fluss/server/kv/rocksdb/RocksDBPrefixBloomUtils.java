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
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KeyPrefixLayout;
import org.apache.fluss.types.RowType;

import io.github.fluss_contrib.rocksdb.PrefixLayout;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Adapts encoder-owned byte layouts to RocksDB prefix extractors. */
public final class RocksDBPrefixBloomUtils {
    private RocksDBPrefixBloomUtils() {}

    /** Derives the bucket-key prefix layout, or returns null when the option is disabled. */
    @Nullable
    public static PrefixLayout createPrefixLayout(TableDescriptor table) {
        return createPrefixLayout(
                table.getSchema().getRowType(),
                table.getPhysicalPrimaryKeys(),
                table.getBucketKeys(),
                new TableConfig(Configuration.fromMap(table.getProperties())));
    }

    /** Derives the layout from the persisted key-format and distribution metadata. */
    @Nullable
    public static PrefixLayout createPrefixLayout(TableInfo table) {
        return createPrefixLayout(
                table.getRowType(),
                table.getPhysicalPrimaryKeys(),
                table.getBucketKeys(),
                table.getTableConfig());
    }

    @Nullable
    private static PrefixLayout createPrefixLayout(
            RowType rowType,
            List<String> primaryKeys,
            List<String> bucketKeys,
            TableConfig config) {
        if (!config.isKvPrefixBloomEnabled()) {
            return null;
        }
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
        KeyPrefixLayout layout =
                encoder.getPrefixLayout(bucketKeys.size())
                        .orElseThrow(
                                () ->
                                        new InvalidTableException(
                                                "Prefix Bloom filters cannot describe primary-key encoder "
                                                        + encoder.getClass().getSimpleName()
                                                        + "."));
        List<PrefixLayout.Step> steps = new ArrayList<>();
        for (KeyPrefixLayout.Component component : layout.getComponents()) {
            switch (component) {
                case FIXED1:
                    steps.add(PrefixLayout.fixed(1));
                    break;
                case FIXED2:
                    steps.add(PrefixLayout.fixed(2));
                    break;
                case FIXED4:
                    steps.add(PrefixLayout.fixed(4));
                    break;
                case FIXED8:
                    steps.add(PrefixLayout.fixed(8));
                    break;
                case VARINT32:
                    steps.add(PrefixLayout.varint32());
                    break;
                case VARINT64:
                    steps.add(PrefixLayout.varint64());
                    break;
                case BYTES_VARINT32:
                    steps.add(PrefixLayout.bytesVarint32());
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported key layout component: " + component);
            }
        }
        return PrefixLayout.of(steps.toArray(new PrefixLayout.Step[0]));
    }
}
