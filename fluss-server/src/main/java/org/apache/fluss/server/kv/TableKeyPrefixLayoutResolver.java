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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KeyPrefixLayout;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** Resolves table-specific prefix layouts from the primary-key encoder. */
public final class TableKeyPrefixLayoutResolver {
    private TableKeyPrefixLayoutResolver() {}

    /** Derives the bucket-key prefix layout, or returns null when the option is disabled. */
    @Nullable
    public static KeyPrefixLayout resolve(TableDescriptor table) {
        return resolve(
                table.getSchema().getRowType(),
                table.getPhysicalPrimaryKeys(),
                table.getBucketKeys(),
                new TableConfig(Configuration.fromMap(table.getProperties())));
    }

    /** Derives the layout from the persisted key-format and distribution metadata. */
    @Nullable
    public static KeyPrefixLayout resolve(TableInfo table) {
        return resolve(
                table.getRowType(),
                table.getPhysicalPrimaryKeys(),
                table.getBucketKeys(),
                table.getTableConfig());
    }

    @Nullable
    private static KeyPrefixLayout resolve(
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
        return encoder.getPrefixLayout(bucketKeys.size())
                .orElseThrow(
                        () ->
                                new InvalidTableException(
                                        "Prefix Bloom filters cannot describe primary-key encoder "
                                                + encoder.getClass().getSimpleName()
                                                + "."));
    }
}
