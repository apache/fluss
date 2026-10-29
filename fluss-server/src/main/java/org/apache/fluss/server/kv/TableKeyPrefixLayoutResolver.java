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

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KeyPrefixLayout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/** Resolves compatible table prefix layouts without changing unsupported tables' behavior. */
public final class TableKeyPrefixLayoutResolver {
    private static final Logger LOG = LoggerFactory.getLogger(TableKeyPrefixLayoutResolver.class);

    private TableKeyPrefixLayoutResolver() {}

    /**
     * Returns a supported bucket-key layout, or null and logs the reason for skipping the table.
     */
    @Nullable
    public static KeyPrefixLayout resolve(TableInfo table) {
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
            KeyPrefixLayout layout = encoder.getPrefixLayout(bucketKeys.size()).orElse(null);
            if (layout == null) {
                LOG.info(
                        "Prefix Bloom filters disabled for table {} (id={}): unsupported key encoder {}.",
                        table.getTablePath(),
                        table.getTableId(),
                        encoder.getClass().getSimpleName());
            }
            return layout;
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            LOG.info(
                    "Prefix Bloom filters disabled for table {} (id={}): unsupported key layout: {}",
                    table.getTablePath(),
                    table.getTableId(),
                    e.getMessage());
            return null;
        }
    }
}
