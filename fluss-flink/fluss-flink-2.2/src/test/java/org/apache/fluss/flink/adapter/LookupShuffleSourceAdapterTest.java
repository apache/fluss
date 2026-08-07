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

package org.apache.fluss.flink.adapter;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.FlinkLookupShuffleTableSource;
import org.apache.fluss.flink.source.FlinkTableSource;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.abilities.SupportsLookupCustomShuffle;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the Flink 2.x {@link LookupShuffleSourceAdapter}, which decides at planning time
 * whether to advertise the custom lookup shuffle ability.
 *
 * <p>The behavior matrix under test:
 *
 * <ul>
 *   <li>eligible + valid {@code bucket.num} -&gt; wrap with {@link FlinkLookupShuffleTableSource};
 *   <li>eligible but {@code bucket.num} missing -&gt; leave unwrapped so Flink falls back to its
 *       default (hash) lookup shuffle (the ability is best-effort, missing metadata must not fail
 *       the query);
 *   <li>not eligible (no primary key / no bucket key) -&gt; leave unwrapped;
 *   <li>{@code bucket.num} present but not a positive integer -&gt; fail fast with a {@link
 *       ValidationException}, regardless of eligibility.
 * </ul>
 */
class LookupShuffleSourceAdapterTest {

    private static FlinkTableSource tableSource() {
        RowType tableOutputType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()))
                                .getLogicalType();
        Configuration flussConfig = new Configuration();
        flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");
        FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                new FlinkConnectorOptionsUtils.StartupOptions();
        startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.EARLIEST;
        return new FlinkTableSource(
                TablePath.of("test_db", "dim"),
                flussConfig,
                new TableConfig(new Configuration()),
                tableOutputType,
                new int[] {0}, // primary key indexes (id)
                new int[] {0}, // bucket key indexes (id)
                new int[] {}, // partition key indexes
                true, // streaming
                startupOptions,
                false, // lookup async
                false, // insert if not exists
                null, // cache
                1000L, // scan partition discovery interval
                false, // is data lake enabled
                null, // merge engine type
                new HashMap<>(),
                null); // lease context
    }

    private static org.apache.flink.configuration.Configuration options(String bucketNum) {
        org.apache.flink.configuration.Configuration options =
                new org.apache.flink.configuration.Configuration();
        if (bucketNum != null) {
            options.setString(FlinkConnectorOptions.BUCKET_NUMBER.key(), bucketNum);
        }
        return options;
    }

    @Test
    void testWrapsWhenEligibleWithValidBucketNumber() {
        FlinkTableSource source = tableSource();
        FlinkTableSource result =
                LookupShuffleSourceAdapter.maybeWithCustomShuffle(source, options("3"), true);
        assertThat(result)
                .isInstanceOf(SupportsLookupCustomShuffle.class)
                .isInstanceOf(FlinkLookupShuffleTableSource.class);
    }

    @Test
    void testNotWrappedWhenBucketNumberMissing() {
        // Eligible table, but the optional bucket.num is not configured: leave the source unwrapped
        // so Flink applies its default (hash) lookup shuffle rather than no shuffle at all.
        FlinkTableSource source = tableSource();
        FlinkTableSource result =
                LookupShuffleSourceAdapter.maybeWithCustomShuffle(source, options(null), true);
        assertThat(result).isSameAs(source);
        assertThat(result).isNotInstanceOf(SupportsLookupCustomShuffle.class);
    }

    @Test
    void testNotWrappedWhenNotEligible() {
        // The factory reports the table has no primary key and/or no bucket key, so bucket routing
        // cannot be reproduced even if bucket.num is present.
        FlinkTableSource source = tableSource();
        FlinkTableSource result =
                LookupShuffleSourceAdapter.maybeWithCustomShuffle(source, options("3"), false);
        assertThat(result).isSameAs(source);
        assertThat(result).isNotInstanceOf(SupportsLookupCustomShuffle.class);
    }

    @Test
    void testFailFastOnNonPositiveBucketNumber() {
        FlinkTableSource source = tableSource();
        for (String invalid : new String[] {"0", "-1"}) {
            assertThatThrownBy(
                            () ->
                                    LookupShuffleSourceAdapter.maybeWithCustomShuffle(
                                            source, options(invalid), true))
                    .as("bucket.num=%s must fail fast", invalid)
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(FlinkConnectorOptions.BUCKET_NUMBER.key());
        }
    }

    @Test
    void testFailFastOnNonNumericBucketNumber() {
        FlinkTableSource source = tableSource();
        assertThatThrownBy(
                        () ->
                                LookupShuffleSourceAdapter.maybeWithCustomShuffle(
                                        source, options("abc"), true))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(FlinkConnectorOptions.BUCKET_NUMBER.key());
    }

    @Test
    void testFailFastOnInvalidBucketNumberEvenWhenNotEligible() {
        // An explicitly configured invalid bucket.num is a real misconfiguration and must be
        // rejected before the eligibility check, so it fails even for a non-eligible table.
        FlinkTableSource source = tableSource();
        for (String invalid : new String[] {"abc", "0"}) {
            assertThatThrownBy(
                            () ->
                                    LookupShuffleSourceAdapter.maybeWithCustomShuffle(
                                            source, options(invalid), false))
                    .as("bucket.num=%s must fail fast even when not eligible", invalid)
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(FlinkConnectorOptions.BUCKET_NUMBER.key());
        }
    }
}
