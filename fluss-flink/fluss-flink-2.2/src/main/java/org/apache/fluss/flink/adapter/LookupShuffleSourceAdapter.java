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

import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.FlinkLookupShuffleTableSource;
import org.apache.fluss.flink.source.FlinkTableSource;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

/**
 * Flink 2.x implementation: wraps the source so it implements {@code SupportsLookupCustomShuffle},
 * but only when the table exposes the metadata required to reproduce Fluss bucket routing. Shadows
 * the common (no-op) class of the same fully-qualified name at package time.
 *
 * <p>Eligibility is decided here, at planning time, rather than inside {@code getPartitioner()}:
 *
 * <ul>
 *   <li>the table is not lookup-shuffle eligible, or {@code bucket.num} is not configured -&gt;
 *       leave the source unwrapped so that an explicitly requested lookup shuffle falls back to
 *       Flink's default hash distribution;
 *   <li>{@code bucket.num} configured but not a positive integer -&gt; fail fast with a {@link
 *       ValidationException} (a real misconfiguration);
 *   <li>otherwise wrap with the validated bucket count, so the wrapped source can always return a
 *       partitioner.
 * </ul>
 *
 * <p>The unwrapped (hash-fallback) branch is a defensive safety net rather than a reachable state
 * for a Fluss table: a Fluss table can only be created through the {@code FlussCatalog}, which
 * always defaults {@code bucket.num} and derives the bucket key from the primary key, while Flink
 * rejects creating a Fluss source as a temporary/{@code connector}-based table. It is therefore
 * covered by adapter unit tests rather than an end-to-end planner test.
 */
public class LookupShuffleSourceAdapter {

    private LookupShuffleSourceAdapter() {}

    /**
     * Wraps an eligible source with the Flink 2.x custom lookup shuffle ability.
     *
     * <p>An explicitly configured invalid bucket number is rejected regardless of eligibility.
     * Missing custom-shuffle metadata leaves the source unwrapped so Flink can apply its normal
     * hash fallback when lookup shuffle is requested.
     *
     * @param source source to wrap
     * @param tableOptions resolved table options
     * @param lookupShuffleEligible whether the table has a primary key and a non-empty bucket key
     * @return the wrapped source when custom bucket routing is available, otherwise the original
     *     source
     * @throws ValidationException if {@code bucket.num} is configured but is not a positive integer
     */
    public static FlinkTableSource maybeWithCustomShuffle(
            FlinkTableSource source, ReadableConfig tableOptions, boolean lookupShuffleEligible) {
        final Integer numBuckets;
        try {
            numBuckets = tableOptions.get(FlinkConnectorOptions.BUCKET_NUMBER);
        } catch (IllegalArgumentException e) {
            // bucket.num is present but not a valid integer.
            throw new ValidationException(
                    String.format(
                            "Invalid value for '%s': it must be a positive integer to enable the "
                                    + "Fluss custom lookup shuffle.",
                            FlinkConnectorOptions.BUCKET_NUMBER.key()),
                    e);
        }
        if (numBuckets != null && numBuckets <= 0) {
            throw new ValidationException(
                    String.format(
                            "Invalid value '%d' for '%s': it must be a positive integer to enable "
                                    + "the Fluss custom lookup shuffle.",
                            numBuckets, FlinkConnectorOptions.BUCKET_NUMBER.key()));
        }
        if (!lookupShuffleEligible || numBuckets == null) {
            // Custom-shuffle metadata is incomplete; do not advertise the ability so that Flink
            // falls back to a hash lookup shuffle instead of no shuffle at all.
            return source;
        }
        return new FlinkLookupShuffleTableSource(source, numBuckets);
    }
}
