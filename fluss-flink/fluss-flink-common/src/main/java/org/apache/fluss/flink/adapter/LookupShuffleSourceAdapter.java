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

import org.apache.fluss.flink.source.FlinkTableSource;

/**
 * Adapter that optionally wraps a {@link FlinkTableSource} with a version-specific ability.
 *
 * <p>This is the default (Flink 1.x) no-op implementation: {@code SupportsLookupCustomShuffle} does
 * not exist before Flink 2.0, so the source is returned unchanged. The {@code fluss-flink-2.2}
 * module ships a class with the same fully-qualified name that wraps the source with the custom
 * lookup shuffle ability; that class shadows this one at shade/package time.
 *
 * <p>TODO: remove this class when no longer supporting Flink 1.x.
 */
public class LookupShuffleSourceAdapter {

    private LookupShuffleSourceAdapter() {}

    public static FlinkTableSource maybeWithCustomShuffle(FlinkTableSource source) {
        return source;
    }
}
