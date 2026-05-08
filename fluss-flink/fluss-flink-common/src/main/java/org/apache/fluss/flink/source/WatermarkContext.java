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

package org.apache.fluss.flink.source;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;

/**
 * Encapsulates watermark configuration from SQL hints and Flink execution config.
 *
 * <p>This context holds all hint-based watermark parameters and the source idle timeout from
 * Flink's execution configuration. It is used by {@link FlinkTableSource} to resolve the effective
 * watermark strategy at runtime.
 *
 * <ol>
 *   <li>{@code enabled = false} -> disable all watermark (noWatermarks)
 *   <li>{@code column != null} -> hint-based watermark overrides table-level
 *   <li>Planner-pushed watermark strategy (table-level WATERMARK clause)
 *   <li>Default -> noWatermarks
 * </ol>
 */
public class WatermarkContext implements Serializable {

    private static final long serialVersionUID = 1L;

    /** A disabled watermark context that forces noWatermarks regardless of table definition. */
    public static final WatermarkContext DISABLED = new WatermarkContext(false, null, null, null);

    /** Default watermark context with watermark enabled but no hint override. */
    public static final WatermarkContext DEFAULT = new WatermarkContext(true, null, null, null);

    private final boolean enabled;
    @Nullable private final String column;
    @Nullable private final Duration delay;
    @Nullable private final Duration sourceIdleTimeout;

    public WatermarkContext(
            boolean enabled,
            @Nullable String column,
            @Nullable Duration delay,
            @Nullable Duration sourceIdleTimeout) {
        this.enabled = enabled;
        this.column = column;
        this.delay = delay;
        this.sourceIdleTimeout = sourceIdleTimeout;
    }

    /** Whether watermark generation is enabled. When false, all watermarks are disabled. */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * The column name used for hint-based watermark generation. When non-null, this overrides the
     * table-level WATERMARK definition.
     */
    @Nullable
    public String getColumn() {
        return column;
    }

    /**
     * The maximum out-of-orderness allowed for hint-based watermark generation. Only used when
     * {@link #getColumn()} is non-null.
     */
    @Nullable
    public Duration getDelay() {
        return delay;
    }

    /**
     * The source idle timeout from Flink's execution config (table.exec.source.idle-timeout). Used
     * to mark idle splits/buckets so they don't block watermark advancement.
     */
    @Nullable
    public Duration getSourceIdleTimeout() {
        return sourceIdleTimeout;
    }
}
