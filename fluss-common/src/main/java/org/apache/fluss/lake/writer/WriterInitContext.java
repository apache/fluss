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

package org.apache.fluss.lake.writer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

/**
 * The WriterInitContext interface provides the context needed to create a LakeWriter. It includes
 * methods to obtain the table path, table bucket, an optional partition, and table metadata
 * information.
 *
 * @since 0.7
 */
@PublicEvolving
public interface WriterInitContext {

    /** Unknown split index for lake writers that do not need split-level coordination. */
    int UNKNOWN_SPLIT_INDEX = -1;

    /** Unknown tiering round timestamp for lake writers that do not need round coordination. */
    long UNKNOWN_TIERING_ROUND_TIMESTAMP = -1L;

    /**
     * Returns the table path.
     *
     * @return the table path
     */
    TablePath tablePath();

    /**
     * Returns the table bucket.
     *
     * @return the table bucket
     */
    TableBucket tableBucket();

    /**
     * Returns the partition, or null if there is no partition.
     *
     * @return the partition, or null if there is no partition
     */
    @Nullable
    String partition();

    /**
     * Returns the Fluss table info.
     *
     * @return the Fluss table info
     */
    TableInfo tableInfo();

    /**
     * Returns the split index in the current tiering round.
     *
     * <p>The split whose index is {@code 0} is the first split in the current tiering round. Lake
     * writers that need one writer to coordinate round-level metadata can use the first split as
     * the coordinator. The value is {@link #UNKNOWN_SPLIT_INDEX} when the lake writer does not need
     * this information.
     *
     * @return the split index in the current tiering round
     */
    default int splitIndex() {
        return UNKNOWN_SPLIT_INDEX;
    }

    /**
     * Returns the timestamp when the current tiering round was generated.
     *
     * <p>The value is {@link #UNKNOWN_TIERING_ROUND_TIMESTAMP} when the lake writer does not need
     * round-level timestamp coordination.
     *
     * @return the tiering round timestamp in milliseconds
     */
    default long tieringRoundTimestamp() {
        return UNKNOWN_TIERING_ROUND_TIMESTAMP;
    }
}
