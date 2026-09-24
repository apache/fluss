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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableInfo;

/**
 * An optional capability that a {@link LakeTieringFactory} may implement to support marking idle
 * partitions of a tiered table as done. Lake formats that do not implement this interface never
 * trigger mark-done maintenance.
 */
@Internal
public interface PartitionMarkDoneEnabler {

    /**
     * Whether partition mark-done is enabled for the given table, used to decide whether an empty
     * tiering round should still reach the committer to run mark-done maintenance.
     *
     * <p>The switch only recognizes the Fluss table metadata (e.g. custom properties): mark-done
     * options configured on the lake table directly are deliberately not honored, so the check is
     * cheap (no lake access) and there is a single source of truth.
     */
    boolean isPartitionMarkDoneEnabled(TableInfo tableInfo);
}
