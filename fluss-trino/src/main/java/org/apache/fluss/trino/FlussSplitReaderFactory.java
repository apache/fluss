/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.client.table.Table;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

final class FlussSplitReaderFactory {

    private FlussSplitReaderFactory() {}

    static FlussSplitReader create(Table table, FlussSplit split) {
        checkNotNull(table, "table is null");
        checkNotNull(split, "split is null");

        switch (split.getScanType()) {
            case LOG:
                return new FlussLogSplitReader(
                        table, split.getBucket(), split.getRequiredLogRange());
            case KV:
                return new FlussKvSplitReader(table, split.getBucket());
            default:
                throw new IllegalArgumentException(
                        "Unsupported Fluss scan type: " + split.getScanType());
        }
    }
}
