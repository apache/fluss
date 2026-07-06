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

import org.apache.flink.table.catalog.TableChange;

import java.util.List;
import java.util.Optional;

/**
 * An adapter for the Flink {@link TableChange}s that are only available in Flink versions newer
 * than the one this module is compiled against, and therefore cannot be referenced directly here
 * (e.g. {@code TableChange.ModifyDefinitionQuery}, introduced in Flink 2.0).
 *
 * <p>A version-specific override provides the conversion. To support a new version-specific table
 * change in the future, only the corresponding override needs a new branch in {@link #convert} - no
 * extra methods or call-site changes are required.
 *
 * <p>TODO: remove this class when no longer support all the Flink 1.x series.
 */
public class TableChangeAdapter {

    private TableChangeAdapter() {}

    /**
     * Converts a version-specific Flink {@link TableChange} into a list of Fluss {@link
     * org.apache.fluss.metadata.TableChange}.
     *
     * @return the converted Fluss table changes, or {@link Optional#empty()} if the given change is
     *     not a version-specific table change handled by this adapter (the caller should then treat
     *     it as unsupported).
     */
    public static Optional<List<org.apache.fluss.metadata.TableChange>> convert(
            TableChange tableChange) {
        // The Flink version this module is compiled against has no version-specific table change to
        // convert.
        return Optional.empty();
    }
}
