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

package org.apache.fluss.flink.utils;

import org.apache.fluss.metadata.FlussTableChange;

import org.apache.flink.table.catalog.TableChange;

/** convert Flink's TableChange class to {@link FlussTableChange}. */
public class FlinkTableChangeToFlussTableChange {

    public static FlussTableChange toFlussTableChange(TableChange tableChange) {
        FlussTableChange flussTableChange = null;
        if (tableChange instanceof TableChange.SetOption) {
            flussTableChange = convertSetOption((TableChange.SetOption) tableChange);
        } else if (tableChange instanceof TableChange.ResetOption) {
            flussTableChange = convertResetOption((TableChange.ResetOption) tableChange);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported flink table change: %s.", tableChange));
        }
        return flussTableChange;
    }

    private static FlussTableChange.SetOption convertSetOption(
            TableChange.SetOption flinkSetOption) {
        return FlussTableChange.set(flinkSetOption.getKey(), flinkSetOption.getValue());
    }

    private static FlussTableChange.ResetOption convertResetOption(
            TableChange.ResetOption flinkResetOption) {
        return FlussTableChange.reset(flinkResetOption.getKey());
    }
}
