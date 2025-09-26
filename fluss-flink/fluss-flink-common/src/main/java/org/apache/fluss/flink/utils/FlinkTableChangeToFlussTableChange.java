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

import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.metadata.TableChange;

/** convert Flink's TableChange class to {@link TableChange}. */
public class FlinkTableChangeToFlussTableChange {

    public static TableChange toFlussTableChange(
            org.apache.flink.table.catalog.TableChange tableChange) {
        TableChange flussTableChange;
        if (tableChange instanceof org.apache.flink.table.catalog.TableChange.SetOption) {
            org.apache.flink.table.catalog.TableChange.SetOption setOption =
                    (org.apache.flink.table.catalog.TableChange.SetOption) tableChange;
            if (setOption.getKey().equals(FlinkConnectorOptions.BUCKET_NUMBER.key())) {
                flussTableChange =
                        convertBucketNumOption(
                                (org.apache.flink.table.catalog.TableChange.SetOption) tableChange);
            } else {
                flussTableChange =
                        convertSetOption(
                                (org.apache.flink.table.catalog.TableChange.SetOption) tableChange);
            }
        } else if (tableChange instanceof org.apache.flink.table.catalog.TableChange.ResetOption) {
            flussTableChange =
                    convertResetOption(
                            (org.apache.flink.table.catalog.TableChange.ResetOption) tableChange);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported flink table change: %s.", tableChange));
        }
        return flussTableChange;
    }

    private static TableChange.BucketNumOption convertBucketNumOption(
            org.apache.flink.table.catalog.TableChange.SetOption flinkBucketNumOption) {
        try {
            return TableChange.bucketNum(Integer.parseInt(flinkBucketNumOption.getValue()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid bucket num: %s.", flinkBucketNumOption.getValue()), e);
        }
    }

    private static TableChange.SetOption convertSetOption(
            org.apache.flink.table.catalog.TableChange.SetOption flinkSetOption) {
        return TableChange.set(flinkSetOption.getKey(), flinkSetOption.getValue());
    }

    private static TableChange.ResetOption convertResetOption(
            org.apache.flink.table.catalog.TableChange.ResetOption flinkResetOption) {
        return TableChange.reset(flinkResetOption.getKey());
    }
}
