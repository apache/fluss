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

package com.alibaba.fluss.flink.utils;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Utils for Flink classes. */
public class FlinkUtils {

    /**
     * Returns projected {@link RowType} by given projection indexes over original {@link RowType}.
     *
     * @param rowType the original row type
     * @param projection the projection indexes
     */
    public static RowType projectRowType(RowType rowType, int[] projection) {
        LogicalType[] types = new LogicalType[projection.length];
        String[] names = new String[projection.length];
        for (int i = 0; i < projection.length; i++) {
            types[i] = rowType.getTypeAt(projection[i]);
            names[i] = rowType.getFieldNames().get(projection[i]);
        }
        return RowType.of(rowType.isNullable(), types, names);
    }
}
