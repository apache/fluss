/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.utils;

import org.apache.fluss.row.InternalRow;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * Runtime converter to convert field in Fluss's {@link InternalRow} to Flink's {@link RowData} type
 * object.
 */
@FunctionalInterface
public interface FlussDeserializationConverter extends Serializable {

    /**
     * Convert a Fluss field object of {@link InternalRow} to the Flink's internal data structure
     * object.
     *
     * @param flussField A single field of a {@link InternalRow}
     */
    Object deserialize(Object flussField);
}
