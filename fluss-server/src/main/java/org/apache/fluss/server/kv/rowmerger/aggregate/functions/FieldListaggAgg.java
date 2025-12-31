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

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.StringType;
import org.apache.fluss.utils.BinaryStringUtils;

/** List aggregation aggregator - concatenates string values with a delimiter. */
public class FieldListaggAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final BinaryString delimiter;

    public FieldListaggAgg(StringType dataType, String delimiter) {
        super(dataType);
        // Cache delimiter as BinaryString to avoid repeated conversions
        this.delimiter = BinaryString.fromString(delimiter);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        BinaryString mergeFieldSD = (BinaryString) accumulator;
        BinaryString inFieldSD = (BinaryString) inputField;

        // Use optimized concat method to avoid string conversion and reduce memory allocation
        return BinaryStringUtils.concat(mergeFieldSD, delimiter, inFieldSD);
    }
}
