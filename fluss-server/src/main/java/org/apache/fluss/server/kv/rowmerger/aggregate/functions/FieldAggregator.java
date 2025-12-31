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

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;

import java.io.Serializable;

/** Abstract class for aggregating a field of a row. */
public abstract class FieldAggregator implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final DataType fieldType;
    protected final DataTypeRoot typeRoot;

    public FieldAggregator(DataType dataType) {
        this.fieldType = dataType;
        this.typeRoot = dataType.getTypeRoot();
    }

    /**
     * Aggregates the accumulator with the input field.
     *
     * @param accumulator the current accumulator value
     * @param inputField the input field value to aggregate
     * @return the aggregated result
     */
    public abstract Object agg(Object accumulator, Object inputField);

    /**
     * Aggregates the accumulator with the input field in reversed order. By default, it calls
     * {@link #agg(Object, Object)} with swapped parameters.
     *
     * @param accumulator the current accumulator value
     * @param inputField the input field value to aggregate
     * @return the aggregated result
     */
    public Object aggReversed(Object accumulator, Object inputField) {
        return agg(inputField, accumulator);
    }

    /** Resets the aggregator to a clean start state. */
    public void reset() {}
}
