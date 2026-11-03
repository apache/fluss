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

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Preserves a field on retract while retaining its configured forward aggregation behavior. */
public final class FieldIgnoreRetractAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final FieldAggregator delegate;

    /** Wraps the field's forward aggregator while preserving its initialization and reset rules. */
    public FieldIgnoreRetractAgg(FieldAggregator delegate) {
        super(delegate.fieldType);
        this.delegate = delegate;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        return delegate.agg(accumulator, inputField);
    }

    @Override
    public Object aggReversed(Object accumulator, Object inputField) {
        return delegate.aggReversed(accumulator, inputField);
    }

    @Override
    public boolean requiresAggOnFirstWrite() {
        return delegate.requiresAggOnFirstWrite();
    }

    @Override
    public Object retract(Object accumulator, Object retractField) {
        return accumulator;
    }

    @Override
    public boolean supportsRetract() {
        return true;
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
