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

package org.apache.fluss.server.log;

import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.LogRecordReadContext;

import javax.annotation.Nullable;

/**
 * Encapsulates the filter-related parameters for server-side filter pushdown during log reads.
 *
 * <p>All three parameters are logically coupled: the predicate defines what to filter, the read
 * context provides batch statistics for filter evaluation, and the predicate resolver handles
 * schema evolution. This class enforces that they are always passed together.
 */
public final class FilterContext {

    private final Predicate recordBatchFilter;
    private final LogRecordReadContext readContext;
    @Nullable private final PredicateSchemaResolver predicateResolver;

    public FilterContext(
            Predicate recordBatchFilter,
            LogRecordReadContext readContext,
            @Nullable PredicateSchemaResolver predicateResolver) {
        this.recordBatchFilter = recordBatchFilter;
        this.readContext = readContext;
        this.predicateResolver = predicateResolver;
    }

    public Predicate getRecordBatchFilter() {
        return recordBatchFilter;
    }

    public LogRecordReadContext getReadContext() {
        return readContext;
    }

    @Nullable
    public PredicateSchemaResolver getPredicateResolver() {
        return predicateResolver;
    }
}
