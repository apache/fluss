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

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.row.InternalRow;

import java.util.List;

/**
 * Used to describe the operation to prefix lookup by {@link PrefixLookuper} to a primary key table.
 *
 * @since 0.6
 */
@PublicEvolving
public class PrefixLookup {

    /**
     * Currently, For non-partitioned table, the lookupColumnNames can only be the field of bucket
     * key.
     *
     * <p>For partitioned table, the lookupColumnNames exclude partition fields should be a prefix
     * of primary key exclude partition fields.
     *
     * <p>See {@link PrefixLookuper#prefixLookup(InternalRow)} for more details.
     */
    private final List<String> lookupColumnNames;

    public PrefixLookup(List<String> lookupColumnNames) {
        this.lookupColumnNames = lookupColumnNames;
    }

    public List<String> getLookupColumnNames() {
        return lookupColumnNames;
    }
}
