/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.concurrent.CompletableFuture;

/**
 * The prefix lookup-er is used to lookup rows of a primary key table by a prefix of primary key.
 *
 * @since 0.6
 */
@PublicEvolving
public interface PrefixLookuper {

    /**
     * Prefix lookup certain rows from the given table by a prefix of primary key.
     *
     * <p>Only available for Primary Key Table. Will throw exception when the table isn't a Primary
     * Key Table.
     *
     * <p>Note: Currently, if you want to use prefix lookup, the table you created must both define
     * the primary key and the bucket key, in addition, the prefixKey needs to be equals with bucket
     * key, and to be a part of the primary key and must be a prefix of the primary key. For
     * example, if a table has fields [a,b,c,d], and the primary key is set to [a, b, c], with the
     * bucket key set to [a, b], then the schema of the prefixKey would also be [a, b]. This pattern
     * can use PrefixLookup to lookup by prefix scan.
     *
     * <p>TODO: currently, the interface only support bucket key as the prefixKey to lookup.
     * Generalize the prefix lookup to support any prefixKey including bucket key.
     *
     * <p>We also support prefix lookup for partitioned table. The schema of the prefixKey should
     * contain partition fields and bucket key. In addition, the schema of the prefixKey exclude
     * partition fields should be a prefix of primary key exclude partition fields.
     *
     * @param prefixKey the given prefix key to do prefix lookup.
     * @return the result of prefix lookup.
     */
    CompletableFuture<PrefixLookupResult> prefixLookup(InternalRow prefixKey);
}
