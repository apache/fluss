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
 * The lookup-er is used to lookup data of specify kv table from Fluss.
 *
 * @since 0.6
 */
@PublicEvolving
public interface Lookuper {

    /**
     * Lookups certain row from the given table primary keys.
     *
     * @param lookupKey the given table primary keys.
     * @return the result of get.
     */
    CompletableFuture<LookupResult> lookup(InternalRow lookupKey);
}
