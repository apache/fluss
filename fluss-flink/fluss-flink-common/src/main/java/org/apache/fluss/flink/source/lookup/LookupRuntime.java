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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.lookup.LookupResult;

import org.apache.flink.table.data.RowData;

import java.util.concurrent.CompletableFuture;

/** Runtime abstraction for Flink lookup functions. */
@Internal
public interface LookupRuntime extends AutoCloseable {

    /** Opens the runtime resources. */
    void open();

    /** Looks up the given normalized key row. */
    CompletableFuture<LookupResult> lookup(RowData keyRow);
}
