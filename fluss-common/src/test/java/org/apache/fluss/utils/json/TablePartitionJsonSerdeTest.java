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

package org.apache.fluss.utils.json;

import org.apache.fluss.metadata.TablePartition;

/** Test for {@link TablePartitionJsonSerde}. */
class TablePartitionJsonSerdeTest extends JsonSerdeTestBase<TablePartition> {

    TablePartitionJsonSerdeTest() {
        super(TablePartitionJsonSerde.INSTANCE);
    }

    @Override
    protected TablePartition[] createObjects() {
        return new TablePartition[] {
            new TablePartition(1, 2), new TablePartition(3, 4), new TablePartition(5, 6)
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"table_id\":1,\"partition_id\":2}",
            "{\"version\":1,\"table_id\":3,\"partition_id\":4}",
            "{\"version\":1,\"table_id\":5,\"partition_id\":6}"
        };
    }
}
