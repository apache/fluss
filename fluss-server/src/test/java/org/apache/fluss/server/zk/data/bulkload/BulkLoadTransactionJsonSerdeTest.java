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

package org.apache.fluss.server.zk.data.bulkload;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.json.JsonSerdeTestBase;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the persisted BulkLoadTransaction format. */
class BulkLoadTransactionJsonSerdeTest extends JsonSerdeTestBase<BulkLoadTransaction> {
    BulkLoadTransactionJsonSerdeTest() {
        super(BulkLoadTransactionJsonSerde.INSTANCE);
    }

    @Override
    protected BulkLoadTransaction[] createObjects() {
        return new BulkLoadTransaction[] {
            new BulkLoadTransaction(
                    42,
                    "job",
                    TablePath.of("db", "orders"),
                    7,
                    "file:/a",
                    BulkLoadTransaction.RUNNING,
                    10,
                    null,
                    null),
            new BulkLoadTransaction(
                    42,
                    "job",
                    TablePath.of("db", "orders"),
                    7,
                    "file:/a",
                    BulkLoadTransaction.ABORTED,
                    10,
                    20L,
                    "cancelled")
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"table_id\":42,\"bulk_load_id\":\"job\",\"database_name\":\"db\",\"table_name\":\"orders\",\"schema_id\":7,\"remote_data_dir\":\"file:/a\",\"state\":0,\"created_time\":10}",
            "{\"version\":1,\"table_id\":42,\"bulk_load_id\":\"job\",\"database_name\":\"db\",\"table_name\":\"orders\",\"schema_id\":7,\"remote_data_dir\":\"file:/a\",\"state\":2,\"created_time\":10,\"finished_time\":20,\"abort_message\":\"cancelled\"}"
        };
    }

    @Override
    protected void assertEquals(BulkLoadTransaction actual, BulkLoadTransaction expected) {
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }
}
