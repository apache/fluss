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

package org.apache.fluss.lake.lance;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DateTruncPartitionTransform;
import org.apache.fluss.metadata.PartitionExpression;
import org.apache.fluss.metadata.PartitionKey;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LanceLakeCatalog}. */
class LanceLakeCatalogTest {

    @Test
    void testCreateTableRejectsImplicitPartition() {
        LanceLakeCatalog catalog = new LanceLakeCatalog(new Configuration());

        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        TablePath.of("test_db", "implicit_partition_table"),
                                        implicitPartitionDescriptor(),
                                        null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Lance lake tables do not support implicit partition expressions yet.");
    }

    private static TableDescriptor implicitPartitionDescriptor() {
        Schema schema =
                Schema.newBuilder()
                        .column("event_time", DataTypes.TIMESTAMP().copy(false))
                        .column("payload", DataTypes.STRING())
                        .build();
        return TableDescriptor.builder()
                .schema(schema)
                .partitionedByKeys(
                        PartitionKey.expression(
                                PartitionExpression.of(
                                        "event_day",
                                        DateTruncPartitionTransform.of(
                                                "event_time", AutoPartitionTimeUnit.DAY))))
                .distributedBy(1)
                .build();
    }
}
