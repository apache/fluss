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

package org.apache.fluss.spark

import org.apache.fluss.config.{AutoPartitionTimeUnit, Configuration => FlussConfiguration}
import org.apache.fluss.metadata.{DateTruncPartitionTransform, PartitionExpression, PartitionKey, Schema, TableDescriptor, TableInfo, TablePath}
import org.apache.fluss.types.DataTypes

import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

class SparkTableTest extends AnyFunSuite {

  test("partitioning rejects implicit partitioned table") {
    val tablePath = TablePath.of("db", "implicit_partition_table")
    val tableInfo = TableInfo.of(tablePath, 1, 0, implicitPartitionDescriptor(), null, 0, 0)
    val sparkTable = new SparkTable(tablePath, tableInfo, new FlussConfiguration(), null)

    val exception = intercept[UnsupportedOperationException] {
      sparkTable.partitioning()
    }

    assertThat(exception)
      .hasMessage("Spark connector does not support implicit partitioned Fluss tables yet.")
  }

  private def implicitPartitionDescriptor(): TableDescriptor = {
    val schema = Schema
      .newBuilder()
      .column("event_time", DataTypes.TIMESTAMP().copy(false))
      .column("payload", DataTypes.STRING())
      .build()
    TableDescriptor
      .builder()
      .schema(schema)
      .partitionedByKeys(PartitionKey.expression(PartitionExpression
        .of("event_day", DateTruncPartitionTransform.of("event_time", AutoPartitionTimeUnit.DAY))))
      .distributedBy(1)
      .build()
  }
}
