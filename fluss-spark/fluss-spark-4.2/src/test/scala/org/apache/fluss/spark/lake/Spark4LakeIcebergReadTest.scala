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

package org.apache.fluss.spark.lake

import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.DataLakeFormat
import org.apache.fluss.spark.SparkFlussConf

import org.apache.spark.sql.DataFrame

import java.nio.file.Files

@SparkLakeTest
@SparkLakeIcebergTest
class Spark4LakeIcebergTimeRangeReadTest extends SparkLakeTimeRangeReadTest {
  override protected def dataLakeFormat: DataLakeFormat = DataLakeFormat.ICEBERG

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.ICEBERG.toString)
    conf.setString("datalake.iceberg.type", "hadoop")
    warehousePath =
      Files.createTempDirectory("fluss-spark4-iceberg-read").resolve("warehouse").toString
    conf.setString("datalake.iceberg.warehouse", warehousePath)
    conf
  }

  override protected def lakeCatalogConf: Configuration = {
    val conf = new Configuration()
    conf.setString("type", "hadoop")
    conf.setString("warehouse", warehousePath)
    conf
  }

  override protected def readTimeRange(tableName: String, start: Long, end: Long): DataFrame = {
    spark.read
      .option(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key(), start.toString)
      .option(SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key(), end.toString)
      .table(s"$DEFAULT_DATABASE.$tableName")
      .orderBy("id")
  }
}
