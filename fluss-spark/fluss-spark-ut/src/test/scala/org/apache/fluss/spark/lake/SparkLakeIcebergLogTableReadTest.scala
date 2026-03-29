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
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory
import org.apache.fluss.lake.writer.LakeTieringFactory
import org.apache.fluss.metadata.DataLakeFormat

import java.nio.file.Files

class SparkLakeIcebergLogTableReadTest extends SparkLakeLogTableReadTestBase {

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.ICEBERG.toString)
    conf.setString("datalake.iceberg.type", "hadoop")
    warehousePath =
      Files.createTempDirectory("fluss-testing-iceberg-lake-read").resolve("warehouse").toString
    conf.setString("datalake.iceberg.warehouse", warehousePath)
    conf
  }

  override protected def createLakeTieringFactory(): LakeTieringFactory[_, _] = {
    val icebergConfig = new Configuration()
    icebergConfig.setString("type", "hadoop")
    icebergConfig.setString("warehouse", warehousePath)
    new IcebergLakeTieringFactory(icebergConfig)
  }
}
