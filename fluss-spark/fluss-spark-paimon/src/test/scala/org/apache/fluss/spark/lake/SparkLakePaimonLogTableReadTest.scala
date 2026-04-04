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
import org.apache.fluss.flink.tiering.LakeTieringJobBuilder
import org.apache.fluss.flink.tiering.source.TieringSourceOptions
import org.apache.fluss.metadata.{DataLakeFormat, TableBucket}

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.nio.file.Files
import java.time.Duration

class SparkLakePaimonLogTableReadTest extends SparkLakeLogTableReadTestBase {

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setString("datalake.format", DataLakeFormat.PAIMON.toString)
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    warehousePath =
      Files.createTempDirectory("fluss-testing-lake-read").resolve("warehouse").toString
    conf.setString("datalake.paimon.warehouse", warehousePath)
    conf
  }

  override protected def tierToLake(tableName: String): Unit = {
    val tableId = loadFlussTable(createTablePath(tableName)).getTableInfo.getTableId

    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    execEnv.setParallelism(2)
    execEnv.enableCheckpointing(1000)

    val flussConfig = new Configuration(flussServer.getClientConfig)
    flussConfig.set(TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(500L))

    val lakeCatalogConf = new Configuration()
    lakeCatalogConf.setString("metastore", "filesystem")
    lakeCatalogConf.setString("warehouse", warehousePath)

    val jobClient = LakeTieringJobBuilder
      .newBuilder(
        execEnv,
        flussConfig,
        lakeCatalogConf,
        new Configuration(),
        DataLakeFormat.PAIMON.toString)
      .build()

    try {
      val tableBucket = new TableBucket(tableId, 0)
      val deadline = System.currentTimeMillis() + 120000
      var synced = false
      while (!synced && System.currentTimeMillis() < deadline) {
        try {
          val replica = flussServer.waitAndGetLeaderReplica(tableBucket)
          synced = replica.getLogTablet.getLakeTableSnapshotId >= 0
        } catch {
          case _: Exception =>
        }
        if (!synced) Thread.sleep(500)
      }
      assert(synced, s"Bucket $tableBucket not synced to lake within 2 minutes")
    } finally {
      jobClient.cancel().get()
    }
  }
}
