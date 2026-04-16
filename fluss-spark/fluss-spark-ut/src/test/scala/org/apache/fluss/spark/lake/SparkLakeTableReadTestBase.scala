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
import org.apache.fluss.spark.FlussSparkTestBase

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.time.Duration

import scala.jdk.CollectionConverters._

/**
 * Base class for lake-enabled table read tests. Subclasses provide the lake format config and lake
 * catalog configuration.
 */
abstract class SparkLakeTableReadTestBase extends FlussSparkTestBase {

  protected var warehousePath: String = _

  /** The lake format used by this test. */
  protected def dataLakeFormat: DataLakeFormat

  /** Lake catalog configuration specific to the format. */
  protected def lakeCatalogConf: Configuration

  private val TIERING_PARALLELISM = 2
  private val CHECKPOINT_INTERVAL_MS = 1000L
  private val POLL_INTERVAL: Duration = Duration.ofMillis(500L)
  private val SYNC_TIMEOUT: Duration = Duration.ofMinutes(2)
  private val SYNC_POLL_INTERVAL_MS = 500L

  /** Tier all pending data for the given table to the lake. */
  protected def tierToLake(tableName: String): Unit = {
    val tablePath = createTablePath(tableName)
    val tableInfo = loadFlussTable(tablePath).getTableInfo
    val tableId = tableInfo.getTableId
    val numBuckets = tableInfo.getNumBuckets

    val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
    execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    execEnv.setParallelism(TIERING_PARALLELISM)
    execEnv.enableCheckpointing(CHECKPOINT_INTERVAL_MS)

    val flussConfig = new Configuration(flussServer.getClientConfig)
    flussConfig.set(TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL, POLL_INTERVAL)

    val jobClient = LakeTieringJobBuilder
      .newBuilder(
        execEnv,
        flussConfig,
        lakeCatalogConf,
        new Configuration(),
        dataLakeFormat.toString)
      .build()

    try {
      // Collect all buckets to wait for sync
      val tableBuckets = if (tableInfo.isPartitioned) {
        val partitionInfos = admin.listPartitionInfos(tablePath).get()
        partitionInfos.asScala.flatMap {
          partitionInfo =>
            (0 until numBuckets).map {
              bucket => new TableBucket(tableId, partitionInfo.getPartitionId, bucket)
            }
        }.toSet
      } else {
        (0 until numBuckets).map(bucket => new TableBucket(tableId, bucket)).toSet
      }

      val deadline = System.currentTimeMillis() + SYNC_TIMEOUT.toMillis
      val syncedBuckets = scala.collection.mutable.Set[TableBucket]()

      while (syncedBuckets.size < tableBuckets.size && System.currentTimeMillis() < deadline) {
        tableBuckets.foreach {
          tableBucket =>
            if (!syncedBuckets.contains(tableBucket)) {
              try {
                val replica = flussServer.waitAndGetLeaderReplica(tableBucket)
                if (replica.getLogTablet.getLakeTableSnapshotId >= 0) {
                  syncedBuckets.add(tableBucket)
                }
              } catch {
                case _: Exception =>
              }
            }
        }
        if (syncedBuckets.size < tableBuckets.size) {
          Thread.sleep(SYNC_POLL_INTERVAL_MS)
        }
      }

      assert(
        syncedBuckets.size == tableBuckets.size,
        s"Not all buckets synced to lake within $SYNC_TIMEOUT. " +
          s"Synced: ${syncedBuckets.size}, Total: ${tableBuckets.size}"
      )
    } finally {
      jobClient.cancel().get()
    }
  }

  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      tableNames.foreach(t => sql(s"DROP TABLE IF EXISTS $DEFAULT_DATABASE.$t"))
    }
  }

}
