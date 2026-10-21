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

package org.apache.fluss.spark.tiering

import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{DataLakeFormat, TableBucket}
import org.apache.fluss.spark.FlussSparkTestBase

import org.apache.spark.internal.Logging
import org.assertj.core.api.Assertions.assertThat

import java.time.Duration

import scala.collection.JavaConverters._

/**
 * Integration test for the Spark tiering pipeline on log tables.
 *
 * Uses the Spark tiering components directly (TieringSplitGenerator, TieringTask, TieringCommitter)
 * instead of the Flink-based LakeTieringJobBuilder, to validate that the Spark tiering path
 * produces correct lake data that can be read back via Spark SQL.
 */
abstract class SparkTieringTestBase extends FlussSparkTestBase with Logging {

  protected var warehousePath: String = _
  private val SYNC_TIMEOUT: Duration = Duration.ofMinutes(2)
  private val SYNC_POLL_INTERVAL_MS = 500L

  protected def dataLakeFormat: DataLakeFormat

  protected def lakeConfig: Configuration

  def tierToLakeViaSpark(tableName: String): Unit = {
    val tablePath = createTablePath(tableName)
    val tableInfo = admin.getTableInfo(tablePath).get()

    val offsetsRetriever = new BucketOffsetsRetrieverImpl(admin, tablePath)
    def bucketOffsets(
        partitionId: java.lang.Long,
        partitionName: String,
        bucketCount: Int): Map[TableBucket, Long] = {
      offsetsRetriever
        .latestOffsets(partitionName, (0 until bucketCount).map(Int.box).asJava)
        .asScala
        .map {
          case (bucket, offset) =>
            new TableBucket(tableInfo.getTableId, partitionId, bucket) -> offset.longValue()
        }
        .toMap
    }
    val expectedOffsets = if (tableInfo.isPartitioned) {
      admin
        .listPartitionInfos(tablePath)
        .get()
        .asScala
        .flatMap {
          partition =>
            bucketOffsets(
              partition.getPartitionId,
              partition.getPartitionName,
              partition.getBucketCount)
        }
        .toMap
    } else {
      bucketOffsets(null, null, tableInfo.getNumBuckets)
    }
    val flussConfig = flussServer.getClientConfig
    flussConfig.set(SparkTieringOptions.POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(100))
    val lakeTieringConfig = new Configuration()

    val runner =
      new SparkTieringJobRunner(
        spark,
        flussConfig,
        lakeTieringConfig,
        dataLakeFormat.toString,
        lakeConfig)

    try {
      val termination = runner.startAsync()
      waitForLakeSnapshotSync(tableInfo, expectedOffsets, termination)
    } finally {
      runner.stop()
    }

  }

  /** Polls replicas until their lake offsets cover the writes made before this tiering round. */
  private def waitForLakeSnapshotSync(
      tableInfo: org.apache.fluss.metadata.TableInfo,
      expectedOffsets: Map[TableBucket, Long],
      termination: scala.concurrent.Future[Unit]): Unit = {
    val tableBuckets = expectedOffsets.filter { case (_, offset) => offset > 0 }.keySet

    val deadline = System.currentTimeMillis() + SYNC_TIMEOUT.toMillis
    val syncedBuckets = scala.collection.mutable.Set[TableBucket]()

    while (syncedBuckets.size < tableBuckets.size && System.currentTimeMillis() < deadline) {
      termination.value.foreach(_.get)
      tableBuckets.foreach {
        tableBucket =>
          if (!syncedBuckets.contains(tableBucket)) {
            try {
              val replica = flussServer.waitAndGetLeaderReplica(tableBucket)
              if (
                replica.getLogTablet.getLakeTableSnapshotId >= 0 &&
                replica.getLogTablet.getLakeLogEndOffset >= expectedOffsets(tableBucket)
              ) {
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

    assertThat(syncedBuckets.size)
      .withFailMessage(s"Not all buckets synced to lake within $SYNC_TIMEOUT")
      .isEqualTo(tableBuckets.size)
    logInfo(s"${tableInfo.getTablePath} synced to lake")
  }
}
