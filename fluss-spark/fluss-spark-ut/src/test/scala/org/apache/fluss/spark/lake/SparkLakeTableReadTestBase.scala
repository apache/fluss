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

import org.apache.fluss.client.initializer.BucketOffsetsRetrieverImpl
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{DataLakeFormat, TableBucket}
import org.apache.fluss.spark.FlussSparkTestBase
import org.apache.fluss.spark.read.{FlussAppendScan, FlussScan, FlussUpsertScan}
import org.apache.fluss.spark.tiering.{SparkTieringJobRunner, SparkTieringOptions}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation}
import org.assertj.core.api.Assertions.assertThat

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

  private val POLL_INTERVAL: Duration = Duration.ofMillis(500L)
  private val SYNC_TIMEOUT: Duration = Duration.ofMinutes(2)
  private val SYNC_POLL_INTERVAL_MS = 500L

  /** Tier all pending data for the given table using the Spark service. */
  protected def tierToLake(tableName: String): Unit = {
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
    val tableBuckets = expectedOffsets.filter { case (_, offset) => offset > 0 }.keySet
    val flussConfig = new Configuration(flussServer.getClientConfig)
    flussConfig.set(SparkTieringOptions.POLL_TIERING_TABLE_INTERVAL, POLL_INTERVAL)
    val runner = new SparkTieringJobRunner(
      spark,
      flussConfig,
      new Configuration(),
      dataLakeFormat.toString,
      lakeCatalogConf)

    try {
      val termination = runner.startAsync()
      val deadline = System.currentTimeMillis() + SYNC_TIMEOUT.toMillis
      val syncedBuckets = scala.collection.mutable.Set[TableBucket]()

      while (syncedBuckets.size < tableBuckets.size && System.currentTimeMillis() < deadline) {
        // Propagate service failures immediately instead of waiting for the sync timeout.
        termination.value.foreach(_.get)
        tableBuckets.foreach {
          tableBucket =>
            if (!syncedBuckets.contains(tableBucket)) {
              val replica = flussServer.waitAndGetLeaderReplica(tableBucket)
              if (
                replica.getLogTablet.getLakeTableSnapshotId >= 0 &&
                replica.getLogTablet.getLakeLogEndOffset >= expectedOffsets(tableBucket)
              ) {
                syncedBuckets.add(tableBucket)
              }
            }
        }
        if (syncedBuckets.size < tableBuckets.size) {
          Thread.sleep(SYNC_POLL_INTERVAL_MS)
        }
      }

      assertThat(syncedBuckets.size)
        .withFailMessage(s"Not all buckets of $tablePath synced to lake within $SYNC_TIMEOUT")
        .isEqualTo(tableBuckets.size)
    } finally {
      runner.stop()
    }
  }

  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      tableNames.foreach(t => sql(s"DROP TABLE IF EXISTS $DEFAULT_DATABASE.$t"))
    }
  }

  protected def flussScan(df: DataFrame): Seq[FlussScan] = {
    val scans =
      df.queryExecution.executedPlan.collect {
        case b: BatchScanExec => b.scan
      } ++ df.queryExecution.optimizedPlan.collect {
        case DataSourceV2ScanRelation(_, scan, _, _, _) => scan
      }
    scans.collect { case s: FlussScan => s }
  }

  protected def pushedPredicates(df: DataFrame): Array[Predicate] = {
    flussScan(df)
      .collect { case f: FlussScan => f.pushedSparkPredicates }
      .flatten
      .toArray
  }

  protected def assertPushedNames(df: DataFrame, expected: Set[String]): Unit = {
    val pushed = pushedPredicates(df).map(_.name()).toSet
    assert(
      expected.exists(pushed.contains),
      s"Expected any of $expected in pushed predicates, got $pushed")
  }

  protected def lakeInputPartitions(df: DataFrame): Array[InputPartition] = {
    val scans =
      df.queryExecution.executedPlan.collect {
        case b: BatchScanExec => b.scan
      } ++ df.queryExecution.optimizedPlan.collect {
        case DataSourceV2ScanRelation(_, scan, _, _, _) => scan
      }
    scans
      .collect {
        case upsert: FlussUpsertScan => upsert
        case append: FlussAppendScan => append
      }
      .flatMap(_.toBatch.planInputPartitions())
      .toArray
  }
}
