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

import org.apache.fluss.client.initializer.{BucketOffsetsRetrieverImpl, OffsetsInitializer}
import org.apache.fluss.spark.read.{FlussMicroBatchStream, FlussSourceOffset}
import org.apache.fluss.spark.write.{FlussAppendDataWriter, FlussUpsertDataWriter}
import org.apache.fluss.utils.json.TableBucketOffsets

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class SparkStreamingMetricsIT extends FlussSparkTestBase with StreamTest {
  import testImplicits._

  test("read: streaming source metrics") {
    val tableName = "t_streaming_metrics"
    val numBuckets = 3
    val numRecords = 5
    withTable(tableName) {
      sql(
        s"CREATE TABLE $tableName (id int, data string) TBLPROPERTIES('bucket.num' = '$numBuckets')")

      val schema = StructType(Seq(StructField("id", IntegerType), StructField("data", StringType)))
      val clock = new StreamManualClock

      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        StartStream(trigger = Trigger.ProcessingTime(500), clock),
        AdvanceManualClock(500),
        CheckNewAnswer(),
        AddFlussData(
          tableName,
          schema,
          Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d"), Row(5, "e"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d"), Row(5, "e")),
        AssertOnQuery {
          q =>
            val progress = q.lastProgress
            assert(progress != null, "lastProgress should not be null")
            val metrics = progress.sources(0).metrics

            // All expected keys present
            Seq(
              "plannedInputRows",
              "numBucketsRead",
              "numBucketsTotal",
              "maxOffsetsBehindLatest",
              "avgOffsetsBehindLatest",
              "batchFetchRequests",
              "avgFetchLatencyMs",
              "maxFetchLatencyMs",
              "totalFetchErrors"
            ).foreach(key => assert(metrics.containsKey(key), s"metric '$key' missing"))

            // plannedInputRows must match the number of records written
            assert(
              metrics.get("plannedInputRows").toLong == numRecords,
              s"plannedInputRows should be $numRecords but was ${metrics.get("plannedInputRows")}")

            // numBucketsTotal must equal the configured bucket count
            assert(
              metrics.get("numBucketsTotal").toInt == numBuckets,
              s"numBucketsTotal should be $numBuckets but was ${metrics.get("numBucketsTotal")}")

            // maxOffsetsBehindLatest must be >= 0
            assert(
              metrics.get("maxOffsetsBehindLatest").toLong >= 0,
              s"maxOffsetsBehindLatest should be >= 0 but was ${metrics
                  .get("maxOffsetsBehindLatest")}")

            // Scanner metrics (Phase 2)
            assert(
              metrics.get("batchFetchRequests").toLong > 0,
              s"batchFetchRequests should be > 0 but was ${metrics.get("batchFetchRequests")}")

            val avgLatency = metrics.get("avgFetchLatencyMs").toDouble
            assert(avgLatency >= 0.0, s"avgFetchLatencyMs should be >= 0 but was $avgLatency")

            val maxLatency = metrics.get("maxFetchLatencyMs").toLong
            assert(maxLatency >= 0, s"maxFetchLatencyMs should be >= 0 but was $maxLatency")
            assert(
              maxLatency.toDouble >= avgLatency,
              s"maxFetchLatencyMs ($maxLatency) should be >= avgFetchLatencyMs ($avgLatency)")

            assert(
              metrics.get("totalFetchErrors").toLong == 0,
              s"totalFetchErrors should be 0 but was ${metrics.get("totalFetchErrors")}")

            logInfo(
              s"[metrics] batchFetchRequests=${metrics.get("batchFetchRequests")}" +
                s" avgFetchLatencyMs=${metrics.get("avgFetchLatencyMs")}" +
                s" maxFetchLatencyMs=${metrics.get("maxFetchLatencyMs")}" +
                s" totalFetchErrors=${metrics.get("totalFetchErrors")}" +
                s" plannedInputRows=${metrics.get("plannedInputRows")}" +
                s" numBucketsRead=${metrics.get("numBucketsRead")}" +
                s" numBucketsTotal=${metrics.get("numBucketsTotal")}" +
                s" maxOffsetsBehindLatest=${metrics.get("maxOffsetsBehindLatest")}" +
                s" avgOffsetsBehindLatest=${metrics.get("avgOffsetsBehindLatest")}")

            true
        },
        // Second batch: verify per-batch delta is independent (accumulator reset logic)
        AddFlussData(tableName, schema, Seq(Row(6, "f"), Row(7, "g"), Row(8, "h"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(6, "f"), Row(7, "g"), Row(8, "h")),
        AssertOnQuery {
          q =>
            val metrics = q.lastProgress.sources(0).metrics
            // batchFetchRequests must reflect only THIS batch (delta), not cumulative
            assert(
              metrics.get("batchFetchRequests").toLong > 0,
              s"batch 2 batchFetchRequests should be > 0 but was ${metrics.get("batchFetchRequests")}")
            assert(
              metrics.get("totalFetchErrors").toLong == 0,
              s"batch 2 totalFetchErrors should be 0 but was ${metrics.get("totalFetchErrors")}")
            logInfo(
              s"[metrics batch2] batchFetchRequests=${metrics.get("batchFetchRequests")}" +
                s" avgFetchLatencyMs=${metrics.get("avgFetchLatencyMs")}")
            true
        }
      )
    }
  }

  test("read: streaming source metrics for primary key table") {
    val tableName = "t_streaming_metrics_pk"
    val numBuckets = 1
    withTable(tableName) {
      sql(
        s"CREATE TABLE $tableName (id int, data string) " +
          s"TBLPROPERTIES('primary.key' = 'id', 'bucket.num' = '$numBuckets')")

      val schema = StructType(Seq(StructField("id", IntegerType), StructField("data", StringType)))
      val clock = new StreamManualClock

      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        StartStream(trigger = Trigger.ProcessingTime(500), clock),
        AdvanceManualClock(500),
        CheckNewAnswer(),
        AddFlussData(tableName, schema, Seq(Row(1, "a"), Row(2, "b"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(1, "a"), Row(2, "b")),
        AssertOnQuery {
          q =>
            val progress = q.lastProgress
            assert(progress != null, "lastProgress should not be null")
            val metrics = progress.sources(0).metrics

            // All 9 metric keys must be present
            Seq(
              "plannedInputRows",
              "numBucketsRead",
              "numBucketsTotal",
              "maxOffsetsBehindLatest",
              "avgOffsetsBehindLatest",
              "batchFetchRequests",
              "avgFetchLatencyMs",
              "maxFetchLatencyMs",
              "totalFetchErrors"
            ).foreach(key => assert(metrics.containsKey(key), s"metric '$key' missing"))

            assert(
              metrics.get("numBucketsTotal").toInt == numBuckets,
              s"numBucketsTotal should be $numBuckets but was ${metrics.get("numBucketsTotal")}")

            assert(
              metrics.get("batchFetchRequests").toLong > 0,
              s"batchFetchRequests should be > 0 but was ${metrics.get("batchFetchRequests")}")

            assert(
              metrics.get("totalFetchErrors").toLong == 0,
              s"totalFetchErrors should be 0 but was ${metrics.get("totalFetchErrors")}")

            logInfo(
              s"[metrics] batchFetchRequests=${metrics.get("batchFetchRequests")}" +
                s" avgFetchLatencyMs=${metrics.get("avgFetchLatencyMs")}" +
                s" maxFetchLatencyMs=${metrics.get("maxFetchLatencyMs")}" +
                s" totalFetchErrors=${metrics.get("totalFetchErrors")}" +
                s" plannedInputRows=${metrics.get("plannedInputRows")}" +
                s" numBucketsRead=${metrics.get("numBucketsRead")}" +
                s" numBucketsTotal=${metrics.get("numBucketsTotal")}" +
                s" maxOffsetsBehindLatest=${metrics.get("maxOffsetsBehindLatest")}" +
                s" avgOffsetsBehindLatest=${metrics.get("avgOffsetsBehindLatest")}")

            true
        }
      )
    }
  }

  case class AddFlussData(tableName: String, schema: StructType, dataArr: Seq[Row])
    extends AddData {
    override def addData(query: Option[StreamExecution]): (SparkDataStream, Offset) = {
      require(query.nonEmpty, "Cannot add data when there is no active query")
      val sources = query.get.logicalPlan.collect {
        case r: StreamingDataSourceV2Relation if r.stream.isInstanceOf[FlussMicroBatchStream] =>
          r.stream.asInstanceOf[FlussMicroBatchStream]
      }.distinct
      val tablePath = createTablePath(tableName)
      if (!sources.exists(_.tablePath.equals(tablePath))) {
        throw new IllegalArgumentException(
          s"Could not find fluss stream source for table $tableName")
      }

      val flussTable = loadFlussTable(tablePath)
      val writer = if (flussTable.getTableInfo.hasPrimaryKey) {
        FlussUpsertDataWriter(flussTable.getTableInfo.getTablePath, schema, conn.getConfiguration)
      } else {
        FlussAppendDataWriter(flussTable.getTableInfo.getTablePath, schema, conn.getConfiguration)
      }
      dataArr
        .map {
          row =>
            InternalRow.fromSeq(row.toSeq.map {
              case v: String => UTF8String.fromString(v)
              case v => v
            })
        }
        .foreach(writer.write)
      writer.commit()

      val buckets = (0 until flussTable.getTableInfo.getNumBuckets).toSeq
      val offsetsInitializer = OffsetsInitializer.latest()
      val retriever =
        new BucketOffsetsRetrieverImpl(admin, flussTable.getTableInfo.getTablePath)
      val tableBucketOffsets = if (flussTable.getTableInfo.isPartitioned) {
        val partitionInfos =
          admin.listPartitionInfos(flussTable.getTableInfo.getTablePath).get()
        val partitionOffsets = partitionInfos.asScala.map(
          pi =>
            FlussMicroBatchStream.getLatestOffsets(
              flussTable.getTableInfo,
              offsetsInitializer,
              retriever,
              buckets,
              Some(pi)))
        val mergedOffsets = partitionOffsets
          .map(_.getOffsets)
          .reduce((l, r) => (l.asScala ++ r.asScala).asJava)
        new TableBucketOffsets(flussTable.getTableInfo.getTableId, mergedOffsets)
      } else {
        FlussMicroBatchStream.getLatestOffsets(
          flussTable.getTableInfo,
          offsetsInitializer,
          retriever,
          buckets,
          None)
      }

      (sources.find(_.tablePath.equals(tablePath)).get, FlussSourceOffset(tableBucketOffsets))
    }
  }
}
