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

import org.apache.fluss.client.admin.OffsetSpec
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.{ConfigOptions, Configuration, MemorySize}
import org.apache.fluss.exception.FetchException
import org.apache.fluss.metadata.{TableBucket, TablePath}
import org.apache.fluss.predicate.PredicateBuilder
import org.apache.fluss.server.log.LogTablet
import org.apache.fluss.spark.read.{AppendPlanner, FlussAppendBatch, FlussAppendInputPartition, FlussAppendMicroBatchStream, FlussAppendPartitionReader}
import org.apache.fluss.spark.read.lake.FlussLakePartitionReaderFactory
import org.apache.fluss.testutils.common.CommonTestUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}

import java.time.Duration
import java.util.{Collections, HashMap, Optional}

import scala.collection.mutable.ArrayBuffer

/** Spark SQL regressions for log buckets emptied by retention. */
class SparkLogTableRetentionReadTest extends FlussSparkTestBase {

  test("Spark Read: small polls retain every row across batch boundaries") {
    withTable("retained_log") {
      val path = createRetainedLogTable(partitioned = false, logFormat = "INDEXED")
      for (id <- 0 until 6) {
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES ($id, 'current')")
      }
      val config = flussServer.getClientConfig
      config.set(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES, new MemorySize(1))
      config.set(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET, new MemorySize(1))
      config.set(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, Int.box(1))
      val batch = createBatch(path, config, Collections.emptyMap[String, String]())

      assertThat(readIds(batch.createReaderFactory(), batch.planInputPartitions()).toArray)
        .containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5)
    }
  }

  test("Spark Read: fully filtered log batches complete using scanner progress") {
    withTable("retained_log") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.retained_log (id INT, pt STRING)
             |TBLPROPERTIES (
             |  'bucket.num' = '1',
             |  'table.log.format' = 'ARROW',
             |  'table.replication.factor' = '1',
             |  'table.statistics.columns' = 'id')
             |""".stripMargin)
      val path = createTablePath("retained_log")
      sql(
        s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (0, 'current'), (1, 'current'), (2, 'current'), (3, 'current'), (4, 'current')")
      val info = admin.getTableInfo(path).get()
      assertThat(info.isStatisticsEnabled).isTrue()
      val bucket = new TableBucket(info.getTableId, 0)
      val stopOffset = logTabletFor(bucket).getHighWatermark
      assertThat(stopOffset).isEqualTo(5L)
      val predicate = new PredicateBuilder(info.getRowType).greaterThan(0, Int.box(100))
      val table = conn.getTable(path)
      try {
        val scanner = table.newScan().filter(predicate).createLogScanner()
        try {
          scanner.subscribe(0, 0L)
          val records = scanner.poll(Duration.ofSeconds(10))
          assertThat(records.isEmpty).isTrue()
          assertThat(records.consumedUpToOffset(bucket)).isEqualTo(Long.box(stopOffset))
        } finally {
          scanner.close()
        }
      } finally {
        table.close()
      }

      val reader = new FlussAppendPartitionReader(
        path,
        Array(0, 1),
        Some(predicate),
        None,
        FlussAppendInputPartition(bucket, 0L, stopOffset),
        flussServer.getClientConfig)
      try {
        assertThat(reader.next()).isFalse()
      } finally {
        reader.close()
      }
      checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.retained_log WHERE id > 100"), Nil)
      checkAnswer(
        sql(s"SELECT id FROM $DEFAULT_DATABASE.retained_log"),
        (0 until 5).map(id => Row(id)))
    }
  }

  for (partitioned <- Seq(false, true)) {
    test(s"Spark Read: expired log table, partitioned=$partitioned") {
      withTable("retained_log") {
        val partitionClause = if (partitioned) "PARTITIONED BY (pt)" else ""
        sql(s"""
               |CREATE TABLE $DEFAULT_DATABASE.retained_log (id INT, pt STRING)
               |$partitionClause
               |TBLPROPERTIES (
               |  'bucket.num' = '1',
               |  'table.replication.factor' = '1',
               |  'table.log.ttl' = '1 ms')
               |""".stripMargin)
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (1, 'expired')")

        val path = createTablePath("retained_log")
        val tableId = admin.getTableInfo(path).get().getTableId
        val partition = if (partitioned) {
          Some(admin.listPartitionInfos(path).get().get(0))
        } else {
          None
        }
        val bucket = new TableBucket(
          tableId,
          partition.map(p => java.lang.Long.valueOf(p.getPartitionId)).orNull,
          0)
        val logTablet = logTabletFor(bucket)
        val endOffset = logTablet.getHighWatermark
        assertThat(endOffset).isPositive
        logTablet.roll(Optional.empty[java.lang.Long]())
        CommonTestUtils.retry(
          Duration.ofSeconds(10),
          () => {
            logTablet.deleteExpiredSegments()
            assertThat(logTablet.logStartOffset()).isEqualTo(endOffset)
          })

        for (spec <- Seq[OffsetSpec](new OffsetSpec.EarliestSpec(), new OffsetSpec.LatestSpec())) {
          val offsets = partition match {
            case Some(info) =>
              admin.listOffsets(
                path,
                info.getPartitionName,
                Collections.singletonList[Integer](0),
                spec)
            case None => admin.listOffsets(path, Collections.singletonList[Integer](0), spec)
          }
          assertThat(offsets.bucketResult(0).get()).isEqualTo(endOffset)
        }

        checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.retained_log"), Nil)
        checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.retained_log LIMIT 100"), Nil)

        // A new partition also exercises a retained-empty partition alongside a readable one.
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (2, 'current')")
        checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.retained_log"), Row(2, "current") :: Nil)
        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.retained_log LIMIT 100"),
          Row(2, "current") :: Nil)
      }
    }
  }

  for (partitioned <- Seq(false, true)) {
    test(s"Spark Read: all planned log records expire, partitioned=$partitioned") {
      withTable("retained_log") {
        val partitionClause = if (partitioned) "PARTITIONED BY (pt)" else ""
        sql(s"""
               |CREATE TABLE $DEFAULT_DATABASE.retained_log (id INT, pt STRING)
               |$partitionClause
               |TBLPROPERTIES (
               |  'bucket.num' = '1',
               |  'table.replication.factor' = '1',
               |  'table.log.ttl' = '1 ms')
               |""".stripMargin)
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (1, 'expired')")

        val path = createTablePath("retained_log")
        val info = admin.getTableInfo(path).get()
        val config = flussServer.getClientConfig
        val planner = new AppendPlanner(
          path,
          info,
          None,
          None,
          Array(0, 1),
          new CaseInsensitiveStringMap(Collections.emptyMap[String, String]()),
          config)
        val partitions = planner.plan()
        assertThat(partitions.length).isEqualTo(1)
        val partition = partitions.head.asInstanceOf[FlussAppendInputPartition]
        val bucket = partition.tableBucket
        val logTablet = logTabletFor(bucket)
        logTablet.roll(Optional.empty[java.lang.Long]())
        CommonTestUtils.retry(
          Duration.ofSeconds(10),
          () => {
            logTablet.deleteExpiredSegments()
            assertThat(logTablet.logStartOffset()).isEqualTo(partition.stopOffset)
          })

        val reader =
          new FlussAppendPartitionReader(path, Array(0, 1), None, None, partition, config)
        try {
          assertThat(reader.next()).isFalse
        } finally {
          reader.close()
        }
      }
    }
  }

  test("Spark Read: retention advances between planning and task execution") {
    withTable("retained_log") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.retained_log (id INT, pt STRING)
             |TBLPROPERTIES (
             |  'bucket.num' = '1',
             |  'table.replication.factor' = '1',
             |  'table.log.ttl' = '1 ms')
             |""".stripMargin)
      sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (1, 'expired')")

      val path = createTablePath("retained_log")
      val info = admin.getTableInfo(path).get()
      val bucket = new TableBucket(info.getTableId, 0)
      val logTablet = logTabletFor(bucket)
      val retainedStart = logTablet.getHighWatermark
      logTablet.roll(Optional.empty[java.lang.Long]())
      sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (2, 'current')")
      val config = flussServer.getClientConfig
      val planner = new AppendPlanner(
        path,
        info,
        None,
        None,
        Array(0, 1),
        new CaseInsensitiveStringMap(Collections.emptyMap[String, String]()),
        config)
      val partitions = planner.plan()
      assertThat(partitions.length).isEqualTo(1)

      // Advance earliest after the plan is fixed, while leaving a record before its stop offset.
      CommonTestUtils.retry(
        Duration.ofSeconds(10),
        () => {
          logTablet.deleteExpiredSegments()
          assertThat(logTablet.logStartOffset()).isEqualTo(retainedStart)
        })
      val reader = new FlussAppendPartitionReader(
        path,
        Array(0, 1),
        None,
        None,
        partitions.head.asInstanceOf[FlussAppendInputPartition],
        config)
      try {
        assertThat(reader.next()).isTrue
        assertThat(reader.get().getInt(0)).isEqualTo(2)
        assertThat(reader.get().getUTF8String(1).toString).isEqualTo("current")
        assertThat(reader.next()).isFalse
      } finally {
        reader.close()
      }
    }
  }

  for (partitioned <- Seq(false, true); readerPath <- Seq("streaming", "lake")) {
    test(s"Spark Read: $readerPath reads retained-empty logs, partitioned=$partitioned") {
      withTable("retained_log") {
        val path = createRetainedLogTable(partitioned)
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (1, 'expired')")
        val info = admin.getTableInfo(path).get()
        val bucket = if (partitioned) {
          new TableBucket(
            info.getTableId,
            admin.listPartitionInfos(path).get().get(0).getPartitionId,
            0)
        } else {
          new TableBucket(info.getTableId, 0)
        }
        val stopOffset = expireAllRecords(bucket)
        val config = flussServer.getClientConfig

        if (readerPath == "streaming") {
          withTempDir {
            checkpoint =>
              val stream = new FlussAppendMicroBatchStream(
                path,
                info,
                new StructType().add("id", IntegerType).add("pt", StringType),
                new CaseInsensitiveStringMap(
                  Collections.singletonMap(SparkFlussConf.SCAN_START_UP_MODE.key(), "earliest")),
                config,
                checkpoint.getAbsolutePath
              )
              try {
                val start = stream.initialOffset()
                val end = stream.latestOffset(start, ReadLimit.allAvailable())
                val partitions = stream.planInputPartitions(start, end)
                assertThat(partitions.length).isEqualTo(1)
                val partition = partitions.head.asInstanceOf[FlussAppendInputPartition]
                assertThat(partition.startOffset).isEqualTo(LogScanner.EARLIEST_OFFSET)
                assertThat(partition.stopOffset).isEqualTo(stopOffset)
                val reader = stream.createReaderFactory().createReader(partition)
                try {
                  assertThat(reader.next()).isFalse
                } finally {
                  reader.close()
                }
              } finally {
                stream.stop()
              }
          }
        } else {
          // Exercise the lake factory's Fluss log-tail branch without loading a lake source.
          val factory = new FlussLakePartitionReaderFactory(
            Collections.emptyMap[String, String](),
            path,
            Array(0, 1),
            None,
            None,
            None,
            config)
          val reader = factory.createReader(
            FlussAppendInputPartition(bucket, LogScanner.EARLIEST_OFFSET, stopOffset))
          try {
            assertThat(reader.next()).isFalse
          } finally {
            reader.close()
          }
        }
      }
    }
  }

  for (partitioned <- Seq(false, true); readMode <- Seq("split", "incremental")) {
    test(s"Spark Read: $readMode batch completes an expired range partitioned=$partitioned") {
      withTable("retained_log") {
        val path = createRetainedLogTable(partitioned)
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (1, 'expired'), (2, 'expired')")
        val config = flussServer.getClientConfig
        val options = if (readMode == "split") {
          config.setLong(SparkFlussConf.SCAN_MAX_RECORDS_PER_PARTITION, 1L)
          Collections.emptyMap[String, String]()
        } else {
          Collections.singletonMap(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key(), "0")
        }
        val batch = createBatch(path, config, options)
        val partitions = batch.planInputPartitions()
        assertThat(partitions).isNotEmpty
        val partition = partitions.head.asInstanceOf[FlussAppendInputPartition]
        assertThat(partition.startOffset).isGreaterThanOrEqualTo(0L)
        val retainedStart = expireAllRecords(partition.tableBucket)
        assertThat(retainedStart).isGreaterThan(partition.startOffset)

        assertThat(readIds(batch.createReaderFactory(), partitions).toArray).isEmpty()
      }
    }
  }

  for (readerPath <- Seq("streaming", "lake")) {
    test(s"Spark Read: $readerPath preserves an expired numeric range failure") {
      withTable("retained_log") {
        val path = createRetainedLogTable(partitioned = false)
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (1, 'expired'), (2, 'expired')")
        val info = admin.getTableInfo(path).get()
        val bucket = new TableBucket(info.getTableId, 0)
        val stopOffset = expireAllRecords(bucket)
        val partition = FlussAppendInputPartition(bucket, 0L, stopOffset)
        val config = flussServer.getClientConfig
        if (readerPath == "streaming") {
          withTempDir {
            checkpoint =>
              val stream = new FlussAppendMicroBatchStream(
                path,
                info,
                new StructType().add("id", IntegerType).add("pt", StringType),
                new CaseInsensitiveStringMap(Collections.emptyMap[String, String]()),
                config,
                checkpoint.getAbsolutePath
              )
              try {
                assertExpiredNumericRangeFails(stream.createReaderFactory(), partition)
              } finally {
                stream.stop()
              }
          }
        } else {
          val factory = new FlussLakePartitionReaderFactory(
            Collections.emptyMap[String, String](),
            path,
            Array(0, 1),
            None,
            None,
            None,
            config)
          assertExpiredNumericRangeFails(factory, partition)
        }
      }
    }
  }

  for (partitioned <- Seq(false, true); incremental <- Seq(false, true)) {
    test(
      s"Spark Read: split batch keeps retained rows incremental=$incremental partitioned=$partitioned") {
      withTable("retained_log") {
        val path = createRetainedLogTable(partitioned)
        sql(
          s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (0, 'current'), (1, 'current'), (2, 'current')")
        val info = admin.getTableInfo(path).get()
        val bucket = if (partitioned) {
          new TableBucket(
            info.getTableId,
            admin.listPartitionInfos(path).get().get(0).getPartitionId,
            0)
        } else {
          new TableBucket(info.getTableId, 0)
        }
        val logTablet = logTabletFor(bucket)
        logTablet.roll(Optional.empty[java.lang.Long]())
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (3, 'current'), (4, 'current')")
        val retainedTimestamp = logTablet.localMaxTimestamp()
        CommonTestUtils.waitUntil(
          () => System.currentTimeMillis() > retainedTimestamp,
          Duration.ofSeconds(10),
          "The final record must have a later commit timestamp than the retained records"
        )
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (5, 'current')")
        val endTimestamp = logTablet.localMaxTimestamp()
        assertThat(endTimestamp).isGreaterThan(retainedTimestamp)

        val config = flussServer.getClientConfig
        config.setLong(SparkFlussConf.SCAN_MAX_RECORDS_PER_PARTITION, 2L)
        val options = new HashMap[String, String]()
        if (incremental) {
          options.put(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key(), "0")
          options.put(SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key(), endTimestamp.toString)
        }
        val batch = createBatch(path, config, options)
        val partitions = batch.planInputPartitions()
        assertThat(partitions.map(_.asInstanceOf[FlussAppendInputPartition].startOffset))
          .containsExactly(0L, 2L, 4L)
        assertThat(partitions.map(_.asInstanceOf[FlussAppendInputPartition].stopOffset))
          .containsExactly(2L, 4L, 6L)

        CommonTestUtils.retry(
          Duration.ofSeconds(10),
          () => {
            logTablet.deleteExpiredSegments()
            assertThat(logTablet.logStartOffset()).isEqualTo(3L)
          })
        // Writes after planning must not extend any split's original stop offset.
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (6, 'current'), (7, 'current')")
        val expectedIds = if (incremental) Array(3, 4) else Array(3, 4, 5)
        assertThat(readIds(batch.createReaderFactory(), partitions).toArray)
          .containsExactlyInAnyOrder(expectedIds: _*)
      }
    }
  }

  test("Spark Read: a batch resumes after retention advances during reading") {
    withTable("retained_log") {
      val path = createRetainedLogTable(partitioned = false, logFormat = "INDEXED")
      // Separate inserts produce separate batches, so a one-byte fetch cannot prefetch them all.
      for (id <- 0 until 3) {
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES ($id, 'current')")
      }
      val info = admin.getTableInfo(path).get()
      val bucket = new TableBucket(info.getTableId, 0)
      val logTablet = logTabletFor(bucket)
      logTablet.roll(Optional.empty[java.lang.Long]())
      sql(
        s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (3, 'current'), (4, 'current'), (5, 'current')")
      val config = flussServer.getClientConfig
      config.set(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES, new MemorySize(1))
      config.set(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET, new MemorySize(1))
      config.set(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, Int.box(1))
      val batch = createBatch(path, config, Collections.emptyMap[String, String]())
      val partitions = batch.planInputPartitions()
      assertThat(partitions.length).isEqualTo(1)
      val reader = batch.createReaderFactory().createReader(partitions.head)
      try {
        assertThat(reader.next()).isTrue
        val ids = ArrayBuffer(reader.get().getInt(0))
        assertThat(ids.head).isEqualTo(0)
        CommonTestUtils.retry(
          Duration.ofSeconds(10),
          () => {
            logTablet.deleteExpiredSegments()
            assertThat(logTablet.logStartOffset()).isEqualTo(3L)
          })
        while (reader.next()) {
          ids += reader.get().getInt(0)
        }
        // One prefetched batch may retain id=1; id=2 must be skipped after its segment expires.
        assertThat(ids.toArray).doesNotContain(2)
        assertThat(ids.filter(_ >= 3).toArray).containsExactlyInAnyOrder(3, 4, 5)
        assertThat(ids.distinct.size).isEqualTo(ids.size)
      } finally {
        reader.close()
      }
    }
  }

  test("Spark Read: recreated table cannot complete an old range") {
    withTable("retained_log") {
      val path = createRetainedLogTable(partitioned = false)
      sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (1, 'expired')")
      val oldTableId = admin.getTableInfo(path).get().getTableId
      val oldBucket = new TableBucket(oldTableId, 0)
      val stopOffset = logTabletFor(oldBucket).getHighWatermark
      assertThat(stopOffset).isEqualTo(1L)

      sql(s"DROP TABLE $DEFAULT_DATABASE.retained_log")
      createRetainedLogTable(partitioned = false)
      sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (2, 'expired'), (3, 'expired')")
      val newTableId = admin.getTableInfo(path).get().getTableId
      assertThat(newTableId).isNotEqualTo(oldTableId)
      val earliestOffset = expireAllRecords(new TableBucket(newTableId, 0))
      assertThat(earliestOffset).isGreaterThanOrEqualTo(stopOffset)

      val partition =
        FlussAppendInputPartition(oldBucket, LogScanner.EARLIEST_OFFSET, stopOffset)
      assertThatThrownBy(
        () => {
          val reader = new FlussAppendPartitionReader(
            path,
            Array(0, 1),
            None,
            None,
            partition,
            flussServer.getClientConfig)
          try {
            reader.next()
          } finally {
            reader.close()
          }
        }).isInstanceOf(classOf[IllegalStateException]).hasMessageContaining("recreated")
    }
  }

  private def createBatch(
      path: TablePath,
      config: Configuration,
      options: java.util.Map[String, String]): FlussAppendBatch = {
    val info = admin.getTableInfo(path).get()
    val scanOptions = new CaseInsensitiveStringMap(options)
    val planner = new AppendPlanner(path, info, None, None, Array(0, 1), scanOptions, config)
    new FlussAppendBatch(
      path,
      info,
      new StructType().add("id", IntegerType).add("pt", StringType),
      None,
      None,
      scanOptions,
      config,
      planner)
  }

  private def readIds(
      factory: PartitionReaderFactory,
      partitions: Array[InputPartition]): Seq[Int] = {
    val ids = ArrayBuffer.empty[Int]
    partitions.foreach {
      partition =>
        val reader = factory.createReader(partition)
        try {
          while (reader.next()) {
            ids += reader.get().getInt(0)
          }
        } finally {
          reader.close()
        }
    }
    ids.toSeq
  }

  private def assertExpiredNumericRangeFails(
      factory: PartitionReaderFactory,
      partition: FlussAppendInputPartition): Unit = {
    assertThatThrownBy(
      () => {
        val reader = factory.createReader(partition)
        try {
          reader.next()
        } finally {
          reader.close()
        }
      }).isInstanceOf(classOf[FetchException]).hasMessageContaining("out of range")
  }

  private def createRetainedLogTable(
      partitioned: Boolean,
      logFormat: String = "ARROW"): TablePath = {
    val partitionClause = if (partitioned) "PARTITIONED BY (pt)" else ""
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.retained_log (id INT, pt STRING)
           |$partitionClause
           |TBLPROPERTIES (
           |  'bucket.num' = '1',
           |  'table.log.format' = '$logFormat',
           |  'table.replication.factor' = '1',
           |  'table.log.ttl' = '1 ms')
           |""".stripMargin)
    createTablePath("retained_log")
  }

  private def expireAllRecords(bucket: TableBucket): Long = {
    val logTablet = logTabletFor(bucket)
    val stopOffset = logTablet.getHighWatermark
    assertThat(stopOffset).isPositive
    logTablet.roll(Optional.empty[java.lang.Long]())
    CommonTestUtils.retry(
      Duration.ofSeconds(10),
      () => {
        logTablet.deleteExpiredSegments()
        assertThat(logTablet.logStartOffset()).isEqualTo(stopOffset)
      })
    stopOffset
  }

  private def logTabletFor(bucket: TableBucket): LogTablet = {
    val leader = flussServer.getZooKeeperClient.getLeaderAndIsr(bucket).get().leader()
    flussServer
      .getTabletServerById(leader)
      .getReplicaManager
      .getReplicaOrException(bucket)
      .getLogTablet
  }

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO)
    conf.set(ConfigOptions.LOG_RETENTION_CHECK_INTERVAL, Duration.ofDays(1))
    conf
  }
}
