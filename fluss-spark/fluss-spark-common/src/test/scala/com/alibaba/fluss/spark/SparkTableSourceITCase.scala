/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.spark

import com.alibaba.fluss.client.admin.Admin
import com.alibaba.fluss.client.metadata.KvSnapshots
import com.alibaba.fluss.config.ConfigOptions
import com.alibaba.fluss.metadata.TablePath
import com.alibaba.fluss.row.InternalRow
import com.alibaba.fluss.spark.FlussSparkTestBase.{row, rowWithPartition, FLUSS_CLUSTER_EXTENSION}
import com.alibaba.fluss.testutils.common.CommonTestUtils.waitUtil

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.fluss.MockedSystemClock
import org.apache.spark.sql.streaming.{StreamingQuery, StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.junit.jupiter.api.Assertions
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import java.{util => ju}
import java.time.{Duration, LocalDate}

import scala.collection.JavaConverters.seqAsJavaListConverter
class SparkTableSourceITCase extends FlussSparkTestBase with StreamTest {

  private var bootstrapServers: String = _

  test("Fluss Source: Test NonPkTable Read") {
    withTempDir {
      checkpointDir =>
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))
        spark.sql("create table non_pk_table_test (a int, b String) using fluss")
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, "non_pk_table_test")

        val rows: ju.List[InternalRow] = ju.Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"))

        // write records
        writeRows(tablePath, rows, true)

        val reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", "non_pk_table_test")
          .load()
        val query: StreamingQuery = reader.writeStream
          .format("memory")
          .queryName("non_pk_table_test")
          .outputMode("append")
          .start()
        try {
          query.processAllAvailable()
          val df: Dataset[Row] = spark.sql("select * from non_pk_table_test")
          val result: Array[Row] = df.collect()
          Assertions.assertEquals(3, result.length)
        } finally {
          query.stop()
        }
    }
  }

  test("Fluss Source: Test PkTable ReadOnlySnapshot") {
    withTempDir {
      checkpointDir =>
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))
        spark.sql(
          "create table read_snapshot_test (a int , b String)  OPTIONS ( 'primary.key' = 'a')")
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, "read_snapshot_test")

        val rows: ju.List[InternalRow] = ju.Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"))

        // write records// write records
        writeRows(tablePath, rows, false)

        waitUtilAllBucketFinishSnapshot(admin, tablePath)

        val reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", "read_snapshot_test")
          .load()
        val query: StreamingQuery = reader.writeStream
          .format("memory")
          .queryName("read_snapshot_test")
          .outputMode("append")
          .start()
        try {
          query.processAllAvailable()
          val df: Dataset[Row] = spark.sql("select * from read_snapshot_test")
          val expectedRows = Seq(Row(1, "v1"), Row(2, "v2"), Row(3, "v3"))
          checkAnswer(df, expectedRows)
        } finally {
          query.stop()
        }
    }
  }

  logFormats.foreach {
    logFormat =>
      test(s"Fluss Source: Test AppendTable ProjectPushDown $logFormat") {
        withTempDir {
          checkpointDir =>
            bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

            val tableName = s"append_table_project_push_down_$logFormat"

            spark.sql(
              s"""
                 |create table $tableName (
                 |a int,
                 |b String,
                 |c bigint,
                 |d int,
                 |e int,
                 |f bigint)
                 |using fluss
                 |options ('table.log.format'='$logFormat')
         """.stripMargin
            )

            val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

            val rows: ju.List[InternalRow] = ju.Arrays.asList(
              row(1, "v1", 100L, 1000, 100, 1000L),
              row(2, "v2", 200L, 2000, 200, 2000L),
              row(3, "v3", 300L, 3000, 300, 3000L),
              row(4, "v4", 400L, 4000, 400, 4000L),
              row(5, "v5", 500L, 5000, 500, 5000L),
              row(6, "v6", 600L, 6000, 600, 6000L),
              row(7, "v7", 700L, 7000, 700, 7000L),
              row(8, "v8", 800L, 8000, 800, 8000L),
              row(9, "v9", 900L, 9000, 900, 9000L),
              row(10, "v10", 1000L, 10000, 1000, 10000L)
            )

            writeRows(tablePath, rows, true)

            val queryStr = s"select b, d, c from $tableName"

            val expectedRows: Seq[Row] = Seq(
              Row("v1", 1000, 100),
              Row("v2", 2000, 200),
              Row("v3", 3000, 300),
              Row("v4", 4000, 400),
              Row("v5", 5000, 500),
              Row("v6", 6000, 600),
              Row("v7", 7000, 700),
              Row("v8", 8000, 800),
              Row("v9", 9000, 900),
              Row("v10", 10000, 1000)
            )

            val reader = spark.readStream
              .format("fluss")
              .option("bootstrap.servers", bootstrapServers)
              .option("maxOffsetsPerTrigger", 5)
              .option("database", DEFAULT_DB)
              .option("table", tableName)
              .load()

            val query: StreamingQuery = reader.writeStream
              .format("memory")
              .queryName(tableName)
              .outputMode("append")
              .start()

            try {
              query.processAllAvailable()

              val df: Dataset[Row] = spark.sql(queryStr)

              checkAnswer(df, expectedRows)
            } finally {
              query.stop()
            }
        }

      }
  }

  modes.foreach {
    mode =>
      test(s"Fluss Source: Test Table Project Push Down $mode") {
        withTempDir {
          checkpointDir =>
            bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

            val isPkTable = mode.startsWith("PK")
            val testPkLog = mode.equals("PK_LOG")
            val tableName = s"table_$mode"

            val pkDDL = if (isPkTable) ", 'primary.key' = 'a'" else ""

            spark.sql(
              s"""
                 |CREATE TABLE $tableName (
                 |a INT,
                 |b STRING,
                 |c BIGINT,
                 |d INT )
                 |USING FLUSS
                 |OPTIONS ('table.log.format'='ARROW' $pkDDL)
           """.stripMargin
            )

            val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

            val rows: ju.List[InternalRow] = ju.Arrays.asList(
              row(1, "v1", 100L, 1000),
              row(2, "v2", 200L, 2000),
              row(3, "v3", 300L, 3000),
              row(4, "v4", 400L, 4000),
              row(5, "v5", 500L, 5000),
              row(6, "v6", 600L, 6000),
              row(7, "v7", 700L, 7000),
              row(8, "v8", 800L, 8000),
              row(9, "v9", 900L, 9000),
              row(10, "v10", 1000L, 10000)
            )

            if (isPkTable) {
              if (!testPkLog) {
                // write records and wait snapshot before collect job start,
                // to make sure reading from kv snapshot
                writeRows(tablePath, rows, false)
                waitUtilAllBucketFinishSnapshot(admin, TablePath.of(DEFAULT_DB, tableName))
              }
            } else {
              writeRows(tablePath, rows, true)
            }

            val queryStr = s"SELECT b, a, c FROM $tableName"

            val expectedRows: Seq[Row] = Seq(
              Row("v1", 1, 100),
              Row("v2", 2, 200),
              Row("v3", 3, 300),
              Row("v4", 4, 400),
              Row("v5", 5, 500),
              Row("v6", 6, 600),
              Row("v7", 7, 700),
              Row("v8", 8, 800),
              Row("v9", 9, 900),
              Row("v10", 10, 1000)
            )

            val reader = spark.readStream
              .format("fluss")
              .option("bootstrap.servers", bootstrapServers)
              .option("maxOffsetsPerTrigger", 5)
              .option("database", DEFAULT_DB)
              .option("table", tableName)
              .load()

            val query: StreamingQuery = reader.writeStream
              .format("memory")
              .queryName(tableName)
              .outputMode("append")
              .start()
            if (testPkLog) {
              // delay the write after collect job start,
              // to make sure reading from log instead of snapshot
              writeRows(tablePath, rows, false)
            }
            try {
              query.processAllAvailable()

              val df: Dataset[Row] = spark.sql(queryStr)

              checkAnswer(df, expectedRows)
            } finally {
              query.stop()
            }

        }
      }
  }

  // -------------------------------------------------------------------------------------
  // Fluss scan start mode tests
  // -------------------------------------------------------------------------------------

  isPartition.foreach {
    isPartitioned =>
      test(s"Fluss Source: Test Read Log Table With Different Scan Startup Mode $isPartitioned") {
        withTempDir {
          checkpointDir =>
            bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

            val tableFullName =
              s"fluss.$DEFAULT_DB.tab1_${if (isPartitioned) "partitioned" else "non_partitioned"}"
            val tableName = s"tab1_${if (isPartitioned) "partitioned" else "non_partitioned"}"
            var partitionName: Option[String] = None

            val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

            if (!isPartitioned) {
              spark.sql(
                s"""
                   |CREATE TABLE $tableFullName (
                   |a INT,
                   |b STRING,
                   |c BIGINT,
                   |d INT)
                   |USING FLUSS
             """.stripMargin
              )
            } else {
              spark.sql(
                s"""
                   |CREATE TABLE $tableFullName (
                   |a INT,
                   |b STRING,
                   |c BIGINT,
                   |d INT,
                   |p STRING )
                   |PARTITIONED BY (p)
                   |OPTIONS(
                   |'table.auto-partition.enabled' = 'true',
                   |'table.auto-partition.time-unit' = 'YEAR',
                   |'table.auto-partition.num-precreate' = '1')
             """.stripMargin
              )
              val partitionNameById =
                waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient, tablePath, 1)
              partitionName = Some(partitionNameById.values.iterator.next())
            }

            val rows1: ju.List[InternalRow] = ju.Arrays.asList(
              rowWithPartition(Array(1, "v1", 100L, 1000), partitionName.orNull),
              rowWithPartition(Array(2, "v2", 200L, 2000), partitionName.orNull),
              rowWithPartition(Array(3, "v3", 300L, 3000), partitionName.orNull),
              rowWithPartition(Array(4, "v4", 400L, 4000), partitionName.orNull),
              rowWithPartition(Array(5, "v5", 500L, 5000), partitionName.orNull)
            )

            writeRows(tablePath, rows1, true)

            val rows2: ju.List[InternalRow] = ju.Arrays.asList(
              rowWithPartition(Array(6, "v6", 600L, 6000), partitionName.orNull),
              rowWithPartition(Array(7, "v7", 700L, 7000), partitionName.orNull),
              rowWithPartition(Array(8, "v8", 800L, 8000), partitionName.orNull),
              rowWithPartition(Array(9, "v9", 900L, 9000), partitionName.orNull),
              rowWithPartition(Array(10, "v10", 1000L, 10000), partitionName.orNull)
            )

            writeRows(tablePath, rows2, true)

            val expectedFull: Seq[Row] = Seq(
              Row(1, "v1", 100L, 1000),
              Row(2, "v2", 200L, 2000),
              Row(3, "v3", 300L, 3000),
              Row(4, "v4", 400L, 4000),
              Row(5, "v5", 500L, 5000),
              Row(6, "v6", 600L, 6000),
              Row(7, "v7", 700L, 7000),
              Row(8, "v8", 800L, 8000),
              Row(9, "v9", 900L, 9000),
              Row(10, "v10", 1000L, 10000)
            )

            // 1. read log table with scan.startup.mode='full'
            val queryStr = s"SELECT a, b, c, d FROM $tableName"
            var reader = spark.readStream
              .format("fluss")
              .option("bootstrap.servers", bootstrapServers)
              .option("maxOffsetsPerTrigger", 5)
              .option("database", DEFAULT_DB)
              .option("table", tableName)
              .option("scan.startup.mode", "full")
              .load()

            var query: StreamingQuery = reader.writeStream
              .format("memory")
              .queryName(tableName)
              .outputMode("append")
              .start()

            try {
              query.processAllAvailable()

              val df: Dataset[Row] = spark.sql(queryStr)

              checkAnswer(df, expectedFull)
            } finally {
              query.stop()
            }

            // 2. read log table with scan.startup.mode='earliest'
            reader = spark.readStream
              .format("fluss")
              .option("bootstrap.servers", bootstrapServers)
              .option("maxOffsetsPerTrigger", 5)
              .option("database", DEFAULT_DB)
              .option("table", tableName)
              .option("scan.startup.mode", "earliest")
              .load()
            query = reader.writeStream
              .format("memory")
              .queryName(tableName)
              .outputMode("append")
              .start()
            try {
              query.processAllAvailable()

              val df: Dataset[Row] = spark.sql(queryStr)

              checkAnswer(df, expectedFull)
            } finally {
              query.stop()
            }

            // 3. read log table with scan.startup.mode='timestamp'
            reader = spark.readStream
              .format("fluss")
              .option("bootstrap.servers", bootstrapServers)
              .option("maxOffsetsPerTrigger", 5)
              .option("database", DEFAULT_DB)
              .option("table", tableName)
              .option("scan.startup.mode", "timestamp")
              .option("scan.startup.timestamp", "1000")
              .load()
            query = reader.writeStream
              .format("memory")
              .queryName(tableName)
              .outputMode("append")
              .start()
            try {
              query.processAllAvailable()

              val df: Dataset[Row] = spark.sql(queryStr)

              checkAnswer(df, expectedFull)
            } finally {
              query.stop()
            }
        }
      }

  }

  test("Fluss Source: Test Read Kv Table With Scan Startup Mode Equals Full") {
    withTempDir {
      checkpointDir =>
        import testImplicits._
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

        val tableName = "read_full_test"
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

        spark.sql(
          s"""
             |CREATE TABLE $tableName (
             |a INT ,
             |b STRING)
             |USING FLUSS
             |OPTIONS ('primary.key' = 'a')
         """.stripMargin
        )

        val rows1: ju.List[InternalRow] = ju.Arrays.asList(
          row(1, "v1"),
          row(2, "v2"),
          row(3, "v3"),
          row(3, "v33")
        )

        writeRows(tablePath, rows1, false)
        waitUtilAllBucketFinishSnapshot(admin, tablePath)

        val rows2: ju.List[InternalRow] = ju.Arrays.asList(
          row(1, "v11"),
          row(2, "v22"),
          row(4, "v4")
        )

        val reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", tableName)
          .option("scan.startup.mode", "full")
          .load()
          .select($"a".as[Int], $"b".as[String])

        val query: StreamingQuery = reader.writeStream
          .format("memory")
          .outputMode("append")
          .foreachBatch {
            (ds: Dataset[(Int, String)], epochId: Long) =>
              if (epochId == 0) {
                // Send more message before the tasks of the current batch start reading the current batch
                // data
                writeRows(tablePath, rows2, false)
                val expected = Array(
                  (1, "v1"),
                  (2, "v2"),
                  (3, "v33")
                )
                checkDatasetUnorderly(ds, expected: _*)
              } else {
                val expected = Seq(
                  (1, "v1"),
                  (1, "v11"),
                  (2, "v2"),
                  (2, "v22"),
                  (4, "v4")
                )
                checkDatasetUnorderly(ds, expected: _*)
              }

          }
          .start()

        try {
          query.processAllAvailable()

        } finally {
          query.stop()
        }
    }
  }

  isPartition.foreach {
    isPartitioned =>
      StartupModes.foreach {
        mode =>
          {
            test(
              s"Fluss Source: Test Read Kv Table With With ScanStartupModel $mode and isPartitioned $isPartitioned") {
              withTempDir {
                checkpointDir =>
                  bootstrapServers =
                    String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))
                  val tableName =
                    s"${mode}_test_${if (isPartitioned) "partitioned" else "non_partitioned"}"
                  val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)
                  var partitionName: Option[String] = None

                  if (!isPartitioned) {
                    spark.sql(
                      s"create table $tableName (a int not null, b string) OPTIONS ( 'primary.key' = 'a')")
                  } else {
                    spark.sql(s"""
                                 |create table $tableName (
                                 |a int not null,
                                 |b string,
                                 |c string
                                 |) PARTITIONED BY(c)
                                 |OPTIONS (
                                 |'primary.key' = 'a,c',
                                 |'table.auto-partition.enabled'='true',
                                 |'table.auto-partition.time-unit'='year',
                                 |'table.auto-partition.num-precreate'='1'
                                 |)
                 """.stripMargin)
                    val partitionNamesById =
                      waitUntilPartitions(
                        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(),
                        tablePath,
                        1)
                    partitionName = Some(partitionNamesById.values.iterator.next())
                  }

                  val rows1: ju.List[InternalRow] = ju.Arrays.asList(
                    rowWithPartition(Array(1, "v1"), partitionName.orNull),
                    rowWithPartition(Array(2, "v2"), partitionName.orNull),
                    rowWithPartition(Array(3, "v3"), partitionName.orNull),
                    rowWithPartition(Array(3, "v33"), partitionName.orNull)
                  )

                  writeRows(tablePath, rows1, false)

                  if (partitionName.isEmpty) {
                    waitUtilAllBucketFinishSnapshot(admin, tablePath)
                  } else {
                    waitUtilAllBucketFinishSnapshot(admin, tablePath, Seq(partitionName.get).asJava)
                  }

                  val rows2: ju.List[InternalRow] = ju.Arrays.asList(
                    rowWithPartition(Array(1, "v11"), partitionName.orNull),
                    rowWithPartition(Array(2, "v22"), partitionName.orNull),
                    rowWithPartition(Array(4, "v4"), partitionName.orNull)
                  )

                  writeRows(tablePath, rows2, false)

                  val queryStr = s"SELECT a, b FROM $tableName"

                  val reader = spark.readStream
                    .format("fluss")
                    .option("bootstrap.servers", bootstrapServers)
                    .option("maxOffsetsPerTrigger", 5)
                    .option("database", DEFAULT_DB)
                    .option("table", tableName)
                    .option("scan.startup.mode", mode)
                    .option("scan.startup.timestamp", "1000")
                    .load()

                  val query: StreamingQuery = reader.writeStream
                    .format("memory")
                    .queryName(tableName)
                    .outputMode("append")
                    .trigger(Trigger.ProcessingTime(0))
                    .start()

                  try {
                    query.processAllAvailable()

                    val df: Dataset[Row] = spark.sql(queryStr)

                    val expectedData: Seq[Row] = Seq(
                      Row(1, "v1"),
                      Row(2, "v2"),
                      Row(3, "v3"),
                      Row(3, "v3"),
                      Row(3, "v33"),
                      Row(1, "v1"),
                      Row(1, "v11"),
                      Row(2, "v2"),
                      Row(2, "v22"),
                      Row(4, "v4")
                    )

                    checkAnswer(df, expectedData)

                  } finally {
                    query.stop()
                  }
              }
            }

          }
      }
  }

  isPartition.foreach(
    isAutoPartition => {
      test(
        s"Fluss Source: Test Read PrimaryKey PartitionedTable isAutoPartition:$isAutoPartition") {
        withTempDir {
          checkpointDir =>
            import testImplicits._
            bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))
            val tableName =
              s"read_primary_key_partitioned_table${if (isAutoPartition) "_auto" else ""}"

            // Create table DDL
            val createTableDDL = if (isAutoPartition) {
              s"""
                 |CREATE TABLE $tableName (
                 |a INT NOT NULL,
                 |b STRING,
                 |c STRING)
                 |PARTITIONED BY (c)
                 |OPTIONS (
                 |'primary.key' = 'a,c',
                 |'table.auto-partition.enabled' = 'true', 'table.auto-partition.time-unit' = 'YEAR')
                """.stripMargin
            } else {
              s"""
                 |CREATE TABLE $tableName (
                 |a INT NOT NULL,
                 |b STRING,
                 |c STRING)
                 |PARTITIONED BY (c)
                 |OPTIONS (
                 |'primary.key' = 'a,c')
                """.stripMargin
            }

            spark.sql(createTableDDL)
            val tablePath = TablePath.of(DEFAULT_DB, tableName)

            // Write data into partitions
            val partitionNameById = if (isAutoPartition) {
              waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath)
            } else {
              val currentYear = LocalDate.now().getYear
              spark.sql(s"ALTER TABLE $tableName ADD PARTITION (c = '$currentYear')")
              waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 1)
            }

            var expectedRowValues = writeRowsToPartition(tablePath, partitionNameById.values)
            waitUtilAllBucketFinishSnapshot(admin, tablePath, partitionNameById.values)

            val reader = spark.readStream
              .format("fluss")
              .option("bootstrap.servers", bootstrapServers)
              .option("maxOffsetsPerTrigger", 5)
              .option("database", DEFAULT_DB)
              .option("table", tableName)
              .option("scan.startup.mode", "full")
              .load()
              .select($"a".as[Int], $"b".as[String], $"c".as[String])

            val query: StreamingQuery = reader.writeStream
              .format("memory")
              .outputMode("append")
              .foreachBatch {
                (ds: Dataset[(Int, String, String)], epochId: Long) =>
                  if (epochId == 0) {
                    // Create new partitions and write rows to them
                    spark.sql(s"ALTER TABLE $tableName ADD IF NOT EXISTS PARTITION (c = '2000')")
                    spark.sql(s"ALTER TABLE $tableName ADD IF NOT EXISTS PARTITION (c = '2001')")
                    checkDatasetUnorderly(ds, expectedRowValues: _*)
                    expectedRowValues = writeRowsToPartition(tablePath, Seq("2000", "2001").asJava)

                  } else {
                    checkDatasetUnorderly(ds, expectedRowValues: _*)
                  }

              }
              .start()

            try {
              query.processAllAvailable()

            } finally {
              query.stop()
            }

        }
      }
    })

  test("Fluss Source: Test Read Timestamp Greater Than Max Timestamp") {
    withTempDir {
      checkpointDir =>
        import testImplicits._
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

        spark.sql("create table timestamp_table (a int, b string)")
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, "timestamp_table")

        val rowsFirstBatch: ju.List[InternalRow] =
          ju.Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"))
        writeRows(tablePath, rowsFirstBatch, true)

        Thread.sleep(100)
        val currentTimeMillis = System.currentTimeMillis()

        var reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", "timestamp_table")
          .option("scan.startup.mode", "timestamp")
          .option("scan.startup.timestamp", currentTimeMillis + Duration.ofMinutes(5).toMillis)
          .load()

        var query: StreamingQuery = reader.writeStream
          .format("memory")
          .queryName("timestamp_table")
          .outputMode("append")
          .start()

        try {

          assert(
            intercept[Exception] {
              query.processAllAvailable()
            }.getCause.getCause.getMessage.contains(
              s"the fetch timestamp ${currentTimeMillis + Duration.ofMinutes(5).toMillis} is larger than the current timestamp"
            ))

        } finally {
          query.stop()
        }

        val reader1 = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", "timestamp_table")
          .option("scan.startup.mode", "timestamp")
          .option("scan.startup.timestamp", currentTimeMillis)
          .load()
          .select($"a".as[Int], $"b".as[String])

        val query1 = reader1.writeStream
          .format("memory")
          .outputMode("append")
          .foreachBatch {
            (ds: Dataset[(Int, String)], epochId: Long) =>
              if (epochId == 0) {
                val rowsSecondBatch: ju.List[InternalRow] =
                  ju.Arrays.asList(row(4, "v4"), row(5, "v5"), row(6, "v6"))
                writeRows(tablePath, rowsSecondBatch, true)
              } else {
                val expected = Array(
                  (4, "v4"),
                  (5, "v5"),
                  (6, "v6")
                )
                checkDatasetUnorderly(ds, expected: _*)
              }

          }
          .start()

        try {
          query1.processAllAvailable()

        } finally {
          query1.stop()
        }

    }

  }

  test("Fluss Source: Test Log Table minOffsetsPerTrigger") {
    withTempDir {
      checkpointDir =>
        import testImplicits._
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

        val tableName = "LogMinOffsetsPerTrigger"
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

        spark.sql(
          s"""
             |CREATE TABLE $tableName (
             |a INT )
             |USING FLUSS

         """.stripMargin
        )

        val rows1: ju.List[InternalRow] = ju.Arrays.asList(
          row(1),
          row(2),
          row(3),
          row(4),
          row(5)
        )

        writeRows(tablePath, rows1, true)

        val reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("min.offset.per.trigger", 5)
          .option("max.trigger.delay", "5s")
          .option("database", DEFAULT_DB)
          .option("table", tableName)
          .option("scan.startup.mode", "full")
          .load()
          .select($"a".as[Int])
        val clock = new StreamManualClock

        testStream(reader)(
          StartStream(Trigger.ProcessingTime(100), clock),
          waitUntilBatchProcessed(clock),
          CheckAnswer(1, 2, 3, 4, 5),
          // Adding more data but less than minOffsetsPerTrigger
          Assert {
            writeRows(
              tablePath,
              ju.Arrays.asList(
                row(6),
                row(7),
                row(8)
              ),
              true)
            true
          },
          // No data is processed for next batch as data is less than minOffsetsPerTrigger
          // and maxTriggerDelay is not expired
          AdvanceManualClock(100),
          waitUntilBatchProcessed(clock),
          CheckNewAnswer(),
          Assert {
            writeRows(
              tablePath,
              ju.Arrays.asList(
                row(9),
                row(10),
                row(11)
              ),
              true)
            true
          },
          AdvanceManualClock(100),
          waitUntilBatchProcessed(clock),
          // Running batch now as number of records is greater than minOffsetsPerTrigger
          CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
          // Testing maxTriggerDelay
          // Adding more data but less than minOffsetsPerTrigger
          Assert {
            writeRows(
              tablePath,
              ju.Arrays.asList(
                row(12),
                row(13),
                row(14)
              ),
              true)
            true
          },
          // No data is processed for next batch till maxTriggerDelay is expired
          AdvanceManualClock(100),
          waitUntilBatchProcessed(clock),
          CheckNewAnswer(),
          // Sleeping for 5s to let maxTriggerDelay expire
          Assert {
            Thread.sleep(5 * 1000)
            true
          },
          AdvanceManualClock(100),
          // Running batch as maxTriggerDelay is expired
          waitUntilBatchProcessed(clock),
          CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
        )
        // When Trigger.Once() is used, the read limit should be ignored
        // NOTE: the test uses the deprecated Trigger.Once() by intention, do not change.
        val allData = (1 to 14)
        withTempDir {
          dir =>
            testStream(reader)(
              StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
              AssertOnQuery {
                q =>
                  q.processAllAvailable()
                  true
              },
              CheckAnswer(allData: _*),
              StopStream,
              Assert {
                writeRows(
                  tablePath,
                  ju.Arrays.asList(
                    row(15),
                    row(16),
                    row(17)
                  ),
                  true)
                true
              },
              StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
              AssertOnQuery {
                q =>
                  q.processAllAvailable()
                  true
              },
              CheckAnswer((allData ++ 15.to(17)): _*)
            )
        }
    }
  }

  test("Fluss Source: Test KV Table minOffsetsPerTrigger") {
    withTempDir {
      checkpointDir =>
        import testImplicits._
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

        val tableName = "KVMinOffsetsPerTrigger"
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

        spark.sql(
          s"""
             |CREATE TABLE $tableName (
             |a INT )
             |USING FLUSS
             |OPTIONS ('primary.key' = 'a')
         """.stripMargin
        )

        val rows1: ju.List[InternalRow] = ju.Arrays.asList(
          row(1),
          row(2),
          row(3),
          row(4),
          row(5)
        )

        writeRows(tablePath, rows1, false)
        waitUtilAllBucketFinishSnapshot(admin, tablePath)

        val reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("min.offset.per.trigger", 5)
          .option("max.trigger.delay", "5s")
          .option("database", DEFAULT_DB)
          .option("table", tableName)
          .option("scan.startup.mode", "full")
          .load()
          .select($"a".as[Int])
        val clock = new StreamManualClock

        testStream(reader)(
          StartStream(Trigger.ProcessingTime(100), clock),
          waitUntilBatchProcessed(clock),
          CheckAnswer(1, 2, 3, 4, 5),
          // Adding more data but less than minOffsetsPerTrigger
          Assert {
            writeRows(
              tablePath,
              ju.Arrays.asList(
                row(6),
                row(7),
                row(8)
              ),
              false)
            true
          },
          // No data is processed for next batch as data is less than minOffsetsPerTrigger
          // and maxTriggerDelay is not expired
          AdvanceManualClock(100),
          waitUntilBatchProcessed(clock),
          CheckNewAnswer(),
          Assert {
            writeRows(
              tablePath,
              ju.Arrays.asList(
                row(9),
                row(10),
                row(11)
              ),
              false)
            true
          },
          AdvanceManualClock(100),
          waitUntilBatchProcessed(clock),
          // Running batch now as number of records is greater than minOffsetsPerTrigger
          CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
          // Testing maxTriggerDelay
          // Adding more data but less than minOffsetsPerTrigger
          Assert {
            writeRows(
              tablePath,
              ju.Arrays.asList(
                row(12),
                row(13),
                row(14)
              ),
              false)
            true
          },
          // No data is processed for next batch till maxTriggerDelay is expired
          AdvanceManualClock(100),
          waitUntilBatchProcessed(clock),
          CheckNewAnswer(),
          // Sleeping for 5s to let maxTriggerDelay expire
          Assert {
            Thread.sleep(5 * 1000)
            true
          },
          AdvanceManualClock(100),
          // Running batch as maxTriggerDelay is expired
          waitUntilBatchProcessed(clock),
          CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
        )
        // When Trigger.Once() is used, the read limit should be ignored
        // NOTE: the test uses the deprecated Trigger.Once() by intention, do not change.
        val allData = (1 to 14)
        withTempDir {
          dir =>
            testStream(reader)(
              StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
              AssertOnQuery {
                q =>
                  q.processAllAvailable()
                  true
              },
              CheckAnswer(allData: _*),
              StopStream,
              Assert {
                writeRows(
                  tablePath,
                  ju.Arrays.asList(
                    row(15),
                    row(16),
                    row(17)
                  ),
                  false)
                true
              },
              StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
              AssertOnQuery {
                q =>
                  q.processAllAvailable()
                  true
              },
              CheckAnswer((allData ++ 15.to(17)): _*)
            )
        }
    }
  }

  test("Fluss Source: Test Log Table maxOffsetsPerTrigger") {
    withTempDir {
      checkpointDir =>
        import testImplicits._
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

        val tableName = "LogMaxOffsetsPerTrigger"
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

        spark.sql(
          s"""
             |CREATE TABLE $tableName (
             |a INT )
             |USING FLUSS

         """.stripMargin
        )

        val rows1: ju.List[InternalRow] = ju.Arrays.asList(
          row(1),
          row(2),
          row(3),
          row(4),
          row(5),
          row(6),
          row(7),
          row(8)
        )

        writeRows(tablePath, rows1, true)

        val reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("max.offset.per.trigger", 3)
          .option("database", DEFAULT_DB)
          .option("table", tableName)
          .option("scan.startup.mode", "full")
          .load()
          .select($"a".as[Int])
        val clock = new StreamManualClock

        testStream(reader)(
          StartStream(Trigger.ProcessingTime(100), clock),
          waitUntilBatchProcessed(clock),
          CheckAnswer(1, 2, 3),
          AdvanceManualClock(100),
          waitUntilBatchProcessed(clock),
          CheckAnswer(1, 2, 3, 4, 5, 6),
          AdvanceManualClock(100),
          waitUntilBatchProcessed(clock),
          // Running batch now as number of records is greater than minOffsetsPerTrigger
          CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8)
        )
        // When Trigger.Once() is used, the read limit should be ignored
        // NOTE: the test uses the deprecated Trigger.Once() by intention, do not change.
        val allData = (1 to 8)
        withTempDir {
          dir =>
            testStream(reader)(
              StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
              AssertOnQuery {
                q =>
                  q.processAllAvailable()
                  true
              },
              CheckAnswer(allData: _*),
              StopStream,
              Assert {
                writeRows(
                  tablePath,
                  ju.Arrays.asList(
                    row(9),
                    row(10),
                    row(11)
                  ),
                  true)
                true
              },
              StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
              AssertOnQuery {
                q =>
                  q.processAllAvailable()
                  true
              },
              CheckAnswer((allData ++ 9.to(11)): _*)
            )
        }
    }
  }

  test("Fluss Source: Test Log Table compositeReadLimit") {
    withTempDir {
      checkpointDir =>
        import testImplicits._
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

        val tableName = "LogCompositeReadLimit"
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

        spark.sql(
          s"""
             |CREATE TABLE $tableName (
             |a INT )
             |USING FLUSS

         """.stripMargin
        )

        val rows1: ju.List[InternalRow] = ju.Arrays.asList(
          row(1),
          row(2),
          row(3),
          row(4),
          row(5),
          row(6),
          row(7),
          row(8)
        )

        writeRows(tablePath, rows1, true)

        val reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("min.offset.per.trigger", 3)
          .option("max.offset.per.trigger", 6)
          .option("max.trigger.delay", "5s")
          .option("database", DEFAULT_DB)
          .option("table", tableName)
          .option("scan.startup.mode", "full")
          // mock system time to ensure deterministic behavior
          // in determining if maxOffsetsPerTrigger is satisfied
          .option("_mockSystemTime", "")
          .load()
          .select($"a".as[Int])
        val clock = new StreamManualClock
        val manualClock = MockedSystemClock.manualClock
        def advanceSystemClock(mills: Long): ExternalAction = () => {
          manualClock.advanceTime(Duration.ofMillis(mills))
        }

        testStream(reader)(
          StartStream(Trigger.ProcessingTime(100), clock),
          waitUntilBatchProcessed(clock),
          // First Batch is always processed but it will process only 6
          CheckAnswer(1, 2, 3, 4, 5, 6),
          // Pending data is less than minOffsetsPerTrigger
          // No data is processed for next batch as data is less than minOffsetsPerTrigger
          // and maxTriggerDelay is not expired
          AdvanceManualClock(100),
          advanceSystemClock(100),
          waitUntilBatchProcessed(clock),
          CheckNewAnswer(),
          // Adding more data but less than minOffsetsPerTrigger
          Assert {
            writeRows(
              tablePath,
              ju.Arrays.asList(
                row(9),
                row(10),
                row(11),
                row(12),
                row(13)
              ),
              true)
            true
          },
          AdvanceManualClock(100),
          advanceSystemClock(100),
          waitUntilBatchProcessed(clock),
          // Running batch now as number of new records is greater than minOffsetsPerTrigger
          // but reading limited data as per maxOffsetsPerTrigger

          CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
          // Testing maxTriggerDelay
          // No data is processed for next batch till maxTriggerDelay is expired
          AdvanceManualClock(100),
          advanceSystemClock(100),
          waitUntilBatchProcessed(clock),
          CheckNewAnswer(),
          AdvanceManualClock(100),
          advanceSystemClock(5000),
          // Running batch as maxTriggerDelay is expired
          waitUntilBatchProcessed(clock),
          CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
        )
        // When Trigger.Once() is used, the read limit should be ignored
        // NOTE: the test uses the deprecated Trigger.Once() by intention, do not change.
        val allData = (1 to 13)
        withTempDir {
          dir =>
            testStream(reader)(
              StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
              AssertOnQuery {
                q =>
                  q.processAllAvailable()
                  true
              },
              CheckAnswer(allData: _*),
              StopStream,
              Assert {
                writeRows(
                  tablePath,
                  ju.Arrays.asList(
                    row(14),
                    row(15),
                    row(16),
                    row(17)
                  ),
                  true)
                true
              },
              StartStream(Trigger.Once(), checkpointLocation = dir.getCanonicalPath),
              AssertOnQuery {
                q =>
                  q.processAllAvailable()
                  true
              },
              CheckAnswer((allData ++ 14.to(17)): _*)
            )
        }
    }
  }

  private def waitUtilAllBucketFinishSnapshot(admin: Admin, tablePath: TablePath): Unit = {
    waitUtil(
      () => {
        import scala.collection.JavaConversions._
        val snapshots = admin.getLatestKvSnapshots(tablePath).get

        val allBucketsHaveSnapshots = snapshots.getBucketIds.forall {
          bucketId => snapshots.getSnapshotId(bucketId).isPresent
        }

        allBucketsHaveSnapshots
      },
      Duration.ofMinutes(1),
      "Fail to wait until all bucket finish snapshot"
    )
  }

  private def waitUtilAllBucketFinishSnapshot(
      admin: Admin,
      tablePath: TablePath,
      partitions: ju.Collection[String]): Unit = {
    waitUtil(
      () => {
        import scala.collection.JavaConversions._

        var allBucketsHaveSnapshots = true
        for (partition <- partitions) {
          val snapshots: KvSnapshots = admin.getLatestKvSnapshots(tablePath, partition).get
          import scala.collection.JavaConversions._
          for (bucketId <- snapshots.getBucketIds) {
            if (!snapshots.getSnapshotId(bucketId).isPresent) {
              allBucketsHaveSnapshots = false
            }
          }
        }
        allBucketsHaveSnapshots

      },
      Duration.ofMinutes(1),
      "Fail to wait util all bucket finish snapshot"
    )
  }

  private def waitUntilBatchProcessed(clock: StreamManualClock) = AssertOnQuery {
    q =>
      eventually(Timeout(streamingTimeout)) {
        if (!q.exception.isDefined) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
  }
}
