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
import org.apache.fluss.client.table.Table
import org.apache.fluss.spark.read.{FlussMicroBatchStream, FlussSourceOffset}
import org.apache.fluss.spark.write.{FlussAppendDataWriter, FlussUpsertDataWriter}
import org.apache.fluss.utils.json.TableBucketOffsets

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamExecution}
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.File

import scala.collection.JavaConverters._

class SparkStreamingTest extends FlussSparkTestBase with StreamTest {
  import testImplicits._

  /**
   * Will run streaming write twice with same checkpoint dir and verify each write results
   * separately.
   */
  private def runTestWithStream(
      tableIdentifier: String,
      input1: Seq[(Long, String)],
      input2: Seq[(Long, String)],
      expect1: Seq[(String, Long, String)],
      expect2: Seq[(String, Long, String)]): Unit = {
    withTempDir {
      checkpointDir =>
        verifyStream(tableIdentifier, checkpointDir, Seq.empty, input1, expect1)

        verifyStream(tableIdentifier, checkpointDir, Seq(input1), input2, expect2)
    }
  }

  private def verifyStream(
      tableIdentifier: String,
      checkpointDir: File,
      prevInputs: Seq[Seq[(Long, String)]],
      newInputs: Seq[(Long, String)],
      expectedOutputs: Seq[(String, Long, String)]): Unit = {
    val inputData = MemoryStream[(Long, String)]
    val inputDF = inputData.toDF().toDF("id", "data")

    prevInputs.foreach(inputsPerBatch => inputData.addData(inputsPerBatch: _*))

    val query = inputDF.writeStream
      .option("checkpointLocation", checkpointDir.getAbsolutePath)
      .toTable(tableIdentifier)

    inputData.addData(newInputs: _*)

    query.processAllAvailable()
    query.stop()

    // TODO verified from spark read
    val table = loadFlussTable(createTablePath(tableIdentifier))
    val rowsWithType = getRowsWithChangeType(table)
    assert(rowsWithType.length == expectedOutputs.length)

    val row = rowsWithType.head._2
    assert(row.getFieldCount == 2)

    val result = rowsWithType.zip(expectedOutputs).forall {
      case (flussRowWithType, expect) =>
        flussRowWithType._1.equals(expect._1) && flussRowWithType._2.getLong(
          0) == expect._2 && flussRowWithType._2.getString(1).toString == expect._3
    }
    if (!result) {
      fail(s"""
              |checking $table data failed
              |expect data:${expectedOutputs.mkString("\n", "\n", "\n")}
              |fluss data:${rowsWithType.mkString("\n", "\n", "\n")}
              |""".stripMargin)
    }
  }

  test("write: write to log table") {
    withTable("t") {
      val tablePath = createTablePath("t")
      spark.sql(s"CREATE TABLE t (id bigint, data string)")
      val table = loadFlussTable(tablePath)
      assert(!table.getTableInfo.hasPrimaryKey)
      assert(!table.getTableInfo.hasBucketKey)

      val rows = getRowsWithChangeType(table).map(_._2)
      assert(rows.isEmpty)

      val input1 = Seq((1L, "a"), (2L, "b"), (3L, "c"))
      val input2 = Seq((4L, "d"), (5L, "e"), (6L, "f"))
      val expect1 = input1.map(r => ("+A", r._1, r._2))
      val expect2 = (input1 ++ input2).map(r => ("+A", r._1, r._2))
      runTestWithStream("t", input1, input2, expect1, expect2)
    }
  }

  test("write: write to primary key table") {
    withTable("t") {
      val tablePath = createTablePath("t")
      spark.sql(s"""
                   |CREATE TABLE t (id bigint, data string) TBLPROPERTIES("primary.key" = "id")
                   |""".stripMargin)
      val table = loadFlussTable(tablePath)
      assert(table.getTableInfo.hasBucketKey)
      assert(table.getTableInfo.hasPrimaryKey)
      assert(table.getTableInfo.getPrimaryKeys.get(0).equalsIgnoreCase("id"))

      val rows = getRowsWithChangeType(table).map(_._2)
      assert(rows.isEmpty)

      val input1 = Seq((1L, "a"), (2L, "b"), (3L, "c"))
      val input2 = Seq((1L, "d"), (5L, "e"), (6L, "f"))
      val expect1 = input1.map(r => ("+I", r._1, r._2))
      val expect2 = Seq(
        ("+I", 1L, "a"),
        ("+I", 2L, "b"),
        ("+I", 3L, "c"),
        ("-U", 1L, "a"),
        ("+U", 1L, "d"),
        ("+I", 5L, "e"),
        ("+I", 6L, "f"))
      runTestWithStream("t", input1, input2, expect1, expect2)
    }
  }

  test("read: log table") {
    val tableName = "t"
    withTable(tableName) {
      sql("CREATE TABLE t (id int, data string)")
      sql("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      val schema = StructType(Seq(StructField("id", IntegerType), StructField("data", StringType)))

      // Test with ProcessAllAvailable
      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        ProcessAllAvailable(),
        CheckLastBatch(),
        StopStream,
        StartStream(),
        AddFlussData(tableName, schema, Seq(Row(4, "data4"), Row(5, "data5"))),
        ProcessAllAvailable(),
        CheckLastBatch(Row(4, "data4"), Row(5, "data5")),
        CheckAnswer(Row(4, "data4"), Row(5, "data5"))
      )

      // Test with timed trigger
      val clock = new StreamManualClock
      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        StartStream(trigger = Trigger.ProcessingTime(500), clock),
        AdvanceManualClock(500),
        CheckNewAnswer(),
        AddFlussData(tableName, schema, Seq(Row(4, "data4"), Row(5, "data5"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(4, "data4"), Row(5, "data5")),
        CheckAnswer(Row(4, "data4"), Row(5, "data5")),
        AddFlussData(tableName, schema, Seq(Row(6, "data6"), Row(7, "data7"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(6, "data6"), Row(7, "data7")),
        CheckAnswer(Row(4, "data4"), Row(5, "data5"), Row(6, "data6"), Row(7, "data7"))
      )
    }
  }

  test("read: log partition table") {
    val tableName = "t"
    withTable(tableName) {
      sql("CREATE TABLE t (id int, data string, pt string) PARTITIONED BY (pt)")
      sql("INSERT INTO t VALUES (1, 'a', '11'), (2, 'b', '11'), (3, 'c', '22')")

      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("pt", StringType)))

      // Test with ProcessAllAvailable
      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        ProcessAllAvailable(),
        CheckLastBatch(),
        StopStream,
        StartStream(),
        AddFlussData(tableName, schema, Seq(Row(4, "data4", "22"), Row(5, "data5", "11"))),
        ProcessAllAvailable(),
        CheckLastBatch(Row(4, "data4", "22"), Row(5, "data5", "11")),
        CheckAnswer(Row(4, "data4", "22"), Row(5, "data5", "11"))
      )

      // Test with timed trigger
      val clock = new StreamManualClock
      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        StartStream(trigger = Trigger.ProcessingTime(500), clock),
        AdvanceManualClock(500),
        CheckNewAnswer(),
        AddFlussData(tableName, schema, Seq(Row(4, "data4", "22"), Row(5, "data5", "11"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(4, "data4", "22"), Row(5, "data5", "11")),
        CheckAnswer(Row(4, "data4", "22"), Row(5, "data5", "11")),
        AddFlussData(tableName, schema, Seq(Row(6, "data6", "22"), Row(7, "data7", "11"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(6, "data6", "22"), Row(7, "data7", "11")),
        CheckAnswer(
          Row(4, "data4", "22"),
          Row(5, "data5", "11"),
          Row(6, "data6", "22"),
          Row(7, "data7", "11"))
      )
    }
  }

  test("read: primary key table") {
    val tableName = "t"
    withTable(tableName) {
      sql(
        "CREATE TABLE t (pk1 int, pk2 string, id int, data string) TBLPROPERTIES('primary.key' = 'pk1, pk2',  'bucket.num' = 1)")
      sql("INSERT INTO t VALUES (1, 'a', 11, 'aa'), (2, 'b', 22, 'bb'), (3, 'c', 33, 'cc')")

      val schema = StructType(
        Seq(
          StructField("pk1", IntegerType),
          StructField("pk2", StringType),
          StructField("id", IntegerType),
          StructField("data", StringType)))

      // Test with ProcessAllAvailable
      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        ProcessAllAvailable(),
        CheckLastBatch(),
        StopStream,
        StartStream(),
        AddFlussData(tableName, schema, Seq(Row(4, "data4", 44, "dd"), Row(5, "data5", 55, "ee"))),
        ProcessAllAvailable(),
        CheckLastBatch(Row(4, "data4", 44, "dd"), Row(5, "data5", 55, "ee")),
        CheckAnswer(Row(4, "data4", 44, "dd"), Row(5, "data5", 55, "ee"))
      )

      // Test with timed trigger
      val clock = new StreamManualClock
      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        StartStream(trigger = Trigger.ProcessingTime(500), clock),
        AdvanceManualClock(500),
        CheckNewAnswer(),
        AddFlussData(tableName, schema, Seq(Row(4, "data4", 44, "dd"), Row(5, "data5", 55, "ee"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(4, "data4", 44, "dd"), Row(5, "data5", 55, "ee")),
        CheckAnswer(Row(4, "data4", 44, "dd"), Row(5, "data5", 55, "ee")),
        AddFlussData(tableName, schema, Seq(Row(6, "data6", 66, "ff"), Row(7, "data7", 77, "gg"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(6, "data6", 66, "ff"), Row(7, "data7", 77, "gg")),
        CheckAnswer(
          Row(4, "data4", 44, "dd"),
          Row(5, "data5", 55, "ee"),
          Row(6, "data6", 66, "ff"),
          Row(7, "data7", 77, "gg"))
      )
    }
  }

  test("read: primary key partition table") {
    val tableName = "t"
    withTable(tableName) {
      sql(
        "CREATE TABLE t (pk1 int, pk2 string, id int, data string, dt string) PARTITIONED BY(dt) TBLPROPERTIES('primary.key' = 'pk1, pk2, dt',  'bucket.num' = 1)")
      sql(
        "INSERT INTO t VALUES (1, 'a', 11, 'aa', 'a'), (2, 'b', 22, 'bb', 'b'), (3, 'c', 33, 'cc', 'b')")

      val schema = StructType(
        Seq(
          StructField("pk1", IntegerType),
          StructField("pk2", StringType),
          StructField("id", IntegerType),
          StructField("data", StringType),
          StructField("dt", StringType)
        ))

      // Test with ProcessAllAvailable
      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        ProcessAllAvailable(),
        CheckLastBatch(),
        StopStream,
        StartStream(),
        AddFlussData(
          tableName,
          schema,
          Seq(Row(4, "data4", 44, "dd", "a"), Row(5, "data5", 55, "ee", "b"))),
        ProcessAllAvailable(),
        CheckLastBatch(Row(4, "data4", 44, "dd", "a"), Row(5, "data5", 55, "ee", "b")),
        CheckAnswer(Row(4, "data4", 44, "dd", "a"), Row(5, "data5", 55, "ee", "b"))
      )

      // Test with timed trigger
      val clock = new StreamManualClock
      testStream(spark.readStream.options(Map("scan.startup.mode" -> "latest")).table(tableName))(
        StartStream(trigger = Trigger.ProcessingTime(500), clock),
        AdvanceManualClock(500),
        CheckNewAnswer(),
        AddFlussData(
          tableName,
          schema,
          Seq(Row(4, "data4", 44, "dd", "a"), Row(5, "data5", 55, "ee", "b"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(4, "data4", 44, "dd", "a"), Row(5, "data5", 55, "ee", "b")),
        CheckAnswer(Row(4, "data4", 44, "dd", "a"), Row(5, "data5", 55, "ee", "b")),
        AddFlussData(
          tableName,
          schema,
          Seq(Row(6, "data6", 66, "ff", "b"), Row(7, "data7", 77, "gg", "a"))),
        AdvanceManualClock(500),
        CheckLastBatch(Row(6, "data6", 66, "ff", "b"), Row(7, "data7", 77, "gg", "a")),
        CheckAnswer(
          Row(4, "data4", 44, "dd", "a"),
          Row(5, "data5", 55, "ee", "b"),
          Row(6, "data6", 66, "ff", "b"),
          Row(7, "data7", 77, "gg", "a"))
      )
    }
  }

  private def writeToLogTable(table: Table, schema: StructType, dataArr: Seq[Row]): Unit = {
    val writer = if (table.getTableInfo.hasPrimaryKey) {
      FlussUpsertDataWriter(table.getTableInfo.getTablePath, schema, conn.getConfiguration)
    } else {
      FlussAppendDataWriter(table.getTableInfo.getTablePath, schema, conn.getConfiguration)
    }
    val rows = dataArr.map {
      row =>
        val internalRow = InternalRow.fromSeq(row.toSeq.map {
          case v: String => UTF8String.fromString(v)
          case v => v
        })
        internalRow
    }
    rows.foreach(internalRow => writer.write(internalRow))
    writer.commit()
  }

  case class AddFlussData[T](tableName: String, schema: StructType, dataArr: Seq[Row])
    extends AddData {
    override def addData(query: Option[StreamExecution]): (SparkDataStream, Offset) = {
      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active fluss stream source")
      val sources: Seq[FlussMicroBatchStream] = {
        query.get.logicalPlan.collect {
          case r: StreamingDataSourceV2Relation if r.stream.isInstanceOf[FlussMicroBatchStream] =>
            r.stream
        }
      }.distinct.map(_.asInstanceOf[FlussMicroBatchStream])
      if (!sources.exists(s => s.tablePath.equals(createTablePath(tableName)))) {
        throw new IllegalArgumentException(
          s"Could not find fluss stream source for table $tableName")
      }

      val flussTable = loadFlussTable(createTablePath(tableName))
      writeToLogTable(flussTable, schema, dataArr)

      val flussSource = sources.filter(s => s.tablePath.equals(createTablePath(tableName))).head
      val buckets = (0 until flussTable.getTableInfo.getNumBuckets).toSeq

      val offsetsInitializer = OffsetsInitializer.latest()
      val tableBucketOffsets = if (flussTable.getTableInfo.isPartitioned) {
        val partitionInfos = admin.listPartitionInfos(flussTable.getTableInfo.getTablePath).get()
        val partitionOffsets = partitionInfos.asScala.map(
          partitionInfo =>
            FlussMicroBatchStream.getLatestOffsets(
              flussTable.getTableInfo,
              offsetsInitializer,
              new BucketOffsetsRetrieverImpl(admin, flussTable.getTableInfo.getTablePath),
              buckets,
              Some(partitionInfo)))
        val mergedOffsets = partitionOffsets
          .map(_.getOffsets)
          .reduce((l, r) => (l.asScala ++ r.asScala).asJava)
        new TableBucketOffsets(flussTable.getTableInfo.getTableId, mergedOffsets)
      } else {
        FlussMicroBatchStream.getLatestOffsets(
          flussTable.getTableInfo,
          offsetsInitializer,
          new BucketOffsetsRetrieverImpl(admin, flussTable.getTableInfo.getTablePath),
          buckets,
          None)
      }

      val offset = FlussSourceOffset(tableBucketOffsets)
      (flussSource, offset)
    }
  }
}
