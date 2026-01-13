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

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

import java.io.File

class SparkStreamingTest extends FlussSparkTestBase with StreamTest {
  import testImplicits._

  private def runTestWithStreamAppend(
      tableIdentifier: String,
      checkFunc: (String, Seq[(String, Long, String)]) => Unit): Unit = {
    withTempDir {
      checkpointDir =>
        val input1 = Seq((1L, "a"), (2L, "b"), (3L, "c"))
        verifyStream(
          tableIdentifier,
          checkpointDir,
          Seq.empty,
          input1,
          input1.map(r => ("+A", r._1, r._2)),
          checkFunc)

        val input2 = Seq((4L, "d"), (5L, "e"), (6L, "f"))
        verifyStream(
          tableIdentifier,
          checkpointDir,
          Seq(input1),
          input2,
          (input1 ++ input2).map(r => ("+A", r._1, r._2)),
          checkFunc)
    }
  }

  private def runTestWithStreamUpsert(
      tableIdentifier: String,
      checkFunc: (String, Seq[(String, Long, String)]) => Unit): Unit = {
    withTempDir {
      checkpointDir =>
        val input1 = Seq((1L, "a"), (2L, "b"), (3L, "c"))
        verifyStream(
          tableIdentifier,
          checkpointDir,
          Seq.empty,
          input1,
          input1.map(r => ("+I", r._1, r._2)),
          checkFunc)

        val input2 = Seq((1L, "d"), (5L, "e"), (6L, "f"))
        val expect = Seq(
          ("+I", 1L, "a"),
          ("+I", 2L, "b"),
          ("+I", 3L, "c"),
          ("-U", 1L, "a"),
          ("+U", 1L, "d"),
          ("+I", 5L, "e"),
          ("+I", 6L, "f"))
        verifyStream(tableIdentifier, checkpointDir, Seq(input1), input2, expect, checkFunc)
    }
  }

  private def verifyStream(
      tableIdentifier: String,
      checkpointDir: File,
      prevInputs: Seq[Seq[(Long, String)]],
      newInputs: Seq[(Long, String)],
      expectedOutputs: Seq[(String, Long, String)],
      checkFunc: (String, Seq[(String, Long, String)]) => Unit): Unit = {
    runStreamQuery(tableIdentifier, checkpointDir, prevInputs, newInputs)
    // TODO verified from spark read
    checkFunc(tableIdentifier, expectedOutputs)
  }

  private def runStreamQuery(
      tableIdentifier: String,
      checkpointDir: File,
      prevInputs: Seq[Seq[(Long, String)]],
      newInputs: Seq[(Long, String)]): Unit = {
    val inputData = MemoryStream[(Long, String)]
    val inputDF = inputData.toDF().toDF("id", "data")

    prevInputs.foreach(inputsPerBatch => inputData.addData(inputsPerBatch: _*))

    val query = inputDF.writeStream
      .option("checkpointLocation", checkpointDir.getAbsolutePath)
      .toTable(tableIdentifier)

    inputData.addData(newInputs: _*)

    query.processAllAvailable()
    query.stop()
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

      val checkFunc = (name: String, expectInput: Seq[(String, Long, String)]) => {
        val table = loadFlussTable(tablePath)
        val rowsWithType = getRowsWithChangeType(table)
        assert(rowsWithType.length == expectInput.length)

        val row = rowsWithType.head._2
        assert(row.getFieldCount == 2)

        val result = rowsWithType.zip(expectInput).forall {
          case (flussRowWithType, expect) =>
            flussRowWithType._1.equals(expect._1) && flussRowWithType._2.getLong(
              0) == expect._2 && flussRowWithType._2.getString(1).toString == expect._3
        }
        if (!result) {
          fail(s"""
                  |checking $name data failed
                  |expect data:${expectInput.mkString("\n", "\n", "\n")}
                  |fluss data:${rows.mkString("\n", "\n", "\n")}
                  |""".stripMargin)
        }
      }

      runTestWithStreamAppend("t", checkFunc)
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

      val checkFunc = (name: String, expectInput: Seq[(String, Long, String)]) => {
        val table = loadFlussTable(tablePath)
        val rowsWithType = getRowsWithChangeType(table)
        assert(rowsWithType.length == expectInput.length)

        val row = rowsWithType.head._2
        assert(row.getFieldCount == 2)

        val result = rowsWithType.zip(expectInput).forall {
          case (flussRowWithType, expect) =>
            flussRowWithType._1.equals(expect._1) && flussRowWithType._2.getLong(
              0) == expect._2 && flussRowWithType._2.getString(1).toString == expect._3
        }
        if (!result) {
          fail(s"""
                  |checking $name data failed
                  |expect data:${expectInput.mkString("\n", "\n", "\n")}
                  |fluss data:${rowsWithType.mkString("\n", "\n", "\n")}
                  |""".stripMargin)
        }
      }

      runTestWithStreamUpsert("t", checkFunc)
    }
  }
}
