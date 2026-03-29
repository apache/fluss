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

import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.lake.committer.{CommitterInitContext, LakeCommitter}
import org.apache.fluss.lake.writer.{LakeTieringFactory, WriterInitContext}
import org.apache.fluss.metadata.{TableBucket, TableInfo, TablePath}
import org.apache.fluss.rpc.messages.{CommitLakeTableSnapshotRequest, PbLakeTableOffsetForBucket, PbLakeTableSnapshotInfo}
import org.apache.fluss.spark.FlussSparkTestBase
import org.apache.fluss.spark.SparkConnectorOptions.BUCKET_NUMBER

import org.apache.spark.sql.Row

import java.time.Duration
import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Base class for lake-enabled log table read tests. Subclasses provide the lake format config and
 * tiering factory.
 */
abstract class SparkLakeLogTableReadTestBase extends FlussSparkTestBase {

  protected var warehousePath: String = _

  protected def createLakeTieringFactory(): LakeTieringFactory[_, _]

  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try {
      f
    } finally {
      tableNames.foreach(t => sql(s"DROP TABLE IF EXISTS $DEFAULT_DATABASE.$t"))
    }
  }

  private def tierToLake(tp: TablePath, ti: TableInfo, expectedRecordCount: Int): Long = {
    val tableId = ti.getTableId

    val table = loadFlussTable(tp)
    val logScanner = table.newScan().createLogScanner()
    logScanner.subscribeFromBeginning(0)

    val scanRecords =
      new java.util.ArrayList[org.apache.fluss.client.table.scanner.ScanRecord]()
    val deadline = System.currentTimeMillis() + 30000
    while (scanRecords.size() < expectedRecordCount && System.currentTimeMillis() < deadline) {
      val batch = logScanner.poll(Duration.ofSeconds(1))
      batch.iterator().asScala.foreach(r => scanRecords.add(r))
    }
    assert(
      scanRecords.size() == expectedRecordCount,
      s"Expected $expectedRecordCount scan records, got ${scanRecords.size()}")
    val logEndOffset = scanRecords.asScala.map(_.logOffset()).max + 1

    val factory = createLakeTieringFactory()

    val tb = new TableBucket(tableId, null, 0)
    val lakeWriter = factory
      .asInstanceOf[LakeTieringFactory[Any, Any]]
      .createLakeWriter(new WriterInitContext {
        override def tablePath(): TablePath = tp
        override def tableBucket(): TableBucket = tb
        override def partition(): String = null
        override def tableInfo(): TableInfo = ti
      })
    for (record <- scanRecords.asScala) {
      lakeWriter.write(record)
    }
    val writeResult = lakeWriter.complete()
    lakeWriter.close()

    val lakeCommitter = factory
      .asInstanceOf[LakeTieringFactory[Any, Any]]
      .createLakeCommitter(new CommitterInitContext {
        override def tablePath(): TablePath = tp
        override def tableInfo(): TableInfo = ti
        override def lakeTieringConfig(): Configuration = new Configuration()
        override def flussClientConfig(): Configuration = new Configuration()
      })
    val committable =
      lakeCommitter.toCommittable(Collections.singletonList(writeResult))
    val commitResult =
      lakeCommitter.commit(committable, Collections.emptyMap())
    val snapshotId = commitResult.getCommittedSnapshotId
    lakeCommitter.close()

    val coordinatorGateway = flussServer.newCoordinatorClient()
    val request = new CommitLakeTableSnapshotRequest()
    val tableReq: PbLakeTableSnapshotInfo = request.addTablesReq()
    tableReq.setTableId(tableId)
    tableReq.setSnapshotId(snapshotId)
    val bucketReq: PbLakeTableOffsetForBucket = tableReq.addBucketsReq()
    bucketReq.setBucketId(0)
    bucketReq.setLogEndOffset(logEndOffset)
    bucketReq.setMaxTimestamp(System.currentTimeMillis())
    coordinatorGateway.commitLakeTableSnapshot(request).get()

    Thread.sleep(2000)

    logScanner.close()
    table.close()

    logEndOffset
  }

  test("Spark Lake Read: log table falls back when no lake snapshot") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t VALUES
             |(1, "hello"), (2, "world"), (3, "fluss")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t ORDER BY id"),
        Row(1, "hello") :: Row(2, "world") :: Row(3, "fluss") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t ORDER BY name"),
        Row("fluss") :: Row("hello") :: Row("world") :: Nil
      )
    }
  }

  test("Spark Lake Read: log table lake-only (all data in lake, no log tail)") {
    withTable("t_lake_only") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_lake_only (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_lake_only VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      val tablePath = createTablePath("t_lake_only")
      val table = loadFlussTable(tablePath)
      val tableInfo = table.getTableInfo
      table.close()

      tierToLake(tablePath, tableInfo, expectedRecordCount = 3)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_lake_only ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t_lake_only ORDER BY name"),
        Row("alpha") :: Row("beta") :: Row("gamma") :: Nil
      )
    }
  }

  test("Spark Lake Read: log table union read (lake + log tail)") {
    withTable("t_union") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_union (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      val tablePath = createTablePath("t_union")
      val table = loadFlussTable(tablePath)
      val tableInfo = table.getTableInfo
      table.close()

      tierToLake(tablePath, tableInfo, expectedRecordCount = 3)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_union VALUES
             |(4, "delta"), (5, "epsilon")
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $DEFAULT_DATABASE.t_union ORDER BY id"),
        Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") ::
          Row(4, "delta") :: Row(5, "epsilon") :: Nil
      )

      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.t_union ORDER BY name"),
        Row("alpha") :: Row("beta") :: Row("delta") ::
          Row("epsilon") :: Row("gamma") :: Nil
      )
    }
  }

  test("Spark Lake Read: non-FULL startup mode skips lake path") {
    withTable("t_earliest") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t_earliest (id INT, name STRING)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${BUCKET_NUMBER.key()}' = 1)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO $DEFAULT_DATABASE.t_earliest VALUES
             |(1, "alpha"), (2, "beta"), (3, "gamma")
             |""".stripMargin)

      val tablePath = createTablePath("t_earliest")
      val table = loadFlussTable(tablePath)
      val tableInfo = table.getTableInfo
      table.close()

      tierToLake(tablePath, tableInfo, expectedRecordCount = 3)

      try {
        spark.conf.set("spark.sql.fluss.scan.startup.mode", "earliest")

        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.t_earliest ORDER BY id"),
          Row(1, "alpha") :: Row(2, "beta") :: Row(3, "gamma") :: Nil
        )
      } finally {
        spark.conf.set("spark.sql.fluss.scan.startup.mode", "full")
      }
    }
  }
}
