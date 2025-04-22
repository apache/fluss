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

import com.alibaba.fluss.client.{Connection, ConnectionFactory}
import com.alibaba.fluss.client.admin.Admin
import com.alibaba.fluss.client.table.writer.{AppendWriter, TableWriter, UpsertWriter}
import com.alibaba.fluss.config.{AutoPartitionTimeUnit, ConfigOption, ConfigOptions, Configuration}
import com.alibaba.fluss.metadata.{Schema, TableBucket, TableDescriptor, TableInfo, TablePath}
import com.alibaba.fluss.row.{BinaryString, GenericRow, InternalRow}
import com.alibaba.fluss.server.coordinator.MetadataManager
import com.alibaba.fluss.server.testutils.FlussClusterExtension
import com.alibaba.fluss.server.utils.TableAssignmentUtils
import com.alibaba.fluss.server.zk.ZooKeeperClient
import com.alibaba.fluss.server.zk.data.{PartitionAssignment, TableAssignment}
import com.alibaba.fluss.spark.FlussSparkTestBase.FLUSS_CLUSTER_EXTENSION
import com.alibaba.fluss.spark.catalog.SparkCatalog
import com.alibaba.fluss.spark.sql.{SparkVersionSupport, WithTableOptions}
import com.alibaba.fluss.testutils.DataTestUtils
import com.alibaba.fluss.testutils.common.CommonTestUtils.{waitUtil, waitValue}
import com.alibaba.fluss.types.DataTypes

import org.apache.spark.{sql, SparkConf}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.fluss.Utils
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.jupiter.api.extension.RegisterExtension
import org.junit.jupiter.api.function.ThrowingSupplier
import org.scalactic.source.Position
import org.scalatest.Tag
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import java.{util => ju}
import java.io.File
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.Random

class FlussSparkTestBase
  extends QueryTest
  with SharedSparkSession
  with SparkVersionSupport
  with WithTableOptions {

  protected lazy val tempDBDir: File = Utils.createTempDir

  protected def flussCatalog: SparkCatalog = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[SparkCatalog]
  }

  protected var conn: Connection = _
  protected var admin: Admin = _

  var flussConf: Configuration = _
  protected val DEFAULT_BUCKET_NUM = 3

  protected val DEFAULT_PK_TABLE_SCHEMA: Schema = Schema.newBuilder
    .primaryKey("id")
    .column("id", DataTypes.INT)
    .column("name", DataTypes.STRING)
    .build

  protected val DEFAULT_PK_TABLE_DESCRIPTOR: TableDescriptor = TableDescriptor.builder
    .schema(DEFAULT_PK_TABLE_SCHEMA)
    .distributedBy(DEFAULT_BUCKET_NUM, "id")
    .build

  protected val DEFAULT_AUTO_PARTITIONED_LOG_TABLE_DESCRIPTOR: TableDescriptor =
    TableDescriptor.builder
      .schema(Schema.newBuilder.column("id", DataTypes.INT).column("name", DataTypes.STRING).build)
      .distributedBy(DEFAULT_BUCKET_NUM)
      .partitionedBy("name")
      .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
      .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR)
      .build

  protected val DEFAULT_AUTO_PARTITIONED_PK_TABLE_DESCRIPTOR: TableDescriptor =
    TableDescriptor.builder
      .schema(
        Schema.newBuilder
          .column("id", DataTypes.INT)
          .column("name", DataTypes.STRING)
          .column("date", DataTypes.STRING)
          .primaryKey("id", "date")
          .build)
      .distributedBy(DEFAULT_BUCKET_NUM)
      .partitionedBy("date")
      .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), "true")
      .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR)
      .build
  protected val DEFAULT_DB: String = "test_spark_db"

  protected val tableName0: String = "T"

  override protected def sparkConf: SparkConf = {
    val serializer = if (Random.nextBoolean()) {
      "org.apache.spark.serializer.KryoSerializer"
    } else {
      "org.apache.spark.serializer.JavaSerializer"
    }
    super.sparkConf
      .set("spark.sql.catalog.fluss", classOf[SparkCatalog].getName)
      .set(
        "spark.sql.catalog.fluss.bootstrap.servers",
        String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS)))
      .set("spark.serializer", serializer)
  }

  override protected def beforeAll(): Unit = {
    FLUSS_CLUSTER_EXTENSION.start()
    flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig
    conn = ConnectionFactory.createConnection(flussConf)
    admin = conn.getAdmin
    super.beforeAll()
    spark.sql(s"USE fluss")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS fluss.$DEFAULT_DB")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql(s"USE fluss")
      spark.sql(s"DROP TABLE IF EXISTS $DEFAULT_DB.$tableName0")
      spark.sql(s"DROP DATABASE fluss.$DEFAULT_DB CASCADE")
      if (admin != null) {
        admin.close()
        admin = null
      }

      if (conn != null) {
        conn.close()
        conn = null
      }
      FLUSS_CLUSTER_EXTENSION.close()
    } finally {
      super.afterAll()
    }
  }

  protected def reset(): Unit = {
    afterAll()
    beforeAll()
  }

  /** Default is paimon catalog */
  override protected def beforeEach(): Unit = {
    super.beforeAll()
    spark.sql(s"USE fluss")
    spark.sql(s"USE fluss.$DEFAULT_DB")
    spark.sql(s"DROP TABLE IF EXISTS $tableName0")
  }

  protected def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir(file1 => withTempDir(file2 => f(file1, file2)))
  }

  protected def withTimeZone(timeZone: String)(f: => Unit): Unit = {
    withSQLConf("spark.sql.session.timeZone" -> timeZone) {
      val originTimeZone = ju.TimeZone.getDefault
      try {
        ju.TimeZone.setDefault(ju.TimeZone.getTimeZone(timeZone))
        f
      } finally {
        ju.TimeZone.setDefault(originTimeZone)
      }
    }
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    println(testName)
    super.test(testName, testTags: _*)(testFun)(pos)
  }

  def loadTable(tableName: String): SparkTable = {
    loadTable(DEFAULT_DB, tableName)
  }
  def createTable(tableName: String): SparkTable = {
    createTable(DEFAULT_DB, tableName)
  }
  def createTable(dbName: String, tableName: String): SparkTable = {
    spark.sql(s"CREATE TABLE $dbName.$tableName USING fluss")
    flussCatalog.loadTable(Identifier.of(Array(dbName), tableName)).asInstanceOf[SparkTable]
  }

  @throws[Exception]
  protected def createTable(tablePath: TablePath, tableDescriptor: TableDescriptor): Long = {
    admin.createTable(tablePath, tableDescriptor, true).get
    admin.getTableInfo(tablePath).get.getTableId
  }

  protected def waitUntilSnapshot(tableId: Long, snapshotId: Long): Unit = {
    for (i <- 0 until DEFAULT_BUCKET_NUM) {
      val tableBucket = new TableBucket(tableId, i)
      FLUSS_CLUSTER_EXTENSION.waitUtilSnapshotFinished(tableBucket, snapshotId)
    }
  }

  /**
   * Wait until the default number of partitions is created. Return the map from partition id to
   * partition name. .
   */
  def waitUntilPartitions(
      zooKeeperClient: ZooKeeperClient,
      tablePath: TablePath): ju.Map[java.lang.Long, String] = waitUntilPartitions(
    zooKeeperClient,
    tablePath,
    ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue)

  def loadTable(dbName: String, tableName: String): SparkTable = {
    flussCatalog.loadTable(Identifier.of(Array.apply(dbName), tableName)).asInstanceOf[SparkTable]
  }

  /**
   * Wait until the given number of partitions is created. Return the map from partition id to
   * partition name.
   */
  def waitUntilPartitions(
      zooKeeperClient: ZooKeeperClient,
      tablePath: TablePath,
      expectPartitions: Int): ju.Map[java.lang.Long, String] = {
    waitValue(
      () => {
        var result = ju.Optional.empty[ju.Map[java.lang.Long, String]]
        val gotPartitions: ju.Map[java.lang.Long, String] =
          zooKeeperClient.getPartitionIdAndNames(tablePath)
        if (expectPartitions == gotPartitions.size) {
          result = ju.Optional.of(gotPartitions)
        }
        result

      },
      Duration.ofMinutes(1),
      s"expect $expectPartitions table partition has not been created"
    )
  }

  @throws[Exception]
  def createPartitions(
      zkClient: ZooKeeperClient,
      tablePath: TablePath,
      partitionsToCreate: ju.List[String]): ju.Map[Long, String] = {
    val metadataManager: MetadataManager = new MetadataManager(zkClient, new Configuration)
    val tableInfo: TableInfo = metadataManager.getTable(tablePath)
    val newPartitionIds: ju.Map[Long, String] = new ju.HashMap[Long, String]
    for (partition <- partitionsToCreate.asScala) {
      val partitionId: Long = zkClient.getPartitionIdAndIncrement
      newPartitionIds.put(partitionId, partition)
      val assignment: TableAssignment = TableAssignmentUtils.generateAssignment(
        tableInfo.getNumBuckets,
        tableInfo.getTableConfig.getReplicationFactor,
        Array[Int](0, 1, 2))
      // register partition assignments
      zkClient.registerPartitionAssignment(
        partitionId,
        new PartitionAssignment(tableInfo.getTableId, assignment.getBucketAssignments))
      // register partition
      zkClient.registerPartition(tablePath, tableInfo.getTableId, partition, partitionId)
    }
    return newPartitionIds
  }

  @throws[Exception]
  def dropPartitions(
      zkClient: ZooKeeperClient,
      tablePath: TablePath,
      droppedPartitions: Set[String]): Unit = {
    import scala.collection.JavaConversions._
    for (partition <- droppedPartitions) {
      zkClient.deletePartition(tablePath, partition)
    }
  }

  @throws[Exception]
  protected def writeRowsToPartition(
      tablePath: TablePath,
      partitions: ju.Collection[String]): Seq[(Int, String, String)] = {
    val rows: ju.List[InternalRow] = new ju.ArrayList[InternalRow]
    var expectedRowValues: Seq[(Int, String, String)] = Seq()
    for (partition <- partitions.asScala) {
      for (i <- 0 until 10) {
        val internalRow =
          DataTestUtils.row(i.asInstanceOf[Object], "v1", partition).asInstanceOf[InternalRow]
        rows.add(internalRow)
        expectedRowValues = expectedRowValues ++ Seq((i, "v1", partition))
      }
    }
    // write records
    writeRows(tablePath, rows, false)
    expectedRowValues
  }

  @throws[Exception]
  protected def writeRows(
      tablePath: TablePath,
      rows: ju.List[InternalRow],
      append: Boolean): Unit = {
    try {
      val table = conn.getTable(tablePath)
      try {
        var tableWriter: TableWriter = null
        if (append) tableWriter = table.newAppend.createWriter
        else tableWriter = table.newUpsert.createWriter
        for (row <- rows.asScala) {
          if (tableWriter.isInstanceOf[AppendWriter])
            tableWriter.asInstanceOf[AppendWriter].append(row)
          else tableWriter.asInstanceOf[UpsertWriter].upsert(row)
        }
        tableWriter.flush()
      } finally if (table != null) table.close()
    }
  }
}

object FlussSparkTestBase {
  var FLUSS_CLUSTER_EXTENSION: FlussClusterExtension = FlussClusterExtension.builder
    .setClusterConf(
      new Configuration()
        .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
        .set(
          ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS.asInstanceOf[ConfigOption[Int]],
          Integer.MAX_VALUE))
    .setNumOfTabletServers(3)
    .build

  def row(objects: Any*): GenericRow = {
    val row = new GenericRow(objects.length)
    for (i <- 0 until objects.length) {
      objects(i) match {
        case s: String => row.setField(i, BinaryString.fromString(s))
        case _ => row.setField(i, objects(i))
      }
    }
    row
  }

  def rowWithPartition(values: Array[Any], partition: String) = if (partition == null)
    row(values: _*)
  else {
    val newValues = new Array[AnyRef](values.length + 1)
    System.arraycopy(values, 0, newValues, 0, values.length)
    newValues(values.length) = partition
    row(newValues: _*)
  }

}
