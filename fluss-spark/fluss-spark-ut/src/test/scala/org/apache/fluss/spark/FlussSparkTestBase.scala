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

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.admin.Admin
import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.metadata.{TableDescriptor, TablePath}
import org.apache.fluss.row.InternalRow
import org.apache.fluss.server.testutils.FlussClusterExtension

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.jupiter.api.extension.RegisterExtension

import java.time.Duration

import scala.collection.JavaConverters._

class FlussSparkTestBase extends QueryTest with SharedSparkSession {

  import FlussSparkTestBase._

  protected val DEFAULT_CATALOG = "fluss_catalog"
  protected val DEFAULT_DATABASE = "fluss"

  protected var conn: Connection = _
  protected var admin: Admin = _

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$DEFAULT_CATALOG", classOf[SparkCatalog].getName)
      .set(s"spark.sql.catalog.$DEFAULT_CATALOG.bootstrap.servers", bootstrapServers)
      .set("spark.sql.defaultCatalog", DEFAULT_CATALOG)
      // Enable read optimized by default temporarily.
      // TODO: remove this when https://github.com/apache/fluss/issues/2427 is done.
      .set("spark.sql.fluss.readOptimized", "true")
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    conn = ConnectionFactory.createConnection(clientConf)
    admin = conn.getAdmin

    sql(s"USE $DEFAULT_DATABASE")
  }

  def createTablePath(tableName: String): TablePath = {
    TablePath.of(DEFAULT_DATABASE, tableName)
  }

  def createFlussTable(tablePath: TablePath, tableDescriptor: TableDescriptor): Unit = {
    admin.createTable(tablePath, tableDescriptor, true).get()
  }

  def loadFlussTable(tablePath: TablePath): Table = {
    conn.getTable(tablePath)
  }

  /**
   * Get row with change type from table and logScanner if provided.
   *
   * @return
   *   Tuple composed of [[org.apache.fluss.record.ChangeType]] and [[InternalRow]]
   */
  def getRowsWithChangeType(
      table: Table,
      logScannerOption: Option[LogScanner] = None): Array[(String, InternalRow)] = {
    val logScanner = logScannerOption match {
      case Some(ls) => ls
      case _ =>
        val ls = table.newScan().createLogScanner()
        (0 until table.getTableInfo.getNumBuckets).foreach(i => ls.subscribeFromBeginning(i))
        ls
    }
    val scanRecords = logScanner.poll(Duration.ofSeconds(1))
    scanRecords
      .iterator()
      .asScala
      .map(record => (record.getChangeType.shortString(), record.getRow))
      .toArray
  }
}

@RegisterExtension
object FlussSparkTestBase {
  val FLUSS_CLUSTER_EXTENSION: FlussClusterExtension =
    FlussClusterExtension.builder
      .setClusterConf(
        new Configuration()
          .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
      )
      .setNumOfTabletServers(3)
      .build

  FLUSS_CLUSTER_EXTENSION.start()

  val clientConf: Configuration = FLUSS_CLUSTER_EXTENSION.getClientConfig
  val bootstrapServers: String = FLUSS_CLUSTER_EXTENSION.getBootstrapServers
}
