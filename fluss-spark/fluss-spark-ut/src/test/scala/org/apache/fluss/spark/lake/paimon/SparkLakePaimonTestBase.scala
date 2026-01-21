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

package org.apache.fluss.spark.lake.paimon

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.admin.Admin
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.exception.FlussRuntimeException
import org.apache.fluss.server.testutils.FlussClusterExtension
import org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties
import org.apache.fluss.spark.SparkCatalog

import org.apache.paimon.catalog.{Catalog, CatalogContext, CatalogFactory}
import org.apache.paimon.options.Options
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

import java.nio.file.Files
import java.time.Duration

class SparkLakePaimonTestBase extends QueryTest with SharedSparkSession {

  protected val DEFAULT_CATALOG = "fluss_catalog"
  protected val DEFAULT_DATABASE = "fluss"

  protected var conn: Connection = _
  protected var admin: Admin = _

  val flussServer: FlussClusterExtension =
    FlussClusterExtension.builder
      .setClusterConf(flussConf)
      .setNumOfTabletServers(3)
      .build

  protected var paimonCatalog: Catalog = _
  protected var warehousePath: String = _

  protected def flussConf: Configuration = {
    val conf = new Configuration
    conf
      .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1))
    conf.setInt(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE)
    conf.setString("datalake.format", "paimon")
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    try {
      warehousePath =
        Files.createTempDirectory("fluss-testing-datalake-tiered").resolve("warehouse").toString
    } catch {
      case e: Exception =>
        throw new FlussRuntimeException("Failed to create warehouse path")
    }
    conf.setString("datalake.paimon.warehouse", warehousePath)
    paimonCatalog = CatalogFactory.createCatalog(
      CatalogContext.create(Options.fromMap(extractLakeProperties(conf))))
    conf
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    flussServer.start()
    conn = ConnectionFactory.createConnection(flussServer.getClientConfig)
    admin = conn.getAdmin

    spark.conf.set(s"spark.sql.catalog.$DEFAULT_CATALOG", classOf[SparkCatalog].getName)
    spark.conf.set(
      s"spark.sql.catalog.$DEFAULT_CATALOG.bootstrap.servers",
      flussServer.getBootstrapServers)
    spark.conf.set("spark.sql.defaultCatalog", DEFAULT_CATALOG)

    sql(s"USE $DEFAULT_DATABASE")
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    if (admin != null) {
      admin.close()
      admin = null
    }
    if (conn != null) {
      conn.close()
      conn = null
    }
    flussServer.close()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    conn = ConnectionFactory.createConnection(flussServer.getClientConfig)
    admin = conn.getAdmin
  }

  override protected def afterEach(): Unit = {
    super.afterEach()

    if (admin != null) {
      admin.close()
      admin = null
    }
    if (conn != null) {
      conn.close()
      conn = null
    }
  }

  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    super.withTable(tableNames: _*)(f)
    tableNames.foreach(
      tableName =>
        paimonCatalog.dropTable(
          org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, tableName),
          true))
  }
}
