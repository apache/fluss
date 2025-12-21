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
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.server.testutils.FlussClusterExtension

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.jupiter.api.extension.RegisterExtension
import org.scalactic.source.Position
import org.scalatest.Tag

import java.time.Duration

class FlussSparkTestBase extends QueryTest with SharedSparkSession {

  import FlussSparkTestBase._

  protected val DEFAULT_DATABASE = "fluss";

  protected var conn: Connection = _
  protected var admin: Admin = _

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.fluss_catalog", classOf[SparkCatalog].getName)
      .set("spark.sql.catalog.fluss_catalog.bootstrap.servers", bootstrapServers)
      .set("spark.sql.defaultCatalog", "fluss_catalog")
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    conn = ConnectionFactory.createConnection(clientConf)
    admin = conn.getAdmin

    sql(s"USE $DEFAULT_DATABASE")
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    println(testName)
    super.test(testName, testTags: _*)(testFun)(pos)
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
