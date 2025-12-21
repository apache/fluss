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

package org.apache.fluss.spark.catalog

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.admin.Admin
import org.apache.fluss.config.{Configuration => FlussConfiguration}
import org.apache.fluss.utils.{IOUtils, Preconditions}

import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

import scala.collection.JavaConverters._

trait WithFlussAdmin extends AutoCloseable {

  private var _connection: Connection = _
  private var _admin: Admin = _

  // TODO: init lake spark catalog
  protected var lakeCatalog: CatalogPlugin = _

  protected def initFlussClient(options: CaseInsensitiveStringMap): Unit = {
    val flussConfigs = new util.HashMap[String, String]()
    options.entrySet().asScala.foreach {
      entry: util.Map.Entry[String, String] => flussConfigs.put(entry.getKey, entry.getValue)
    }

    _connection = ConnectionFactory.createConnection(FlussConfiguration.fromMap(flussConfigs))
    _admin = _connection.getAdmin
  }

  protected def admin: Admin = {
    Preconditions.checkNotNull(_admin, "Fluss Admin is not initialized.")
    _admin
  }

  override def close(): Unit = {
    IOUtils.closeQuietly(_admin, "fluss-admin")
    IOUtils.closeQuietly(_connection, "fluss-connection");
  }

}
