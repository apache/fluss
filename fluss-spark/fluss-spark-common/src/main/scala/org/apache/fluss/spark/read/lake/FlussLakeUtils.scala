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

package org.apache.fluss.spark.read.lake

import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp
import org.apache.fluss.lake.source.{LakeSource, LakeSplit}
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.utils.PropertiesUtils

import java.util

import scala.collection.JavaConverters._

object FlussLakeUtils {

  /**
   * Paimon DLF supports multiple authentication schemes (see
   * https://paimon.apache.org/docs/1.3/concepts/rest/dlf/), allowing clients to freely choose the
   * appropriate method based on their runtime environment. However, the Fluss server does not
   * propagate auth configs for all scenarios. These keys are therefore overridden with the values
   * from the Catalog configuration, replacing whatever the Fluss server provides, so that clients
   * can use their preferred authentication method.
   */
  private val REMOVAL_KEYS = Set(
    "dlf.access-key-id",
    "dlf.access-key-secret",
    "dlf.security-token",
    "dlf.token-path",
    "dlf.token-loader")

  def createLakeSource(
      catalogProperties: util.Map[String, String],
      tableProperties: util.Map[String, String],
      tablePath: TablePath): LakeSource[LakeSplit] = {
    val tableConfig = Configuration.fromMap(tableProperties)
    val datalakeFormat = tableConfig.get(ConfigOptions.TABLE_DATALAKE_FORMAT)
    val dataLakePrefix = "table.datalake." + datalakeFormat + "."

    val rewriteProperties =
      PropertiesUtils.extractAndRemovePrefix(tableProperties, dataLakePrefix).asScala.filterNot {
        case (k, _) => REMOVAL_KEYS(k)
      } ++
        catalogProperties.asScala.filter { case (k, _) => REMOVAL_KEYS(k) }

    val lakeConfig =
      Configuration.fromMap(rewriteProperties.asJava)
    val lakeStoragePlugin =
      LakeStoragePluginSetUp.fromDataLakeFormat(datalakeFormat.toString, null)
    val lakeStorage = lakeStoragePlugin.createLakeStorage(lakeConfig)
    lakeStorage.createLakeSource(tablePath).asInstanceOf[LakeSource[LakeSplit]]
  }

  def lakeProjection(projection: Array[Int]): Array[Array[Int]] = {
    projection.map(i => Array(i))
  }
}
