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

package org.apache.fluss.spark.procedure

import org.apache.fluss.config.cluster.{AlterConfig, AlterConfigOpType}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/**
 * Procedure to set cluster configuration dynamically.
 *
 * This procedure allows modifying dynamic cluster configurations. The changes are:
 *   - Validated by the CoordinatorServer before persistence
 *   - Persisted in ZooKeeper for durability
 *   - Applied to all relevant servers (Coordinator and TabletServers)
 *   - Survive server restarts
 *
 * Usage examples:
 * {{{
 * -- Set a single configuration
 * CALL sys.set_cluster_configs(config_pairs => array('key1', 'value1'))
 *
 * -- Set multiple configurations at once
 * CALL sys.set_cluster_configs(config_pairs => array('key1', 'value1', 'key2', 'value2'))
 * }}}
 *
 * Note: Not all configurations support dynamic changes. The server will validate the change and
 * reject it if the configuration cannot be modified dynamically or if the new value is invalid.
 */
class SetClusterConfigsProcedure(tableCatalog: TableCatalog) extends BaseProcedure(tableCatalog) {

  override def parameters(): Array[ProcedureParameter] = {
    SetClusterConfigsProcedure.PARAMETERS
  }

  override def outputType(): StructType = {
    SetClusterConfigsProcedure.OUTPUT_TYPE
  }

  override def call(args: InternalRow): Array[InternalRow] = {
    val configPairs = if (args.numFields > 0 && !args.isNullAt(0)) {
      val pairsArray = args.getArray(0)
      (0 until pairsArray.numElements()).map {
        i =>
          if (pairsArray.isNullAt(i)) {
            throw new IllegalArgumentException(
              s"config_pairs contains a null element at position $i. " +
                "Please specify valid configuration key/value pairs.")
          }
          pairsArray.getUTF8String(i).toString
      }.toArray
    } else {
      Array.empty[String]
    }

    setConfigs(configPairs)
  }

  private def setConfigs(configPairs: Array[String]): Array[InternalRow] = {
    if (configPairs.isEmpty) {
      throw new IllegalArgumentException(
        "config_pairs cannot be null or empty. " +
          "Please specify valid configuration pairs.")
    }

    if (configPairs.length % 2 != 0) {
      throw new IllegalArgumentException(
        "config_pairs must be set in pairs (key, value). " +
          "Please specify valid configuration pairs.")
    }

    try {
      val configList = new java.util.ArrayList[AlterConfig]()
      val results = scala.collection.mutable.ArrayBuffer[InternalRow]()

      for (i <- configPairs.indices by 2) {
        val configKey = configPairs(i).trim
        if (configKey.isEmpty) {
          throw new IllegalArgumentException(
            "Config key cannot be null or empty. " +
              "Please specify a valid configuration key.")
        }
        val configValue = configPairs(i + 1)

        configList.add(new AlterConfig(configKey, configValue, AlterConfigOpType.SET))
        results += newInternalRow(
          UTF8String.fromString(configKey),
          UTF8String.fromString(configValue),
          UTF8String.fromString(s"Successfully set configuration '$configKey' to '$configValue'.")
        )
      }

      admin.alterClusterConfigs(configList).get()

      results.toArray
    } catch {
      case e: IllegalArgumentException =>
        throw e
      case e: Exception =>
        throw new RuntimeException(s"Failed to set cluster config: ${e.getMessage}", e)
    }
  }

  override def description(): String = {
    "Set cluster configuration values dynamically."
  }
}

object SetClusterConfigsProcedure {

  private val PARAMETERS: Array[ProcedureParameter] = Array(
    ProcedureParameter.required("config_pairs", DataTypes.createArrayType(DataTypes.StringType))
  )

  private val OUTPUT_TYPE: StructType = new StructType(
    Array(
      new StructField("config_key", DataTypes.StringType, nullable = false, Metadata.empty),
      new StructField("config_value", DataTypes.StringType, nullable = false, Metadata.empty),
      new StructField("result", DataTypes.StringType, nullable = false, Metadata.empty)
    )
  )

  def builder(): ProcedureBuilder = {
    new BaseProcedure.Builder[SetClusterConfigsProcedure]() {
      override protected def doBuild(): SetClusterConfigsProcedure = {
        new SetClusterConfigsProcedure(getTableCatalog)
      }
    }
  }
}
