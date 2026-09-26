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
 * Procedure to reset cluster configurations to their default values.
 *
 * This procedure reverts the configurations to their initial system defaults. The changes are:
 *   - Validated by the CoordinatorServer before persistence
 *   - Persisted in ZooKeeper for durability
 *   - Applied to all relevant servers (Coordinator and TabletServers)
 *   - Survive server restarts
 *
 * Usage examples:
 * {{{
 * -- Reset a single configuration
 * CALL sys.reset_cluster_configs(config_keys => array('key1'))
 *
 * -- Reset multiple configurations at once
 * CALL sys.reset_cluster_configs(config_keys => array('key1', 'key2'))
 * }}}
 */
class ResetClusterConfigsProcedure(tableCatalog: TableCatalog) extends BaseProcedure(tableCatalog) {

  override def parameters(): Array[ProcedureParameter] = {
    ResetClusterConfigsProcedure.PARAMETERS
  }

  override def outputType(): StructType = {
    ResetClusterConfigsProcedure.OUTPUT_TYPE
  }

  override def call(args: InternalRow): Array[InternalRow] = {
    val configKeys = if (args.numFields > 0 && !args.isNullAt(0)) {
      val keysArray = args.getArray(0)
      (0 until keysArray.numElements()).map {
        i =>
          if (keysArray.isNullAt(i)) {
            throw new IllegalArgumentException(
              s"config_keys contains a null element at position $i. " +
                "Please specify valid configuration keys.")
          }
          keysArray.getUTF8String(i).toString
      }.toArray
    } else {
      Array.empty[String]
    }

    resetConfigs(configKeys)
  }

  private def resetConfigs(configKeys: Array[String]): Array[InternalRow] = {
    if (configKeys.isEmpty) {
      throw new IllegalArgumentException(
        "config_keys cannot be null or empty. " +
          "Please specify valid configuration keys.")
    }

    try {
      val configList = new java.util.ArrayList[AlterConfig]()
      val results = scala.collection.mutable.ArrayBuffer[InternalRow]()

      for (key <- configKeys) {
        val configKey = key.trim
        if (configKey.isEmpty) {
          throw new IllegalArgumentException(
            "Config key cannot be null or empty. " +
              "Please specify a valid configuration key.")
        }

        configList.add(new AlterConfig(configKey, null, AlterConfigOpType.DELETE))
        results += newInternalRow(
          UTF8String.fromString(configKey),
          UTF8String.fromString(
            s"Successfully deleted (reset to default) configuration '$configKey'.")
        )
      }

      admin.alterClusterConfigs(configList).get()

      results.toArray
    } catch {
      case e: IllegalArgumentException =>
        throw e
      case e: Exception =>
        throw new RuntimeException(s"Failed to reset cluster config: ${e.getMessage}", e)
    }
  }

  override def description(): String = {
    "Reset cluster configuration values to their defaults."
  }
}

object ResetClusterConfigsProcedure {

  private val PARAMETERS: Array[ProcedureParameter] = Array(
    ProcedureParameter.required("config_keys", DataTypes.createArrayType(DataTypes.StringType))
  )

  private val OUTPUT_TYPE: StructType = new StructType(
    Array(
      new StructField("config_key", DataTypes.StringType, nullable = false, Metadata.empty),
      new StructField("result", DataTypes.StringType, nullable = false, Metadata.empty)
    )
  )

  def builder(): ProcedureBuilder = {
    new BaseProcedure.Builder[ResetClusterConfigsProcedure]() {
      override protected def doBuild(): ResetClusterConfigsProcedure = {
        new ResetClusterConfigsProcedure(getTableCatalog)
      }
    }
  }
}
