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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/**
 * Procedure to get cluster configuration(s).
 *
 * This procedure allows querying dynamic cluster configurations. It can retrieve:
 *   - Specific configurations by key(s)
 *   - All configurations (when no keys are provided)
 *
 * Usage examples:
 * {{{
 * -- Get a specific configuration
 * CALL sys.get_cluster_configs('kv.rocksdb.shared-rate-limiter.bytes-per-sec')
 *
 * -- Get all cluster configurations
 * CALL sys.get_cluster_configs()
 * }}}
 */
class GetClusterConfigsProcedure(tableCatalog: TableCatalog) extends BaseProcedure(tableCatalog) {

  override def parameters(): Array[ProcedureParameter] = {
    GetClusterConfigsProcedure.PARAMETERS
  }

  override def outputType(): StructType = {
    GetClusterConfigsProcedure.OUTPUT_TYPE
  }

  override def call(args: InternalRow): Array[InternalRow] = {
    val configKeys = if (args.numFields > 0 && !args.isNullAt(0)) {
      val keysArray = args.getArray(0)
      (0 until keysArray.numElements())
        .map(i => keysArray.getUTF8String(i).toString)
        .toArray
    } else {
      Array.empty[String]
    }

    getConfigs(configKeys)
  }

  private def getConfigs(configKeys: Array[String]): Array[InternalRow] = {
    try {
      val admin = getAdmin()
      val configs = admin.describeClusterConfigs().get().asScala

      if (configKeys.isEmpty) {
        configs
          .map(
            entry =>
              newInternalRow(
                UTF8String.fromString(entry.key()),
                UTF8String.fromString(entry.value()),
                UTF8String.fromString(formatConfigSource(entry.source()))
              ))
          .toArray
      } else {
        val configEntryMap = configs.map(e => e.key() -> e).toMap
        configKeys.flatMap {
          key =>
            configEntryMap.get(key).map {
              entry =>
                newInternalRow(
                  UTF8String.fromString(entry.key()),
                  UTF8String.fromString(entry.value()),
                  UTF8String.fromString(formatConfigSource(entry.source()))
                )
            }
        }
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to get cluster config: ${e.getMessage}", e)
    }
  }

  private def formatConfigSource(
      source: org.apache.fluss.config.cluster.ConfigEntry.ConfigSource): String = {
    if (source == null) {
      "UNKNOWN"
    } else {
      source.name() match {
        case "DYNAMIC_SERVER_CONFIG" => "DYNAMIC"
        case "INITIAL_SERVER_CONFIG" => "STATIC"
        case _ => source.name()
      }
    }
  }

  override def description(): String = {
    "Retrieve cluster configuration values."
  }
}

object GetClusterConfigsProcedure {

  private val PARAMETERS: Array[ProcedureParameter] = Array(
    ProcedureParameter.optional("config_keys", DataTypes.createArrayType(DataTypes.StringType))
  )

  private val OUTPUT_TYPE: StructType = new StructType(
    Array(
      new StructField("config_key", DataTypes.StringType, nullable = false, Metadata.empty),
      new StructField("config_value", DataTypes.StringType, nullable = false, Metadata.empty),
      new StructField("config_source", DataTypes.StringType, nullable = false, Metadata.empty)
    )
  )

  def builder(): ProcedureBuilder = {
    new BaseProcedure.Builder[GetClusterConfigsProcedure]() {
      override protected def doBuild(): GetClusterConfigsProcedure = {
        new GetClusterConfigsProcedure(getTableCatalog)
      }
    }
  }
}
