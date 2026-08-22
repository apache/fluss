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

import org.apache.fluss.config.{ConfigOption, ConfigOptions, MemorySize}
import org.apache.fluss.config.cluster.ConfigEntry

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.types.{BooleanType, DataTypes, Metadata, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.time.Duration

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Procedure to get cluster configuration(s).
 *
 * This procedure allows querying dynamic cluster configurations. It can retrieve:
 *   - Specific configurations by key(s)
 *   - All configurations (when no keys are provided)
 *
 * Besides the resolved value and its source, each row also exposes the option's declared default
 * value, whether the current value equals that default, and the option description (when the key is
 * a well-known [[ConfigOptions]] entry).
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
      val configs = admin.describeClusterConfigs().get().asScala

      if (configKeys.isEmpty) {
        configs.map(buildRow).toArray
      } else {
        val configEntryMap = configs.map(e => e.key() -> e).toMap
        configKeys.flatMap(key => configEntryMap.get(key).map(buildRow))
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to get cluster config: ${e.getMessage}", e)
    }
  }

  private def buildRow(entry: ConfigEntry): InternalRow = {
    val resolvedValue = if (entry.value() != null) entry.value() else null
    val (defaultValue, isDefault, description) = resolveConfigMeta(entry.key(), resolvedValue)
    newInternalRow(
      UTF8String.fromString(entry.key()),
      if (resolvedValue != null) UTF8String.fromString(resolvedValue) else null,
      UTF8String.fromString(formatConfigSource(entry.source())),
      if (defaultValue != null) UTF8String.fromString(defaultValue) else null,
      Boolean.box(isDefault),
      if (description != null) UTF8String.fromString(description) else null
    )
  }

  /**
   * Resolves the declared metadata for a config key from the [[ConfigOptions]] registry.
   *
   * @param key
   *   the config key
   * @param resolvedValue
   *   the resolved value from the cluster, used to determine whether the option is at its default
   * @return
   *   a triple of (default value as string or null, whether the option is at its default, the
   *   option description or null). When the key is not a well-known option, the default value and
   *   description are null and the option is reported as being at its default.
   */
  private def resolveConfigMeta(key: String, resolvedValue: String): (String, Boolean, String) = {
    val option = GetClusterConfigsProcedure.CONFIG_OPTIONS_MAP.get(key)
    option match {
      case Some(configOption) =>
        val defaultValue = GetClusterConfigsProcedure.formatDefaultValue(configOption)
        val description = configOption.description()
        val isDefault =
          if (defaultValue == null) {
            true
          } else {
            defaultValue == resolvedValue
          }
        val resolvedDescription =
          if (description == null || description.isEmpty) null else description
        (defaultValue, isDefault, resolvedDescription)
      case None =>
        (null, true, null)
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
    "Retrieve cluster configuration values with default value, override status and description."
  }
}

object GetClusterConfigsProcedure {

  private val PARAMETERS: Array[ProcedureParameter] = Array(
    ProcedureParameter.optional("config_keys", DataTypes.createArrayType(DataTypes.StringType))
  )

  private val OUTPUT_TYPE: StructType = new StructType(
    Array(
      new StructField("config_key", DataTypes.StringType, nullable = false, Metadata.empty),
      new StructField("config_value", DataTypes.StringType, nullable = true, Metadata.empty),
      new StructField("config_source", DataTypes.StringType, nullable = false, Metadata.empty),
      new StructField("default_value", DataTypes.StringType, nullable = true, Metadata.empty),
      new StructField("is_default", BooleanType, nullable = false, Metadata.empty),
      new StructField("description", DataTypes.StringType, nullable = true, Metadata.empty)
    )
  )

  /**
   * Lazily builds a map from config key to its [[ConfigOption]] by reflection over the static
   * fields of [[ConfigOptions]]. This is used to enrich the procedure output with the declared
   * default value and description.
   */
  lazy val CONFIG_OPTIONS_MAP: Map[String, ConfigOption[_]] = {
    val map = mutable.Map[String, ConfigOption[_]]()
    val fields = classOf[ConfigOptions].getDeclaredFields
    fields.foreach {
      field =>
        field.setAccessible(true)
        if (classOf[ConfigOption[_]].isAssignableFrom(field.getType)) {
          field.get(null) match {
            case option: ConfigOption[_] => map.put(option.key(), option)
            case _ =>
          }
        }
    }
    map.toMap
  }

  /** Formats a config option's default value as a user-facing string. */
  def formatDefaultValue(option: ConfigOption[_]): String = {
    val value = option.defaultValue()
    if (value == null) {
      return null
    }
    value match {
      case duration: Duration => formatDuration(duration)
      case memorySize: MemorySize => memorySize.toString
      case other => other.toString
    }
  }

  private def formatDuration(duration: Duration): String = {
    val seconds = duration.getSeconds
    if (seconds == 0) {
      "0 s"
    } else if (seconds >= 3600 && seconds % 3600 == 0) {
      (seconds / 3600) + " hours"
    } else if (seconds >= 60 && seconds % 60 == 0) {
      (seconds / 60) + " min"
    } else {
      seconds + " s"
    }
  }

  def builder(): ProcedureBuilder = {
    new BaseProcedure.Builder[GetClusterConfigsProcedure]() {
      override protected def doBuild(): GetClusterConfigsProcedure = {
        new GetClusterConfigsProcedure(getTableCatalog)
      }
    }
  }
}
