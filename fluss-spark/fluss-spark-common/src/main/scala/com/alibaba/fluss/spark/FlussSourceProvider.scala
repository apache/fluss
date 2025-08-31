/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.spark

import com.alibaba.fluss.client.ConnectionFactory
import com.alibaba.fluss.config.Configuration
import com.alibaba.fluss.metadata.{TableInfo, TablePath}
import com.alibaba.fluss.spark.utils.SparkConversions.toFlussClientConfig
import com.alibaba.fluss.spark.utils.SparkTypeUtils

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SaveMode => SparkSaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

class FlussSourceProvider
  extends DataSourceRegister
  with SessionConfigSupport
  with CreatableRelationProvider {

  override def shortName(): String = {
    FlussSourceProvider.NAME
  }

  override def keyPrefix(): String = FlussSourceProvider.NAME

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // ignore schema.
    // getTable will get schema by itself.
    null
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    // ignore partition.
    // getTable will get partition by itself.
    null
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: JMap[String, String]): Table = {
    val (flussConfig, tableInfo) = loadTable(properties)
    SparkTable(null, flussConfig, tableInfo)
  }
  override def createRelation(
      sqlContext: SQLContext,
      mode: SparkSaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val (flussConfig, tableInfo) = loadTable(parameters.asJava)
    FlussSourceProvider.toBaseRelation(tableInfo, sqlContext)
  }

  private def loadTable(options: JMap[String, String]): (Configuration, TableInfo) = {
    val database = options.get(FlussSourceProvider.DATABASE)
    val table = options.get(FlussSourceProvider.TABLE)
    val configuration = toFlussClientConfig(new CaseInsensitiveStringMap(options))
    val connection = ConnectionFactory.createConnection(configuration)
    val admin = connection.getAdmin
    val tableInfo = admin.getTableInfo(new TablePath(database, table)).get()
    (configuration, tableInfo)

  }

}

object FlussSourceProvider {

  val NAME = "fluss"
  val DATABASE = "database"
  val TABLE = "table"

  def toBaseRelation(table: TableInfo, _sqlContext: SQLContext): BaseRelation = {
    new BaseRelation {
      override def sqlContext: SQLContext = _sqlContext
      override def schema: StructType = SparkTypeUtils.fromFlussRowType(table.getRowType)
    }
  }
}
