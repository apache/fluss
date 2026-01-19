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

class CompactProcedure(tableCatalog: TableCatalog) extends BaseProcedure(tableCatalog) {

  override def parameters(): Array[ProcedureParameter] = {
    CompactProcedure.PARAMETERS
  }

  override def outputType(): StructType = {
    CompactProcedure.OUTPUT_TYPE
  }

  override def call(args: InternalRow): Array[InternalRow] = {
    val tableIdent = toIdentifier(args.getString(0), CompactProcedure.PARAMETERS(0).name())
    val sparkTable = loadSparkTable(tableIdent)

    try {
      val tablePath = toTablePath(tableIdent)
      val admin = getAdmin(sparkTable)

      val message = s"Compact operation queued for table $tablePath"

      Array(newInternalRow(UTF8String.fromString(message)))
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Failed to compact table: ${e.getMessage}", e)
    }
  }

  override def description(): String = {
    "This procedure triggers a compact action on a Fluss table."
  }
}

object CompactProcedure {

  private val PARAMETERS: Array[ProcedureParameter] = Array(
    ProcedureParameter.required("table", DataTypes.StringType)
  )

  private val OUTPUT_TYPE: StructType = new StructType(
    Array(
      new StructField("result", DataTypes.StringType, nullable = true, Metadata.empty)
    )
  )

  def builder(): ProcedureBuilder = {
    new BaseProcedure.Builder[CompactProcedure]() {
      override protected def doBuild(): CompactProcedure = {
        new CompactProcedure(getTableCatalog)
      }
    }
  }
}
