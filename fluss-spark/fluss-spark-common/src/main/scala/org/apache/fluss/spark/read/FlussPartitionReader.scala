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

package org.apache.fluss.spark.read

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.ScanRecord
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{TableInfo, TablePath}
import org.apache.fluss.row.{InternalRow => FlussInternalRow}
import org.apache.fluss.spark.row.DataConverter
import org.apache.fluss.types.RowType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.time.Duration

abstract class FlussPartitionReader(tablePath: TablePath, flussConfig: Configuration)
  extends PartitionReader[InternalRow] {

  protected val POLL_TIMEOUT: Duration = Duration.ofMillis(100)
  protected lazy val conn: Connection = ConnectionFactory.createConnection(flussConfig)
  protected lazy val table: Table = conn.getTable(tablePath)
  protected lazy val tableInfo: TableInfo = table.getTableInfo
  protected val rowType: RowType = tableInfo.getRowType

  protected var currentRow: InternalRow = _
  protected var closed = false

  override def get(): InternalRow = currentRow

  def close0(): Unit

  override def close(): Unit = {
    if (!closed) {
      closed = true
      close0()

      if (table != null) {
        table.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  protected def convertToSparkRow(scanRecord: ScanRecord): InternalRow = {
    convertToSparkRow(scanRecord.getRow)
  }

  protected def convertToSparkRow(flussRow: FlussInternalRow): InternalRow = {
    DataConverter.toSparkInternalRow(flussRow, rowType)
  }
}
