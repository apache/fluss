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

import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.ScanRecord
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{TableInfo, TablePath}
import org.apache.fluss.row.{InternalRow => FlussInternalRow}
import org.apache.fluss.spark.SparkFlussConf
import org.apache.fluss.spark.row.DataConverter
import org.apache.fluss.spark.utils.FlussConnectionCache
import org.apache.fluss.types.RowType
import org.apache.fluss.utils.IOUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader

import java.time.Duration

abstract class FlussPartitionReader(
    tablePath: TablePath,
    flussConfig: Configuration,
    limit: Option[Int])
  extends PartitionReader[InternalRow]
  with Logging {

  protected val POLL_TIMEOUT: Duration =
    Duration.ofMillis(flussConfig.get(SparkFlussConf.SCAN_POLL_TIMEOUT).toMillis)
  private var connectionLease: FlussConnectionCache.Lease = _
  private var openedTable: Table = _

  protected var currentRow: InternalRow = _
  protected var closed = false
  protected var numRowsRead: Long = 0L

  protected lazy val table: Table = initializeResources {
    val (lease, acquiredTable) = FlussConnectionCache.acquireWithTable(flussConfig, tablePath)
    connectionLease = lease
    openedTable = acquiredTable
    openedTable
  }
  protected lazy val tableInfo: TableInfo = table.getTableInfo
  protected lazy val rowType: RowType = tableInfo.getRowType

  override def get(): InternalRow = currentRow

  override def currentMetricsValues(): Array[CustomTaskMetric] =
    Array(FlussNumRowsReadTaskMetric(numRowsRead))

  def next0(): Boolean

  override def next(): Boolean = {
    if (limit.exists(numRowsRead >= _)) {
      return false
    }
    val hasNext = next0()
    if (hasNext) {
      numRowsRead += 1
    }
    hasNext
  }

  def close0(): Unit

  override def close(): Unit = {
    if (!closed) {
      closed = true
      IOUtils.closeAll(() => close0(), openedTable, connectionLease)
    }
  }

  /** Releases resources if a reader fails before Spark can register its close callback. */
  protected def initializeResources[T](initialize: => T): T = {
    try {
      initialize
    } catch {
      case error: Throwable =>
        try {
          close()
        } catch {
          case closeError: Throwable =>
            if (closeError ne error) {
              error.addSuppressed(closeError)
            }
        }
        throw error
    }
  }

  protected def convertToSparkRow(scanRecord: ScanRecord): InternalRow = {
    convertToSparkRow(scanRecord.getRow)
  }

  protected def projectedRowType: RowType

  protected def convertToSparkRow(flussRow: FlussInternalRow): InternalRow = {
    DataConverter.toSparkInternalRow(flussRow, projectedRowType)
  }
}
