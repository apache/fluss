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

package org.apache.fluss.spark.utils

import org.apache.fluss.client.table.scanner.ScanRecord
import org.apache.fluss.client.utils.SingleElementHeadIterator
import org.apache.fluss.record.ChangeType
import org.apache.fluss.row.{InternalRow, KeyValueRow, ProjectedRow}
import org.apache.fluss.utils.CloseableIterator

import java.util.Comparator

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * An iterator that processes log records and merges changes for rows with the same primary key.
 *
 * This iterator sorts log records by primary key and then by offset, groups records with the same
 * primary key together, and merges them according to change type semantics. The merging logic
 * follows these rules:
 *   - INSERT: replaces previous state
 *   - DELETE: removes the record
 *   - UPDATE_BEFORE: ignored during merge
 *   - UPDATE_AFTER: replaces previous state
 *
 * The iterator outputs [[KeyValueRow]] instances representing the final state after merging all
 * changes for each primary key.
 */
case class LogChangesIterator(
    logRecords: Array[ScanRecord],
    pkProjection: Array[Int],
    comparator: Comparator[InternalRow]
) extends CloseableIterator[KeyValueRow] {

  // Sort the records by primary key and then by offset
  private val sortedLogRecords = logRecords.sortWith {
    case (record1, record2) =>
      val keyComparison = comparator.compare(record1.getRow, record2.getRow)
      if (keyComparison == 0) {
        record1.logOffset() < record2.logOffset() // For same key, lower offset comes first
      } else {
        keyComparison < 0
      }
  }

  private var recordsIterator = SingleElementHeadIterator.addElementToHead(
    sortedLogRecords.head,
    CloseableIterator.wrap(sortedLogRecords.tail.toIterator.asJava))

  private val projectRow1 = ProjectedRow.from(pkProjection)
  private val projectRow2 = ProjectedRow.from(pkProjection)

  private var currentScanRecord: ScanRecord = _

  override def hasNext: Boolean = {
    if (currentScanRecord != null) {
      return true
    }
    if (!recordsIterator.hasNext) {
      return false
    }

    val recordsWithSamePK = mutable.ArrayBuffer(recordsIterator.next())
    val current = recordsWithSamePK.head

    var continue = true
    while (continue && recordsIterator.hasNext) {
      val next = recordsIterator.next()
      if (hasSamePrimaryKey(current, next)) {
        recordsWithSamePK.append(next)
      } else {
        recordsIterator = SingleElementHeadIterator.addElementToHead(next, recordsIterator)
        continue = false
      }
    }

    mergeLogRecordsWithSamePK(recordsWithSamePK.toArray) match {
      case Some(record) =>
        currentScanRecord = record
        true
      case None =>
        hasNext
    }
  }

  override def next(): KeyValueRow = {
    assert(currentScanRecord != null)

    val result = new KeyValueRow(
      pkProjection,
      currentScanRecord.getRow,
      isDelete(currentScanRecord.getChangeType)
    )
    currentScanRecord = null
    result
  }

  private def hasSamePrimaryKey(s1: ScanRecord, s2: ScanRecord): Boolean = {
    comparator.compare(
      projectRow1.replaceRow(s1.getRow),
      projectRow2.replaceRow(s2.getRow)
    ) == 0
  }

  private def mergeLogRecordsWithSamePK(logRecords: Array[ScanRecord]): Option[ScanRecord] = {
    var result: Option[ScanRecord] = Some(logRecords.head)
    logRecords.tail.foreach {
      record =>
        record.getChangeType match {
          case ChangeType.INSERT =>
            result = Some(record)
          case ChangeType.DELETE =>
            result = None
          case ChangeType.UPDATE_BEFORE =>
          // Ignore
          case ChangeType.UPDATE_AFTER =>
            result = Some(record)
          case _ =>
            throw new IllegalArgumentException(s"Unknown change type: ${record.getChangeType}")
        }
    }
    result
  }

  private def isDelete(changeType: ChangeType): Boolean = {
    changeType == ChangeType.DELETE || changeType == ChangeType.UPDATE_BEFORE
  }

  override def close(): Unit = {}
}
