/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.spark
import com.alibaba.fluss.client.table.scanner.ScanRecord
import com.alibaba.fluss.client.table.scanner.log.{LogScanner, ScanRecords}
import com.alibaba.fluss.exception.PartitionNotExistException
import com.alibaba.fluss.utils.ExceptionUtils

import org.apache.spark.{sql, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.fluss.FetchedDataPool
import org.apache.spark.sql.fluss.FetchedDataPool.{CacheKey, UNKNOWN_OFFSET}

import java.{util => ju}
import java.time.Duration
import java.util.concurrent.TimeoutException

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class helps caller to read from Fluss leveraging logScanner as well as fetched data pool.
 */
private class FlussDataConsumer(
    tableBucketInfo: TableBucketInfo,
    logScanner: LogScanner,
    fetchedDataPool: FetchedDataPool)
  extends Logging
  with AutoCloseable {

  override def close(): Unit = {
    logScanner.close()
    releaseFetchedData()
  }

  // Exposed for testing
  @volatile private var _fetchedData: Option[FetchedData] = None

  private val cacheKey = CacheKey(tableBucketInfo)

  /**
   * The fetched record returned from the `fetchRecord` method. This is a reusable private object to
   * avoid memory allocation.
   */
  private val fetchedRecord: FetchedRecord = FetchedRecord(null, UNKNOWN_OFFSET)

  /**
   * Get the record for the given offset if available. When this method reaches `untilOffset` and
   * still can't find an available record, it will return `null`.
   *
   * @param offset
   *   the offset to fetch.
   * @param untilOffset
   *   the max offset to fetch. Exclusive.
   * @param pollTimeoutMs
   *   timeout in milliseconds to poll data from Fluss.
   */
  def get(offset: Long, untilOffset: Long, pollTimeoutMs: Long): ScanRecord = {
    require(
      offset < untilOffset,
      s"offset must always be less than untilOffset [offset: $offset, untilOffset: $untilOffset]")

    val fetchedData = getOrRetrieveFetchedData(offset)

    logDebug(
      s"Get $tableBucketInfo nextOffset ${fetchedData.nextOffsetInFetchedData} " +
        s"requested $offset")

    // we will try to fetch the record at `offset`, if the record does not exist, we will
    // try to fetch next available record within [offset, untilOffset).
    var toFetchOffset = offset
    var fetchedRecord: FetchedRecord = null
    // We want to break out of the while loop on a successful fetch to avoid using "return"
    // which may cause a NonLocalReturnControl exception when this method is used as a function.
    var isFetchComplete = false

    while (toFetchOffset != UNKNOWN_OFFSET && !isFetchComplete) {
      try {
        fetchedRecord =
          fetchRecord(logScanner, fetchedData, toFetchOffset, untilOffset, pollTimeoutMs)
        if (fetchedRecord.record != null) {
          isFetchComplete = true
        } else {
          toFetchOffset = fetchedRecord.nextOffsetToFetch
          if (toFetchOffset >= untilOffset) {
            fetchedData.reset()
            toFetchOffset = UNKNOWN_OFFSET
          } else {
            logDebug(s"Skipped offsets [$offset, $toFetchOffset]")
          }
        }
      } catch {
        case e: Exception =>
          logError(s"Error occurred when fetching record at offset $offset", e)
      }
    }

    if (isFetchComplete) {
      fetchedRecord.record
    } else {
      fetchedData.reset()
      null
    }
  }

  /**
   * Get the fetched record for the given offset if available. thismethod will return `null` if the
   * next available record is within [offset, untilOffset).
   *
   * @throws OffsetOutOfRangeException
   *   if `offset` is out of range
   * @throws TimeoutException
   *   if cannot fetch the record in `pollTimeoutMs` milliseconds.
   */
  private def fetchRecord(
      consumer: LogScanner,
      fetchedData: FetchedData,
      offset: Long,
      untilOffset: Long,
      pollTimeoutMs: Long): FetchedRecord = {
    if (offset != fetchedData.nextOffsetInFetchedData) {
      // This is the first fetch, or the fetched data has been reset.
      // Fetch records from Fluss and update `fetchedData`.
      fetchData(consumer, fetchedData, offset, pollTimeoutMs)
    } else if (!fetchedData.hasNext) { // The last pre-fetched data has been drained.
      if (offset < fetchedData.offsetAfterPoll) {
        // Offsets in [offset, fetchedData.offsetAfterPoll) are invisible. Return a record to ask
        // the next call to start from `fetchedData.offsetAfterPoll`.
        val nextOffsetToFetch = fetchedData.offsetAfterPoll
        fetchedData.reset()
        return fetchedRecord.withRecord(null, nextOffsetToFetch)
      } else {
        // Fetch records from Fluss and update `fetchedData`.
        fetchData(consumer, fetchedData, offset, pollTimeoutMs)
      }
    }

    if (!fetchedData.hasNext) {
      // When we reach here, we have already tried to poll from Fluss. As `fetchedData` is still
      // empty, all messages in [offset, fetchedData.offsetAfterPoll) are invisible. Return a
      // record to ask the next call to start from `fetchedData.offsetAfterPoll`.
      assert(
        offset <= fetchedData.offsetAfterPoll,
        s"seek to $offset and poll but the offset was reset to ${fetchedData.offsetAfterPoll}")
      fetchedRecord.withRecord(null, fetchedData.offsetAfterPoll)
    } else {
      val record = fetchedData.next()
      // In general, FLuss uses the specified offset as the start point, and tries to fetch the next
      // available offset. Hence we need to handle offset mismatch.
      if (record.logOffset() > offset) {
        // This may happen when some records aged out but their offsets already got verified
        if (record.logOffset() >= untilOffset) {
          // After processing a record with offset of "stoppingOffset - 1", the we
          // should not continue fetching because the record with stoppingOffset may not
          // exist. Keep polling will just block forever
          // Set `nextOffsetToFetch` to `untilOffset` to finish the current batch.
          fetchedRecord.withRecord(null, untilOffset)
        } else {
          fetchedRecord.withRecord(record, fetchedData.nextOffsetInFetchedData)
        }
      } else if (record.logOffset() < offset) {
        // This should not happen. If it does happen, then we probably misunderstand Fluss internal
        // mechanism.
        throw new IllegalStateException(
          s"Tried to fetch $offset but the returned record offset was ${record.logOffset()}")
      } else {
        fetchedRecord.withRecord(record, fetchedData.nextOffsetInFetchedData)
      }
    }
  }

  /**
   * Poll messages from Fluss starting from `offset` and update `fetchedData`.
   *
   * @throws OffsetOutOfRangeException
   *   if `offset` is out of range.
   * @throws TimeoutException
   *   if the consumer position is not changed after polling. It means the consumer polls nothing
   *   before timeout.
   */
  private def fetchData(
      logScanner: LogScanner,
      fetchedData: FetchedData,
      offset: Long,
      pollTimeoutMs: Long): Unit = {
    subscribeLog(logScanner, offset)
    var scanRecords = logScanner.poll(Duration.ofMillis(pollTimeoutMs))
    var records = scanRecords.records(tableBucketInfo.getTableBucket)
    if (records.isEmpty) {
      // fetch twice
      scanRecords = logScanner.poll(Duration.ofMillis(pollTimeoutMs))
      records = scanRecords.records(tableBucketInfo.getTableBucket)
      if (records.isEmpty) {
        throw new TimeoutException(
          s"Cannot fetch record at offset $offset within $pollTimeoutMs ms")
      }
    }
    val offsetAfterPoll = records.get(records.size() - 1).logOffset()
    fetchedData.withNewPoll(records.listIterator, offsetAfterPoll)
  }

  private def subscribeLog(logScanner: LogScanner, offset: Long): Unit = {
    val partitionId = tableBucketInfo.getTableBucket.getPartitionId
    if (partitionId != null) {
      try {
        logScanner.subscribe(partitionId, tableBucketInfo.getTableBucket.getBucket, offset)

      } catch {
        case e: Exception => {
          // the PartitionNotExistException may still happens when partition is removed
          // Traverse the exception chain to check for PartitionNotExistException.
          val partitionNotExist =
            ExceptionUtils.findThrowable(e, classOf[PartitionNotExistException]).isPresent()
          if (partitionNotExist) {
            logWarning(
              s"Partition $partitionId does not exist when subscribing. Skipping subscription."
            )
            return
          }
        }
      }
    } else {
      logScanner.subscribe(tableBucketInfo.getTableBucket.getBucket, offset)

    }
  }
  private def getOrRetrieveFetchedData(offset: Long): FetchedData = _fetchedData match {
    case None =>
      _fetchedData = Option(fetchedDataPool.acquire(cacheKey, offset))
      require(_fetchedData.isDefined, "acquiring fetched data from cache must always succeed.")
      _fetchedData.get

    case Some(fetchedData) => fetchedData
  }

  private def releaseFetchedData(): Unit = {
    if (_fetchedData.isDefined) {
      fetchedDataPool.release(cacheKey, _fetchedData.get)
      _fetchedData = None
    }
  }

}

private object FlussDataConsumer extends Logging {

  private val sparkConf = SparkEnv.get.conf
  private val fetchedDataPool = new FetchedDataPool(sparkConf)

  Runtime.getRuntime.addShutdownHook {
    new Thread(
      () =>
        try fetchedDataPool.shutdown()
        catch {
          case e: Throwable =>
            logWarning("Ignoring Exception while shutting down pools from shutdown hook", e)
        })
  }

  def acquire(tableBucketInfo: TableBucketInfo, logScanner: LogScanner): FlussDataConsumer = {
    if (TaskContext.get != null && TaskContext.get.attemptNumber >= 1) {
      val cacheKey = new CacheKey(tableBucketInfo)

      // invalidate all fetched data for the key as well
      // sadly we can't pinpoint specific data and invalidate cause we don't have unique id
      fetchedDataPool.invalidate(cacheKey)
    }

    new FlussDataConsumer(tableBucketInfo, logScanner, fetchedDataPool)
  }

}

/**
 * The internal object to store the fetched data from Fluss consumer and the next offset to poll.
 *
 * @param _records
 *   the pre-fetched Fluss records.
 * @param _nextOffsetInFetchedData
 *   the next offset in `records`. We use this to verify if we should check if the pre-fetched data
 *   is still valid.
 * @param _offsetAfterPoll
 *   the Fluss offset after calling `poll`. We will use this offset to poll when `records` is
 *   drained.
 */
case class FetchedData(
    private var _records: ju.ListIterator[ScanRecord],
    private var _nextOffsetInFetchedData: Long,
    private var _offsetAfterPoll: Long) {

  def withNewPoll(records: ju.ListIterator[ScanRecord], offsetAfterPoll: Long): FetchedData = {
    this._records = records
    this._nextOffsetInFetchedData = UNKNOWN_OFFSET
    this._offsetAfterPoll = offsetAfterPoll
    this
  }

  /** Whether there are more elements */
  def hasNext: Boolean = _records.hasNext

  /** Move `records` forward and return the next record. */
  def next(): ScanRecord = {
    val record = _records.next()
    _nextOffsetInFetchedData = record.logOffset() + 1
    record
  }

  /** Move `records` backward and return the previous record. */
  def previous(): ScanRecord = {
    assert(_records.hasPrevious, "fetchedData cannot move back")
    val record = _records.previous()
    _nextOffsetInFetchedData = record.logOffset()
    record
  }

  /** Reset the internal pre-fetched data. */
  def reset(): Unit = {
    _records = ju.Collections.emptyListIterator()
    _nextOffsetInFetchedData = UNKNOWN_OFFSET
    _offsetAfterPoll = UNKNOWN_OFFSET
  }

  /**
   * Returns the next offset in `records`. We use this to verify if we should check if the
   * pre-fetched data is still valid.
   */
  def nextOffsetInFetchedData: Long = _nextOffsetInFetchedData

  /** Returns the next offset to poll after draining the pre-fetched records. */
  def offsetAfterPoll: Long = _offsetAfterPoll

}

/** The internal object returned by the `fetchRecord` method. */
private case class FetchedRecord(var record: ScanRecord, var nextOffsetToFetch: Long) {

  def withRecord(record: ScanRecord, nextOffsetToFetch: Long): FetchedRecord = {
    this.record = record
    this.nextOffsetToFetch = nextOffsetToFetch
    this
  }
}
