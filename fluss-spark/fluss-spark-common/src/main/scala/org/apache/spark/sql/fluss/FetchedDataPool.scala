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

package org.apache.spark.sql.fluss

import com.alibaba.fluss.client.table.scanner.ScanRecord
import com.alibaba.fluss.spark.{FetchedData, SparkConnectorOptions, TableBucketInfo}
import com.alibaba.fluss.utils.clock.{Clock, SystemClock}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

import java.{util => ju}
import java.time.Duration
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.LongAdder

import scala.collection.mutable

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Provides object pool for [[FetchedData]] which is grouped by [[CacheKey]].
 *
 * Along with CacheKey, it receives desired start offset to find cached FetchedData which may be
 * stored from previous batch. If it can't find one to match, it will create a new FetchedData. As
 * "desired start offset" plays as second level of key which can be modified in same instance, this
 * class cannot be replaced with general pool implementations including Apache Commons Pool.
 */
class FetchedDataPool(executorService: ScheduledExecutorService, clock: Clock, conf: SparkConf)
  extends Logging {
  import FetchedDataPool._

  def this(sparkConf: SparkConf) = {
    this(
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("fluss-fetched-data-cache-evictor"),
      SystemClock.getInstance(),
      sparkConf)
  }

  private val cache: mutable.Map[CacheKey, CachedFetchedDataList] = mutable.HashMap.empty

  private val minEvictableIdleTimeMillis =
    conf.get(SparkConnectorOptions.FETCHED_DATA_CACHE_TIMEOUT.key(), "300000").toLong
  private val evictorThreadRunIntervalMillis =
    conf
      .get(SparkConnectorOptions.FETCHED_DATA_CACHE_EVICTOR_THREAD_RUN_INTERVAL.key(), "60000")
      .toLong

  private def startEvictorThread(): Option[ScheduledFuture[_]] = {
    if (evictorThreadRunIntervalMillis > 0) {
      val future = executorService.scheduleAtFixedRate(
        () => {
          Utils.tryLogNonFatalError(removeIdleFetchedData())
        },
        0,
        evictorThreadRunIntervalMillis,
        TimeUnit.MILLISECONDS)
      Some(future)
    } else {
      None
    }
  }

  private var scheduled = startEvictorThread()

  private val numCreatedFetchedData = new LongAdder()
  private val numTotalElements = new LongAdder()

  def numCreated: Long = numCreatedFetchedData.sum()
  def numTotal: Long = numTotalElements.sum()

  def acquire(key: CacheKey, desiredStartOffset: Long): FetchedData = synchronized {
    val fetchedDataList = cache.getOrElseUpdate(key, new CachedFetchedDataList())

    val cachedFetchedDataOption = fetchedDataList.find {
      p => !p.inUse && p.getObject.nextOffsetInFetchedData == desiredStartOffset
    }

    var cachedFetchedData: CachedFetchedData = null
    if (cachedFetchedDataOption.isDefined) {
      cachedFetchedData = cachedFetchedDataOption.get
    } else {
      cachedFetchedData = CachedFetchedData.empty()
      fetchedDataList += cachedFetchedData

      numCreatedFetchedData.increment()
      numTotalElements.increment()
    }

    cachedFetchedData.lastAcquiredTimestamp = clock.milliseconds()
    cachedFetchedData.inUse = true

    cachedFetchedData.getObject
  }

  def invalidate(key: CacheKey): Unit = synchronized {
    cache.remove(key) match {
      case Some(lst) => numTotalElements.add(-1 * lst.size)
      case None =>
    }
  }

  def release(key: CacheKey, fetchedData: FetchedData): Unit = synchronized {
    def warnReleasedDataNotInPool(key: CacheKey, fetchedData: FetchedData): Unit = {
      logWarning(
        s"No matching data in pool for $fetchedData in key $key. " +
          "It might be released before, or it was not a part of pool.")
    }

    cache.get(key) match {
      case Some(fetchedDataList) =>
        val cachedFetchedDataOption = fetchedDataList.find {
          p => p.inUse && p.getObject == fetchedData
        }

        if (cachedFetchedDataOption.isEmpty) {
          warnReleasedDataNotInPool(key, fetchedData)
        } else {
          val cachedFetchedData = cachedFetchedDataOption.get
          cachedFetchedData.inUse = false
          cachedFetchedData.lastReleasedTimestamp = clock.milliseconds()
        }

      case None =>
        warnReleasedDataNotInPool(key, fetchedData)
    }
  }

  def shutdown(): Unit = {
    ThreadUtils.shutdown(executorService)
  }

  def reset(): Unit = synchronized {
    scheduled.foreach(_.cancel(true))

    cache.clear()
    numTotalElements.reset()
    numCreatedFetchedData.reset()

    scheduled = startEvictorThread()
  }

  private def removeIdleFetchedData(): Unit = synchronized {
    val now = clock.milliseconds()
    val maxAllowedReleasedTimestamp = now - minEvictableIdleTimeMillis
    cache.values.foreach {
      p: CachedFetchedDataList =>
        val expired = p.filter {
          q => !q.inUse && q.lastReleasedTimestamp < maxAllowedReleasedTimestamp
        }
        p --= expired
        numTotalElements.add(-1 * expired.size)
    }
  }
}
object FetchedDataPool {
  val UNKNOWN_OFFSET = -3L

  case class CacheKey(tableBucketInfo: TableBucketInfo) {}

  case class CachedFetchedData(fetchedData: FetchedData) {
    var lastReleasedTimestamp: Long = Long.MaxValue
    var lastAcquiredTimestamp: Long = Long.MinValue
    var inUse: Boolean = false

    def getObject: FetchedData = fetchedData
  }

  object CachedFetchedData {
    def empty(): CachedFetchedData = {
      val emptyData =
        FetchedData(ju.Collections.emptyListIterator[ScanRecord], UNKNOWN_OFFSET, UNKNOWN_OFFSET)

      CachedFetchedData(emptyData)
    }
  }

  type CachedFetchedDataList = mutable.ListBuffer[CachedFetchedData]
}
