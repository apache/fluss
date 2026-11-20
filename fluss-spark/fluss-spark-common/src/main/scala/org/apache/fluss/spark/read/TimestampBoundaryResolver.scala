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

import org.apache.fluss.client.admin.{Admin, OffsetSpec}
import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.scanner.log.LogScanner
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.metadata.{TableBucket, TableInfo}
import org.apache.fluss.spark.SparkFlussConf

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.time.Duration
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Resolves an exclusive commit-time boundary against a fixed committed prefix of the log. */
private[spark] object TimestampBoundaryResolver {

  def timestamp(
      options: CaseInsensitiveStringMap,
      config: Configuration,
      tableInfo: TableInfo): Option[Long] = {
    val key = SparkFlussConf.SCAN_AS_OF_TIMESTAMP.key()
    Option(options.get(key)).map {
      raw =>
        require(raw.trim.nonEmpty, s"'$key' must not be blank.")
        require(
          !options.containsKey(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()) &&
            !options.containsKey(SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()),
          "Snapshot-at-timestamp cannot be combined with incremental scan options."
        )
        require(
          !config.get(SparkFlussConf.READ_OPTIMIZED_OPTION) &&
            !options.getBoolean(SparkFlussConf.READ_OPTIMIZED_OPTION.key(), false),
          "Snapshot-at-timestamp cannot use read.optimized: complete log replay is required."
        )
        require(
          !tableInfo.isPartitioned,
          "Snapshot-at-timestamp does not support partitioned tables.")
        require(
          tableInfo.getTableConfig.isDataLakeEnabled,
          "Snapshot-at-timestamp requires a retained Paimon base; log-only recovery is unsupported.")
        require(
          !tableInfo.getTableConfig.getMergeEngineType.isPresent,
          "Snapshot-at-timestamp does not support custom Fluss merge engines.")
        require(
          !tableInfo.getTableConfig.getKvTTL.isPresent,
          "Snapshot-at-timestamp does not support implicit KV TTL expiration.")
        val value = FlussOffsetInitializers.parseTimestamp(raw.trim, key)
        require(
          value >= 0 && value <= System.currentTimeMillis(),
          s"Invalid '$key': $value must be a non-negative timestamp no later than query time.")
        value
    }
  }

  def resolve(
      admin: Admin,
      table: Table,
      tableInfo: TableInfo,
      timestamp: Long,
      config: Configuration): (Map[TableBucket, Long], Map[TableBucket, Long]) = {
    val buckets = (0 until tableInfo.getNumBuckets).map(Int.box).asJava
    val path = tableInfo.getTablePath
    val timeout = config.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT)
    def offsets(spec: OffsetSpec): Map[TableBucket, Long] =
      admin
        .listOffsets(path, buckets, spec)
        .all()
        .get(timeout.toMillis, TimeUnit.MILLISECONDS)
        .asScala
        .map {
          case (bucket, offset) =>
            new TableBucket(tableInfo.getTableId, bucket.intValue()) -> offset.longValue()
        }
        .toMap
    val earliest = offsets(new OffsetSpec.EarliestSpec())
    val high = offsets(new OffsetSpec.LatestSpec())
    // A floor time index for T can land inside the equal-T run. Searching T-1 guarantees a
    // starting point before the first T record, even on sparse remote indexes.
    val seek = offsets(new OffsetSpec.TimestampSpec(math.max(0L, timestamp - 1L)))
    val scanner = table.newScan().project(Array(0)).createLogScanner()
    try {
      (earliest, calibrate(scanner, earliest, high, seek, timestamp, timeout))
    } finally {
      scanner.close()
    }
  }

  private[read] def calibrate(
      scanner: LogScanner,
      earliest: Map[TableBucket, Long],
      high: Map[TableBucket, Long],
      seek: Map[TableBucket, Long],
      timestamp: Long,
      timeout: Duration): Map[TableBucket, Long] = {
    val pending = mutable.Set.empty[TableBucket]
    val boundary = mutable.Map.empty[TableBucket, Long]
    val firstRecord = mutable.Set.empty[TableBucket]
    high.foreach {
      case (bucket, stop) =>
        val start = seek(bucket)
        require(
          start >= earliest(bucket),
          s"Historical boundary expired for $bucket: seek=$start, earliest=${earliest(bucket)}")
        if (start >= stop) {
          require(
            stop == 0 || earliest(bucket) < stop,
            s"Cannot prove historical boundary for $bucket at T=$timestamp: no retained log prefix")
          boundary(bucket) = stop
        } else {
          scanner.subscribe(bucket.getBucket, start)
          pending += bucket
          firstRecord += bucket
        }
    }
    val deadline = System.nanoTime() + timeout.toNanos
    while (pending.nonEmpty) {
      if (Thread.currentThread().isInterrupted) {
        throw new InterruptedException("Historical timestamp boundary resolution cancelled")
      }
      if (System.nanoTime() >= deadline) {
        throw new IllegalStateException(
          s"Unable to confirm historical boundary T=$timestamp for $pending before timeout")
      }
      val records = scanner.poll(Duration.ofMillis(math.min(1000L, timeout.toMillis)))
      pending.toSeq.foreach {
        bucket =>
          val iterator = records.records(bucket).iterator()
          while (pending(bucket) && iterator.hasNext) {
            val record = iterator.next()
            if (
              firstRecord.remove(bucket) && earliest(bucket) > 0 && seek(bucket) == earliest(bucket)
              && record.timestamp() >= timestamp
            ) {
              throw new IllegalStateException(
                s"Cannot prove historical boundary for $bucket at T=$timestamp: earliest=${earliest(bucket)}")
            }
            if (record.logOffset() >= high(bucket) || record.timestamp() >= timestamp) {
              boundary(bucket) = math.min(record.logOffset(), high(bucket))
              pending -= bucket
            }
          }
          val consumed = records.consumedUpToOffset(bucket)
          if (pending(bucket) && consumed != null && consumed.longValue() >= high(bucket)) {
            if (firstRecord(bucket) && earliest(bucket) > 0 && seek(bucket) == earliest(bucket)) {
              throw new IllegalStateException(
                s"Cannot prove historical boundary for $bucket at T=$timestamp: retained tail has no timestamps")
            }
            boundary(bucket) = high(bucket)
            pending -= bucket
          }
          if (!pending(bucket)) {
            scanner.unsubscribe(bucket.getBucket)
          }
      }
    }
    boundary.toMap
  }
}
