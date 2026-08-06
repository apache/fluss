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

import org.apache.fluss.client.initializer.{NoStoppingOffsetsInitializer, OffsetsInitializer}
import org.apache.fluss.config.{ConfigOption, Configuration}
import org.apache.fluss.spark.SparkFlussConf

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object FlussOffsetInitializers {

  private val DATE_TIME_FORMATTER: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /**
   * Whether an incremental (time-range) batch read is requested, i.e.
   * `scan.incremental.start.timestamp` is set on the relation being scanned.
   *
   * The `scan.incremental.*` options are read from the per-query scan options only — set by the
   * `fluss_incremental_between_timestamp` table-valued function or `DataFrameReader.option` — and
   * deliberately not from session configuration, so a window can never leak into another query.
   * Streaming reads ignore them.
   */
  def isIncrementalRead(options: CaseInsensitiveStringMap): Boolean = {
    incrementalOption(options, SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP).isDefined
  }

  /**
   * Whether a resolved start offset predates the data Fluss still retains for a bucket. A bucket
   * whose earliest offset is still 0 has dropped nothing and is never flagged.
   */
  def isBeforeRetention(startOffset: Long, earliestOffset: Long): Boolean =
    earliestOffset > 0 && startOffset <= earliestOffset

  /**
   * Rejects a start offset that predates the earliest retained data (see [[isBeforeRetention]]), so
   * a truncated window is never returned silently. Callers must pass a concrete earliest offset,
   * i.e. from a retriever built with `fetchEarliestOffset = true`.
   */
  def requireStartWithinRetention(
      tableDescription: String,
      partitionName: String,
      bucketId: Int,
      startOffset: Long,
      earliestOffset: Long): Unit = {
    if (isBeforeRetention(startOffset, earliestOffset)) {
      val partitionDesc = if (partitionName != null) s" partition '$partitionName'" else ""
      throw new IllegalArgumentException(
        s"The requested start timestamp resolves to log offset $startOffset for bucket " +
          s"$bucketId$partitionDesc of table $tableDescription, which is at or before the " +
          s"earliest retained offset $earliestOffset. The requested time range exceeds Fluss " +
          s"retention (table.log.ttl); narrow the time range or increase table.log.ttl.")
    }
  }

  /**
   * Whether a start timestamp preceding the earliest retained data fails fast (default) instead of
   * being clamped to that offset. Controlled by `scan.incremental.timestamp.out-of-range`.
   */
  def failOnTimestampOutOfRange(options: CaseInsensitiveStringMap): Boolean = {
    val mode =
      incrementalOption(
        options,
        SparkFlussConf.SCAN_INCREMENTAL_TIMESTAMP_OUT_OF_RANGE).get.toUpperCase
    SparkFlussConf.TimestampOutOfRangeMode.withName(mode) ==
      SparkFlussConf.TimestampOutOfRangeMode.ERROR
  }

  /**
   * Start offsets of an incremental batch read, resolved from `scan.incremental.start.timestamp`.
   * Requires that option to be set.
   */
  def incrementalStartOffsetsInitializer(options: CaseInsensitiveStringMap): OffsetsInitializer =
    OffsetsInitializer.timestamp(
      requiredTimestamp(options, SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP))

  /**
   * Start offsets of a streaming read, driven by `scan.startup.mode`. Batch reads ignore this
   * option (see [[incrementalStartOffsetsInitializer]]).
   */
  def startOffsetsInitializer(
      options: CaseInsensitiveStringMap,
      flussConfig: Configuration): OffsetsInitializer = {
    val startupMode = resolveStartupMode(options, flussConfig).toUpperCase

    SparkFlussConf.StartUpMode.withName(startupMode) match {
      case SparkFlussConf.StartUpMode.EARLIEST => OffsetsInitializer.earliest()
      case SparkFlussConf.StartUpMode.FULL => OffsetsInitializer.full()
      case SparkFlussConf.StartUpMode.LATEST => OffsetsInitializer.latest()
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported scan start up mode: " +
            s"${resolveStartupMode(options, flussConfig)}. Supported values are 'full', " +
            s"'earliest' and 'latest'. For a time-range batch read set " +
            s"'${SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()}' instead.")
    }
  }

  def stoppingOffsetsInitializer(
      isBatch: Boolean,
      options: CaseInsensitiveStringMap): OffsetsInitializer = {
    if (!isBatch) {
      new NoStoppingOffsetsInitializer()
    } else if (!isIncrementalRead(options)) {
      // A plain batch read stops at the latest committed data; an end timestamp alone must not
      // truncate it.
      OffsetsInitializer.latest()
    } else {
      val end =
        incrementalOption(options, SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP).getOrElse("").trim
      if (
        end.isEmpty ||
        end.equalsIgnoreCase(SparkFlussConf.END_TIMESTAMP_LATEST)
      ) {
        OffsetsInitializer.latest()
      } else {
        OffsetsInitializer.timestamp(
          parseTimestamp(end.trim, SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()))
      }
    }
  }

  /** Reads a `scan.incremental.*` option from the scan options, falling back to its default. */
  private def incrementalOption(
      options: CaseInsensitiveStringMap,
      option: ConfigOption[String]): Option[String] =
    Option(options.getOrDefault(option.key(), option.defaultValue()))

  private def resolveStartupMode(
      options: CaseInsensitiveStringMap,
      flussConfig: Configuration): String =
    options.getOrDefault(
      SparkFlussConf.SCAN_START_UP_MODE.key(),
      flussConfig.get(SparkFlussConf.SCAN_START_UP_MODE))

  private def requiredTimestamp(
      options: CaseInsensitiveStringMap,
      option: ConfigOption[String]): Long = {
    val value = incrementalOption(options, option)
    if (value.getOrElse("").isEmpty) {
      throw new IllegalArgumentException(
        s"'${option.key()}' must not be empty. Provide epoch milliseconds or a " +
          s"'yyyy-MM-dd HH:mm:ss' timestamp.")
    }
    parseTimestamp(value.get.trim, option.key())
  }

  /**
   * Parses a timestamp option value to epoch milliseconds: a purely numeric string is epoch
   * milliseconds, otherwise it is parsed as 'yyyy-MM-dd HH:mm:ss' in the Spark session time zone.
   */
  private def parseTimestamp(timestampStr: String, optionKey: String): Long = {
    if (timestampStr.matches("\\d+")) {
      timestampStr.toLong
    } else {
      try {
        LocalDateTime
          .parse(timestampStr, DATE_TIME_FORMATTER)
          .atZone(ZoneId.of(SQLConf.get.sessionLocalTimeZone))
          .toInstant
          .toEpochMilli
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"Invalid value for '$optionKey': '$timestampStr'. It should be epoch milliseconds or " +
              s"follow the format 'yyyy-MM-dd HH:mm:ss', e.g. '2023-12-09 23:09:12' or " +
              s"'1678883047356'.",
            e
          )
      }
    }
  }
}
