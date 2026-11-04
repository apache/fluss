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
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.spark.SparkFlussConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.time.{Duration, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object FlussOffsetInitializers extends Logging {

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
   *
   * An explicitly set but blank start timestamp fails fast instead of silently falling back to a
   * full-table batch read.
   */
  def isIncrementalRead(options: CaseInsensitiveStringMap): Boolean = {
    val startOption = SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP
    val rawValue = Option(options.get(startOption.key()))
    if (rawValue.exists(_.trim.isEmpty)) {
      throw new IllegalArgumentException(
        s"'${startOption.key()}' must not be blank. Provide epoch milliseconds or a " +
          s"'yyyy-MM-dd HH:mm:ss' timestamp, or omit the option for a full-table batch read.")
    }
    rawValue.isDefined
  }

  /**
   * Start offsets of an incremental batch read, resolved from `scan.incremental.start.timestamp`.
   * Requires that option to be set.
   */
  def incrementalStartOffsetsInitializer(options: CaseInsensitiveStringMap): OffsetsInitializer = {
    val start = requiredTimestamp(options, SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP)
    OffsetsInitializer.timestamp(
      parseTimestamp(start, SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()))
  }

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
    } else {
      if (!isIncrementalRead(options)) {
        val endKey = SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()
        if (Option(options.get(endKey)).isDefined) {
          throw new IllegalArgumentException(
            s"'$endKey' is set but " +
              s"'${SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()}' is missing. An end " +
              s"timestamp alone cannot truncate a batch read; set a start timestamp for an " +
              s"incremental read, or remove the end option.")
        }
      }
      // A batch read stops at the latest committed data, an incremental one included: its end bound
      // is applied by the reader on the record commit timestamp (see [[incrementalTimeRange]]).
      OffsetsInitializer.latest()
    }
  }

  /** The `[start, end)` window of an incremental read, empty for a plain batch read. */
  def incrementalTimeRange(options: CaseInsensitiveStringMap): Option[FlussTimeRange] = {
    if (!isIncrementalRead(options)) {
      return None
    }
    val start = requiredTimestamp(options, SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP)
    val startMs = parseTimestamp(start, SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key())
    val end = incrementalOption(options, SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP)
      .map(_.trim)
    val endMs = end
      .map(parseTimestamp(_, SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()))
      .getOrElse(Long.MaxValue)
    end.foreach(requireValidWindow(start, _, startMs, endMs))
    Some(FlussTimeRange(startMs, endMs))
  }

  /** Rejects a reversed or degenerate window instead of silently reading nothing. */
  private[spark] def requireValidWindow(
      start: String,
      end: String,
      startMs: Long,
      endMs: Long): Unit = {
    if (startMs >= endMs) {
      throw new IllegalArgumentException(
        s"Invalid incremental time range: the start timestamp '$start' must be strictly before " +
          s"the end timestamp '$end'. The window is left-closed right-open '[start, end)'.")
    }
  }

  /**
   * Warns when the requested window starts before what the table is still guaranteed to retain: the
   * log below `table.log.ttl` has been dropped, so the read can only return a subset of the window.
   *
   * Stays a warning rather than an error: TTL is a lower bound — expired segments are deleted
   * lazily — so the data may in fact still be there, and a partial window is the documented
   * behavior of this read mode.
   */
  def warnIfWindowPredatesRetention(
      tablePath: TablePath,
      range: FlussTimeRange,
      logTtlMs: Long): Unit = {
    if (predatesRetention(range.startMs, logTtlMs, System.currentTimeMillis())) {
      logWarning(
        s"Incremental read of $tablePath starts at ${range.startMs}, which is earlier than the " +
          s"table is guaranteed to retain (table.log.ttl = ${Duration.ofMillis(logTtlMs)}). " +
          s"Records that already expired cannot be returned, so the result may be a subset of " +
          s"the requested window. Increase table.log.ttl or move the window forward.")
    }
  }

  private[spark] def predatesRetention(startMs: Long, logTtlMs: Long, nowMs: Long): Boolean =
    startMs < nowMs - logTtlMs

  /**
   * Reads a `scan.incremental.*` option from the scan options, falling back to its default. A blank
   * value counts as unset; a blank start timestamp is rejected by [[isIncrementalRead]] before it
   * can reach here.
   */
  private def incrementalOption(
      options: CaseInsensitiveStringMap,
      option: ConfigOption[String]): Option[String] =
    Option(options.getOrDefault(option.key(), option.defaultValue())).filter(_.trim.nonEmpty)

  private def resolveStartupMode(
      options: CaseInsensitiveStringMap,
      flussConfig: Configuration): String =
    options.getOrDefault(
      SparkFlussConf.SCAN_START_UP_MODE.key(),
      flussConfig.get(SparkFlussConf.SCAN_START_UP_MODE))

  private def requiredTimestamp(
      options: CaseInsensitiveStringMap,
      option: ConfigOption[String]): String = {
    val value = incrementalOption(options, option)
    if (value.getOrElse("").isEmpty) {
      throw new IllegalArgumentException(
        s"'${option.key()}' must not be empty. Provide epoch milliseconds or a " +
          s"'yyyy-MM-dd HH:mm:ss' timestamp.")
    }
    value.get.trim
  }

  /**
   * Parses a timestamp option value to epoch milliseconds: a purely numeric string is epoch
   * milliseconds, otherwise it is parsed as 'yyyy-MM-dd HH:mm:ss' in the Spark session time zone.
   */
  private[spark] def parseTimestamp(timestampStr: String, optionKey: String): Long = {
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
