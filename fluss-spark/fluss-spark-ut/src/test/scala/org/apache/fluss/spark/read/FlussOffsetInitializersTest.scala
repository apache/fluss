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

import org.apache.fluss.config.Configuration
import org.apache.fluss.spark.SparkFlussConf

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for how scan options are resolved into offset initializers, and for the retention
 * guard of an incremental (time-range) read. The end-to-end behavior is covered by
 * [[org.apache.fluss.spark.SparkTimeRangeTvfTest]].
 */
class FlussOffsetInitializersTest extends AnyFunSuite {

  private def scanOptions(entries: (String, String)*): CaseInsensitiveStringMap = {
    val map = new java.util.HashMap[String, String]()
    entries.foreach { case (k, v) => map.put(k, v) }
    new CaseInsensitiveStringMap(map)
  }

  test("incremental read is enabled by the presence of a start timestamp") {
    val startKey = SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()
    assertThat(FlussOffsetInitializers.isIncrementalRead(scanOptions())).isFalse
    assertThat(
      FlussOffsetInitializers.isIncrementalRead(scanOptions(startKey -> "1767225600000"))).isTrue
  }

  test("a blank start timestamp fails fast instead of silently reading the full table") {
    val startKey = SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()
    val ex = intercept[IllegalArgumentException] {
      FlussOffsetInitializers.isIncrementalRead(scanOptions(startKey -> "  "))
    }
    assertThat(ex.getMessage).contains(startKey)
    assertThat(ex.getMessage).contains("must not be blank")
  }

  test("an end timestamp without a start timestamp fails fast") {
    val startKey = SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()
    val endKey = SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()
    val ex = intercept[IllegalArgumentException] {
      FlussOffsetInitializers.stoppingOffsetsInitializer(
        true,
        scanOptions(endKey -> "1767312000000"))
    }
    assertThat(ex.getMessage).contains(endKey)
    assertThat(ex.getMessage).contains(startKey)
  }

  test("the time range carried to the reader mirrors the requested window") {
    val startKey = SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()
    val endKey = SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()
    // a plain batch read has no window to apply
    assertThat(FlussOffsetInitializers.incrementalTimeRange(scanOptions()).isDefined).isFalse
    assertThat(
      FlussOffsetInitializers
        .incrementalTimeRange(scanOptions(startKey -> "1767225600000", endKey -> "1767312000000"))
        .get)
      .isEqualTo(FlussTimeRange(1767225600000L, 1767312000000L))
    // an unset end leaves the upper bound open
    assertThat(
      FlussOffsetInitializers.incrementalTimeRange(scanOptions(startKey -> "1767225600000")).get)
      .isEqualTo(FlussTimeRange(1767225600000L, Long.MaxValue))
  }

  test("a window whose start is not strictly before its end fails fast") {
    val startKey = SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()
    val endKey = SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key()
    for (end <- Seq("1767225600000", "1767139200000")) {
      val ex = intercept[IllegalArgumentException] {
        FlussOffsetInitializers.incrementalTimeRange(
          scanOptions(startKey -> "1767225600000", endKey -> end))
      }
      assertThat(ex.getMessage).contains("must be strictly before")
      assertThat(ex.getMessage).contains("1767225600000")
    }
  }

  test("predatesRetention compares the window start against now minus table.log.ttl") {
    val nowMs = 1767225600000L
    val ttlMs = 7L * 24 * 60 * 60 * 1000
    // a start well before the retention horizon predates it
    assertThat(FlussOffsetInitializers.predatesRetention(nowMs - ttlMs - 1, ttlMs, nowMs)).isTrue
    // a start inside the retention horizon does not
    assertThat(FlussOffsetInitializers.predatesRetention(nowMs - ttlMs + 1, ttlMs, nowMs)).isFalse
    // a start exactly at the horizon is not warned about
    assertThat(FlussOffsetInitializers.predatesRetention(nowMs - ttlMs, ttlMs, nowMs)).isFalse
    // a ttl larger than the elapsed time retains everything since epoch 0
    assertThat(FlussOffsetInitializers.predatesRetention(0L, nowMs + 1, nowMs)).isFalse
  }

  test("invalid start timestamp format fails with the option name") {
    val startKey = SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key()
    val ex = intercept[IllegalArgumentException] {
      FlussOffsetInitializers.incrementalStartOffsetsInitializer(
        scanOptions(startKey -> "not-a-timestamp"))
    }
    assertThat(ex.getMessage).contains(startKey)
  }

  test("scan.startup.mode=timestamp is not a batch option") {
    val ex = intercept[IllegalArgumentException] {
      FlussOffsetInitializers.startOffsetsInitializer(
        scanOptions(SparkFlussConf.SCAN_START_UP_MODE.key() -> "timestamp"),
        new Configuration())
    }
    assertThat(ex.getMessage).contains("Unsupported scan start up mode")
    assertThat(ex.getMessage).contains(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key())
  }
}
