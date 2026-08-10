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
    assertThat(FlussOffsetInitializers.isIncrementalRead(scanOptions(startKey -> "  "))).isFalse
    assertThat(
      FlussOffsetInitializers.isIncrementalRead(scanOptions(startKey -> "1767225600000"))).isTrue
  }

  test("scan.incremental.timestamp.out-of-range toggles fail-fast (default error)") {
    val key = SparkFlussConf.SCAN_INCREMENTAL_TIMESTAMP_OUT_OF_RANGE.key()
    // default (unset) is error -> fail fast
    assertThat(FlussOffsetInitializers.failOnTimestampOutOfRange(scanOptions())).isTrue
    assertThat(
      FlussOffsetInitializers.failOnTimestampOutOfRange(scanOptions(key -> "error"))).isTrue
    assertThat(
      FlussOffsetInitializers.failOnTimestampOutOfRange(scanOptions(key -> "ERROR"))).isTrue
    // a blank value counts as unset and falls back to the default (error)
    assertThat(FlussOffsetInitializers.failOnTimestampOutOfRange(scanOptions(key -> "  "))).isTrue
    // adjust -> clamp instead of failing
    assertThat(
      FlussOffsetInitializers.failOnTimestampOutOfRange(scanOptions(key -> "adjust"))).isFalse
  }

  test("invalid scan.incremental.timestamp.out-of-range value fails with supported values") {
    val key = SparkFlussConf.SCAN_INCREMENTAL_TIMESTAMP_OUT_OF_RANGE.key()
    val ex = intercept[IllegalArgumentException] {
      FlussOffsetInitializers.failOnTimestampOutOfRange(scanOptions(key -> "warn"))
    }
    assertThat(ex.getMessage).contains(key)
    assertThat(ex.getMessage).contains("WARN")
    assertThat(ex.getMessage).contains("'error', 'adjust'")
  }

  test("retention guard decision (isBeforeRetention)") {
    // brand-new bucket (earliest == 0) is never flagged, even for a very old start offset
    assertThat(FlussOffsetInitializers.isBeforeRetention(0L, 0L)).isFalse
    assertThat(FlussOffsetInitializers.isBeforeRetention(5L, 0L)).isFalse
    // a trimmed bucket (earliest > 0) is flagged when the start lands at or before earliest
    assertThat(FlussOffsetInitializers.isBeforeRetention(10L, 10L)).isTrue
    assertThat(FlussOffsetInitializers.isBeforeRetention(3L, 10L)).isTrue
    // a start strictly after earliest is within retention
    assertThat(FlussOffsetInitializers.isBeforeRetention(11L, 10L)).isFalse
  }

  test("TTL-exceeded start fails fast with a table.log.ttl hint") {
    // start at/before a trimmed earliest (earliest > 0): fail fast with a clear TTL message
    val ex = intercept[IllegalArgumentException] {
      FlussOffsetInitializers.requireStartWithinRetention("fluss.t", "dt=2026", 2, 5L, 10L)
    }
    assertThat(ex.getMessage).contains("table.log.ttl")
    assertThat(ex.getMessage).contains("bucket 2")
    assertThat(ex.getMessage).contains("partition 'dt=2026'")
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
