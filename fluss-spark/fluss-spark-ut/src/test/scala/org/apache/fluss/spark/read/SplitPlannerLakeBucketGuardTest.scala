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

import org.apache.fluss.metadata.TablePath

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit test for the fail-loud guard used by [[UpsertPlanner]] union-read planning: a lake snapshot
 * bucket id outside a partition's enumerated bucket range must be rejected rather than silently
 * dropped from the union read. This is the Spark counterpart of the Flink LakeSplitGeneratorTest
 * guard.
 */
class SplitPlannerLakeBucketGuardTest extends AnyFunSuite {

  private val tablePath = TablePath.of("db", "pk_rescale")

  test("out-of-range lake bucket fails loud") {
    val ex = intercept[IllegalStateException] {
      // enumerated range is [0, 2) but the lake snapshot has a bucket 5 (e.g. an "old" partition
      // whose actual bucket count is smaller than a stale table-level value)
      SplitPlanner.checkLakeBucketsWithinEnumeratedRange(tablePath, "old", Set(0, 1, 5), 2)
    }
    assert(ex.getMessage.contains("outside the enumerated range"))
    assert(ex.getMessage.contains("refusing to generate union-read"))
    // only bucket 5 is reported, rendered Scala-version independently
    assert(ex.getMessage.contains("[5]"))
    assert(ex.getMessage.contains("[0, 2)"))
  }

  test("in-range lake buckets pass") {
    // all bucket ids within [0, 4) -> no exception
    SplitPlanner.checkLakeBucketsWithinEnumeratedRange(tablePath, "new", Set(0, 1, 2, 3), 4)
  }

  test("empty lake buckets pass") {
    SplitPlanner.checkLakeBucketsWithinEnumeratedRange(tablePath, "p", Set.empty[Int], 2)
  }
}
