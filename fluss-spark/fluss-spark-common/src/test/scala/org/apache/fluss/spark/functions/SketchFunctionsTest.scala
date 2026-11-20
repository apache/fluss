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

package org.apache.fluss.spark.functions

import org.apache.datasketches.hll.HllSketch
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.BinaryType
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import javax.management.{JMException, ObjectName}

import java.lang.management.ManagementFactory

/** Executes the SQL extension with shuffle boundaries, nulls and invalid data. */
class SketchFunctionsTest extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("sketch-functions")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.extensions", "org.apache.fluss.spark.FlussSparkSessionExtensions")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally { super.afterAll() }
  }

  test("SQL build and read functions support distributed aggregation") {
    val result = spark
      .sql("SELECT fluss_hll_estimate(fluss_hll_build_agg(id)) AS n, " +
        "fluss_kll_double_quantile(fluss_kll_double_build_agg(CAST(id AS DOUBLE)), 0.5) AS median " +
        "FROM range(100)")
      .head()
    assertThat(result.getDouble(0)).isBetween(95.0, 105.0)
    assertThat(result.getDouble(1)).isBetween(45.0, 55.0)
  }

  test("HLL skips empty strings but counts whitespace") {
    val empty = spark
      .sql("SELECT fluss_hll_build_agg(v), " +
        "fluss_hll_estimate(fluss_hll_build_agg(v)) FROM VALUES (''), (CAST(NULL AS STRING)) t(v)")
      .head()
    assertThat(HllSketch.heapify(empty.getAs[Array[Byte]](0)).isEmpty).isTrue()
    assertThat(empty.isNullAt(1)).isTrue()
    val mixed = spark
      .sql(
        "SELECT fluss_hll_estimate(fluss_hll_build_agg(v)) " +
          "FROM VALUES (''), (' '), ('user'), ('user') t(v)")
      .head()
    assertThat(mixed.getDouble(0)).isBetween(1.99, 2.01)
  }

  test("KLL merge does not retain or change input and restored buffers stay mutable") {
    for (k <- Seq(8, 200, 1024); count <- Seq(0, 1, 2, 200, 201, 20000)) {
      val input = KllDoublesSketch.newHeapInstance(k)
      (0 until count).foreach(value => input.update(value.toDouble))
      val bytes = input.toByteArray
      val original = bytes.clone()
      val function =
        SketchAggregate(Literal.create(bytes, BinaryType), hll = false, mergeInput = true)
      val buffer = function.createAggregationBuffer()
      function.update(buffer, InternalRow.empty)
      assertThat(bytes).containsExactly(original: _*)
      assertThat(buffer.kll.getN).isEqualTo(count.toLong)
      java.util.Arrays.fill(bytes, 0.toByte)
      if (count > 0) {
        assertThat(buffer.kll.getMinItem).isEqualTo(0.0)
        assertThat(buffer.kll.getMaxItem).isEqualTo((count - 1).toDouble)
      }
      val restored = function.deserialize(function.serialize(buffer))
      restored.kll.update(-1.0)
      assertThat(restored.kll.getN).isEqualTo(count + 1L)
      assertThat(buffer.kll.getN).isEqualTo(count.toLong)
    }
  }

  test("benchmark SQL with one million events") {
    if (!sys.env.get("FLUSS_SKETCH_BENCHMARK").exists(_.equalsIgnoreCase("true"))) {
      cancel("Set FLUSS_SKETCH_BENCHMARK=true to run the SQL benchmark")
    }
    val events = 1000000L
    for (workload <- Seq("global", "grouped", "merge")) {
      val groups = if (workload == "global") 1 else 10000
      val outputGroups = if (workload == "grouped") groups else 1
      var query = s"SELECT id % $groups AS g, fluss_hll_build_agg(id) AS h, " +
        s"fluss_kll_double_build_agg(CAST(id AS DOUBLE)) AS q FROM range(0, $events, 1, 2) GROUP BY id % $groups"
      if (workload == "merge") {
        query = "SELECT CAST(0 AS BIGINT) AS g, fluss_hll_merge_agg(h) AS h, " +
          s"fluss_kll_double_merge_agg(q) AS q FROM ($query)"
      }
      query =
        s"SELECT g, h, q, fluss_hll_estimate(h), fluss_kll_double_quantile(q, 0.5) FROM ($query)"
      println(spark.sql(query).queryExecution.executedPlan)
      // Iterations 0 and 1 warm up the engine; the following five are measured.
      for (iteration <- 0 until 7) {
        val allocatedBefore = allocatedBytes()
        val start = System.nanoTime()
        val result = spark.sql(query).collect()
        val elapsed = System.nanoTime() - start
        val allocatedAfter = allocatedBytes()
        val allocated =
          if (allocatedBefore < 0 || allocatedAfter < allocatedBefore) -1L
          else allocatedAfter - allocatedBefore
        assertThat(result.length).isEqualTo(outputGroups)
        var bytes = 0L
        var hllError = 0.0
        var rankError = 0.0
        result.foreach {
          row =>
            val key = row.getLong(0)
            val h = row.getAs[Array[Byte]](1)
            val q = row.getAs[Array[Byte]](2)
            bytes += h.length + q.length
            val kll = KllDoublesSketch.heapify(Memory.wrap(q))
            val count = events / outputGroups
            assertThat(kll.getN).isEqualTo(count)
            assertThat(kll.getMinItem).isEqualTo(key.toDouble)
            assertThat(kll.getMaxItem).isEqualTo((events - outputGroups + key).toDouble)
            hllError = math.max(hllError, math.abs(row.getDouble(3) / count - 1))
            rankError =
              math.max(rankError, math.abs((row.getDouble(4) - key) / outputGroups / count - 0.5))
        }
        assertThat(hllError).isLessThan(0.08)
        assertThat(rankError).isLessThan(0.04)
        // Timing includes planning, source generation, shuffle and collection, not validation.
        println(
          f"SKETCH_SQL_BENCH engine=spark-${spark.version} workload=$workload iteration=$iteration events=$events output_groups=$outputGroups elapsed_ms=${elapsed / 1000000.0}%.3f allocated_bytes=$allocated sketch_bytes=$bytes hll_error=$hllError%.6f kll_rank_error=$rankError%.6f")
      }
    }
  }

  private def allocatedBytes(): Long = {
    try {
      ManagementFactory.getPlatformMBeanServer
        .getAttribute(new ObjectName("java.lang:type=Threading"), "TotalThreadAllocatedBytes")
        .asInstanceOf[java.lang.Long]
        .longValue()
    } catch {
      // Older JVMs do not expose the all-thread allocation counter.
      case _: JMException => -1L
    }
  }

  test("sketch merge SQL combines partial binary sketches") {
    val built = spark.sql(
      "SELECT id % 2 AS k, fluss_hll_build_agg(id) AS h, " +
        "fluss_kll_double_build_agg(CAST(id AS DOUBLE)) AS q FROM range(20) GROUP BY id % 2")
    built.createOrReplaceTempView("sketches")
    val result =
      spark.sql("SELECT fluss_hll_merge_agg(h), fluss_kll_double_merge_agg(q) FROM sketches").head()
    assertThat(HllSketch.heapify(result.getAs[Array[Byte]](0)).getEstimate).isBetween(19.0, 21.0)
    val kll = KllDoublesSketch.heapify(Memory.wrap(result.getAs[Array[Byte]](1)))
    assertThat(kll.getN).isEqualTo(20L)
    assertThat(kll.getMinItem).isEqualTo(0.0)
    assertThat(kll.getMaxItem).isEqualTo(19.0)
  }

  test("null and empty sketches have explicit reader semantics") {
    val result = spark
      .sql(
        "SELECT fluss_hll_estimate(CAST(NULL AS BINARY)), " +
          "fluss_kll_double_quantile(CAST(NULL AS BINARY), 0.5)")
      .head()
    assertThat(result.isNullAt(0)).isTrue()
    assertThat(result.isNullAt(1)).isTrue()
  }

  test("invalid sketches and ranks fail") {
    assertThatThrownBy(() => spark.sql("SELECT fluss_hll_estimate(X'010203')").collect())
      .isInstanceOf(classOf[Exception])
    assertThatThrownBy(
      () =>
        spark
          .sql("SELECT fluss_kll_double_quantile(fluss_kll_double_build_agg(CAST(id AS DOUBLE)), 1.1) FROM range(2)")
          .collect())
      .isInstanceOf(classOf[Exception])
  }

  test("Spark built-in HLL sketches remain interoperable when available") {
    if (spark.catalog.functionExists("hll_sketch_agg")) {
      val result = spark
        .sql(
          "SELECT fluss_hll_estimate(hll_sketch_agg(id)), " +
            "hll_sketch_estimate(fluss_hll_build_agg(id)) FROM range(10)")
        .head()
      assertThat(result.getDouble(0)).isBetween(9.9, 10.1)
      assertThat(result.getLong(1)).isEqualTo(10L)
    }
  }
}
