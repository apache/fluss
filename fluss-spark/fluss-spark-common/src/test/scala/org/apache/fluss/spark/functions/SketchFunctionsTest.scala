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
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

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
