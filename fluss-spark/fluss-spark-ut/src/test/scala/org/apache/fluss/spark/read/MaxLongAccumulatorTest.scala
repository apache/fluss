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

import org.apache.spark.util.LongAccumulator

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.scalatest.funsuite.AnyFunSuite

class MaxLongAccumulatorTest extends AnyFunSuite {

  test("isZero: true on fresh instance") {
    val acc = new MaxLongAccumulator
    assertThat(acc.isZero).isTrue
  }

  test("isZero: false after add") {
    val acc = new MaxLongAccumulator
    acc.add(1L)
    assertThat(acc.isZero).isFalse
  }

  test("add: keeps maximum value") {
    val acc = new MaxLongAccumulator
    acc.add(5L)
    acc.add(3L)
    acc.add(9L)
    acc.add(2L)
    assertThat(acc.value).isEqualTo(9L)
  }

  test("add: single value is returned as-is") {
    val acc = new MaxLongAccumulator
    acc.add(42L)
    assertThat(acc.value).isEqualTo(42L)
  }

  test("reset: sets value back to zero") {
    val acc = new MaxLongAccumulator
    acc.add(100L)
    acc.reset()
    assertThat(acc.value).isEqualTo(0L)
    assertThat(acc.isZero).isTrue
  }

  test("copy: produces independent accumulator with same value") {
    val acc = new MaxLongAccumulator
    acc.add(7L)
    val copy = acc.copy()
    assertThat(copy.value).isEqualTo(7L)
    // Mutations to copy must not affect original
    copy.add(100L)
    assertThat(acc.value).isEqualTo(7L)
    // Mutations to original must not affect copy
    acc.add(50L)
    assertThat(copy.value).isEqualTo(100L)
  }

  test("merge: takes maximum of two accumulators") {
    val acc1 = new MaxLongAccumulator
    acc1.add(10L)
    val acc2 = new MaxLongAccumulator
    acc2.add(20L)
    acc1.merge(acc2)
    assertThat(acc1.value).isEqualTo(20L)
  }

  test("merge: retains own value when other is smaller") {
    val acc1 = new MaxLongAccumulator
    acc1.add(50L)
    val acc2 = new MaxLongAccumulator
    acc2.add(30L)
    acc1.merge(acc2)
    assertThat(acc1.value).isEqualTo(50L)
  }

  test("merge: with zero accumulator leaves value unchanged") {
    val acc1 = new MaxLongAccumulator
    acc1.add(15L)
    val acc2 = new MaxLongAccumulator // value is 0
    acc1.merge(acc2)
    assertThat(acc1.value).isEqualTo(15L)
  }

  test("merge: throws on incompatible accumulator type") {
    val acc = new MaxLongAccumulator
    val other = new LongAccumulator
    assertThatThrownBy(() => acc.merge(other))
      .isInstanceOf(classOf[UnsupportedOperationException])
  }
}
