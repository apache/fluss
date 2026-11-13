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

import org.apache.datasketches.hll.{HllSketch, Union}
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, ByteType, DataType, DoubleType, IntegerType, LongType, ShortType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/** Mutable state; only the portable sketch bytes cross shuffle boundaries. */
final class SketchBuffer(val hll: Union, val kll: KllDoublesSketch)

/** Builds or merges sketch bytes with partial aggregation and spill support. */
case class SketchAggregate(
    child: Expression,
    hll: Boolean,
    mergeInput: Boolean,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[SketchBuffer]
  with ImplicitCastInputTypes {

  override def children: Seq[Expression] = Seq(child)
  override def dataType: DataType = BinaryType
  override def nullable: Boolean = true
  override lazy val deterministic: Boolean = false
  override def inputTypes = Seq(
    if (mergeInput) BinaryType else if (hll) child.dataType else DoubleType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (
      hll && !mergeInput && !Seq(ByteType, ShortType, IntegerType, LongType, StringType).contains(
        child.dataType)
    ) {
      TypeCheckResult.TypeCheckFailure("HLL build requires an integer or string input")
    } else {
      super.checkInputDataTypes()
    }
  }

  override def createAggregationBuffer(): SketchBuffer =
    if (hll) new SketchBuffer(new Union(12), null)
    else new SketchBuffer(null, KllDoublesSketch.newHeapInstance())

  override def update(buffer: SketchBuffer, input: InternalRow): SketchBuffer = {
    val value = child.eval(input)
    if (value != null) {
      if (hll) {
        if (mergeInput) buffer.hll.update(HllSketch.heapify(value.asInstanceOf[Array[Byte]]))
        else if (child.dataType == StringType)
          buffer.hll.update(value.asInstanceOf[UTF8String].toString)
        else buffer.hll.update(value.asInstanceOf[Number].longValue())
      } else if (mergeInput) {
        buffer.kll.merge(KllDoublesSketch.heapify(Memory.wrap(value.asInstanceOf[Array[Byte]])))
      } else buffer.kll.update(value.asInstanceOf[Double])
    }
    buffer
  }

  override def merge(buffer: SketchBuffer, other: SketchBuffer): SketchBuffer = {
    if (hll) buffer.hll.update(other.hll.getResult()) else buffer.kll.merge(other.kll)
    buffer
  }

  override def eval(buffer: SketchBuffer): Any = {
    if (!hll && buffer.kll.isEmpty) null
    else serialize(buffer)
  }

  override def serialize(buffer: SketchBuffer): Array[Byte] =
    if (hll) buffer.hll.toCompactByteArray else buffer.kll.toByteArray

  override def deserialize(bytes: Array[Byte]): SketchBuffer =
    if (hll) new SketchBuffer(Union.heapify(bytes), null)
    else new SketchBuffer(null, KllDoublesSketch.heapify(Memory.wrap(bytes)))

  override def withNewMutableAggBufferOffset(offset: Int): SketchAggregate =
    copy(mutableAggBufferOffset = offset)
  override def withNewInputAggBufferOffset(offset: Int): SketchAggregate =
    copy(inputAggBufferOffset = offset)
  override protected def withNewChildrenInternal(
      children: IndexedSeq[Expression]): SketchAggregate =
    copy(child = children.head)
}

/** Decodes DataSketches HLL bytes without converting them to Spark's internal HLL format. */
case class HllEstimate(child: Expression)
  extends UnaryExpression
  with ImplicitCastInputTypes
  with CodegenFallback {
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def inputTypes = Seq(BinaryType)
  override protected def nullSafeEval(value: Any): Any = {
    val sketch = HllSketch.heapify(value.asInstanceOf[Array[Byte]])
    if (sketch.isEmpty) null else sketch.getEstimate
  }
  override protected def withNewChildInternal(child: Expression): HllEstimate = copy(child = child)
}

/** Decodes a quantile from DataSketches KLL doubles bytes. */
case class KllDoubleQuantile(left: Expression, right: Expression)
  extends BinaryExpression
  with ImplicitCastInputTypes
  with CodegenFallback {
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def inputTypes = Seq(BinaryType, DoubleType)
  override protected def nullSafeEval(bytes: Any, rank: Any): Any = {
    val r = rank.asInstanceOf[Double]
    require(r >= 0.0 && r <= 1.0, "Quantile rank must be in [0, 1]")
    val sketch = KllDoublesSketch.heapify(Memory.wrap(bytes.asInstanceOf[Array[Byte]]))
    if (sketch.isEmpty) null else sketch.getQuantile(r)
  }
  override protected def withNewChildrenInternal(
      left: Expression,
      right: Expression): KllDoubleQuantile =
    copy(left = left, right = right)
}
