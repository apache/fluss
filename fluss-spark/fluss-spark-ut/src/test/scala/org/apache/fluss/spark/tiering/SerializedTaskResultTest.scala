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

package org.apache.fluss.spark.tiering

import org.apache.fluss.client.tiering.TableBucketWriteResult
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer
import org.apache.fluss.metadata.{TableBucket, TablePath}
import org.apache.fluss.utils.InstantiationUtils

import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8

/** The executor result must not require Java serialization of plugin-specific write results. */
class SerializedTaskResultTest extends AnyFunSuite {

  private class WriteResult(val value: String)

  private val serializer = new SimpleVersionedSerializer[AnyRef] {
    override def getVersion: Int = 7
    override def serialize(value: AnyRef): Array[Byte] =
      value.asInstanceOf[WriteResult].value.getBytes(UTF_8)
    override def deserialize(version: Int, bytes: Array[Byte]): AnyRef = {
      assertThat(version).isEqualTo(7)
      new WriteResult(new String(bytes, UTF_8))
    }
  }

  test("non-serializable plugin results survive the Spark transport round trip") {
    val result = new TableBucketWriteResult[AnyRef](
      TablePath.of("db", "table"),
      new TableBucket(1L, 2),
      "partition",
      new WriteResult("files"),
      123L,
      456L,
      3)
    val serialized = SerializedTaskResult.fromWriteResult(result, serializer)
    val bytes = InstantiationUtils.serializeObject(serialized)
    val transported =
      InstantiationUtils.deserializeObject[SerializedTaskResult](bytes, getClass.getClassLoader)
    val restored = transported.toWriteResult(serializer)
    assertThat(restored.writeResult().asInstanceOf[WriteResult].value).isEqualTo("files")
    assertThat(restored.tablePath()).isEqualTo(result.tablePath())
    assertThat(restored.tableBucket()).isEqualTo(result.tableBucket())
    assertThat(restored.partitionName()).isEqualTo("partition")
    assertThat(restored.logEndOffset()).isEqualTo(123L)
    assertThat(restored.maxTimestamp()).isEqualTo(456L)
    assertThat(restored.numberOfWriteResults()).isEqualTo(3)
  }

  test("empty write results preserve tiered offsets without invoking the plugin serializer") {
    val result = new TableBucketWriteResult[AnyRef](
      TablePath.of("db", "table"),
      new TableBucket(1L, 0),
      null,
      null,
      10L,
      -1L,
      1)
    val restored =
      SerializedTaskResult.fromWriteResult(result, serializer).toWriteResult(serializer)
    assertThat(restored.writeResult()).isNull()
    assertThat(restored.partitionName()).isNull()
    assertThat(restored.logEndOffset()).isEqualTo(10L)
  }
}
