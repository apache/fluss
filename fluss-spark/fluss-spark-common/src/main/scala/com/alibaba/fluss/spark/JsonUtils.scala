/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.spark

import com.alibaba.fluss.metadata.TableBucket

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, TreeNode}
import com.fasterxml.jackson.databind.{DeserializationContext, DeserializationFeature, JsonNode, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.{StdDeserializer, StdKeyDeserializer}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

import scala.collection.mutable

object JsonUtils {

  lazy val mapper = {
    val _mapper = new ObjectMapper with ScalaObjectMapper {}
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.registerModule(DefaultScalaModule)

    val customModule = new SimpleModule()
    customModule.addSerializer(
      classOf[FlussSourceOffset],
      new StdSerializer[FlussSourceOffset](classOf[FlussSourceOffset]) {
        override def serialize(
            offset: FlussSourceOffset,
            gen: JsonGenerator,
            provider: SerializerProvider): Unit = {
          gen.writeStartObject()
          gen.writeFieldName("buckets")
          gen.writeStartArray()
          offset.bucketToOffsets.foreach {
            case (bucketInfo, offset) =>
              gen.writeStartObject()
              serializeTableBucketInfo(bucketInfo, gen, provider)
              gen.writeNumberField("offset", offset)
              gen.writeEndObject()
          }
          gen.writeEndArray()
          gen.writeEndObject()
        }
        private def serializeTableBucketInfo(
            tableBucketInfo: TableBucketInfo,
            gen: JsonGenerator,
            serializerProvider: SerializerProvider): Unit = {
          gen.writeObjectFieldStart("tableBucketInfo")
          serializeNestedObject(tableBucketInfo.getTableBucket, gen, serializerProvider)
          gen.writeStringField(
            "partitionName",
            Some(tableBucketInfo.getPartitionName).getOrElse(""))
          gen.writeStringField(
            "snapshotId",
            Option(tableBucketInfo.getSnapshotId).getOrElse(-1L).toString)
          gen.writeEndObject()
        }
        private def serializeNestedObject(
            bucket: TableBucket,
            gen: JsonGenerator,
            serializerProvider: SerializerProvider): Unit = {
          gen.writeObjectFieldStart("tableBucket")
          gen.writeStringField("tableId", bucket.getTableId.toString)
          gen.writeStringField("bucket", bucket.getBucket.toString)
          gen.writeStringField("partitionId", Option(bucket.getPartitionId).getOrElse(-1L).toString)
          gen.writeEndObject()
        }
      }
    )

    customModule.addDeserializer(
      classOf[FlussSourceOffset],
      new StdDeserializer[FlussSourceOffset](classOf[FlussSourceOffset]) {
        override def deserialize(p: JsonParser, ctxt: DeserializationContext): FlussSourceOffset = {
          val codec = p.getCodec
          val node: JsonNode = codec.readTree(p)
          val bucketToOffsets = mutable.Map[TableBucketInfo, Long]()

          val buckets = node.get("buckets")
          if (buckets != null) {
            buckets.forEach {
              bucketNode =>
                val tableBucket = deserializeTableBucketInfo(bucketNode.get("tableBucketInfo"))
                val offset = bucketNode.get("offset").asLong()

                bucketToOffsets += (tableBucket -> offset)
            }
          }

          new FlussSourceOffset(bucketToOffsets.toMap)
        }

        private def deserializeTableBucketInfo(bucketNode: JsonNode): TableBucketInfo = {
          val tableBucketNode = bucketNode.get("tableBucket")

          val tableId = tableBucketNode.get("tableId").asText()
          val bucket = tableBucketNode.get("bucket").asText()
          val partitionId =
            Option(tableBucketNode.get("partitionId").asLong()).filter(_ >= 0).orElse(None)

          var tableBucket: TableBucket = null

          partitionId match {
            case None => tableBucket = new TableBucket(tableId.toLong, bucket.toInt)
            case Some(partitionId) =>
              tableBucket = new TableBucket(tableId.toLong, partitionId, bucket.toInt)
          }

          val partitionName = bucketNode.get("partitionName").asText(null)
          val snapshotId = Option(bucketNode.get("snapshotId").asLong()).filter(_ >= 0).orElse(None)
          snapshotId match {
            case None => new TableBucketInfo(tableBucket, partitionName, null)
            case Some(snapshotId) => new TableBucketInfo(tableBucket, partitionName, snapshotId)
          }
        }
      }
    )
    _mapper.registerModule(customModule)
    _mapper
  }

  def toJson(obj: FlussSourceOffset): String = {
    mapper.writeValueAsString(obj)
  }

  def fromJson(json: String): FlussSourceOffset = {
    mapper.readValue(json, classOf[FlussSourceOffset])
  }
}
