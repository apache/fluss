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

package org.apache.fluss.spark.read.lake

import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer
import org.apache.fluss.lake.source.{LakeSource, LakeSplit}
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.utils.PropertiesUtils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util
import java.util.ArrayList

object FlussLakeUtils {

  def createLakeSource(
      tableProperties: util.Map[String, String],
      tablePath: TablePath): LakeSource[LakeSplit] = {
    val tableConfig = Configuration.fromMap(tableProperties)
    val datalakeFormat = tableConfig.get(ConfigOptions.TABLE_DATALAKE_FORMAT)
    val dataLakePrefix = "table.datalake." + datalakeFormat + "."

    val catalogProperties = PropertiesUtils.extractAndRemovePrefix(tableProperties, dataLakePrefix)
    val lakeConfig = Configuration.fromMap(catalogProperties)
    val lakeStoragePlugin =
      LakeStoragePluginSetUp.fromDataLakeFormat(datalakeFormat.toString, null)
    val lakeStorage = lakeStoragePlugin.createLakeStorage(lakeConfig)
    lakeStorage.createLakeSource(tablePath).asInstanceOf[LakeSource[LakeSplit]]
  }

  def lakeProjection(projection: Array[Int]): Array[Array[Int]] = {
    projection.map(i => Array(i))
  }

  def serializeLakeSplits(
      lakeSplits: Seq[LakeSplit],
      splitSerializer: SimpleVersionedSerializer[LakeSplit]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)

    // Write serializer version
    dos.writeInt(splitSerializer.getVersion)
    // Write number of splits
    dos.writeInt(lakeSplits.size)
    // Write each split
    for (split <- lakeSplits) {
      val serialized = splitSerializer.serialize(split)
      dos.writeInt(serialized.length)
      dos.write(serialized)
    }

    dos.flush()
    baos.toByteArray
  }

  def deserializeLakeSplits(
      bytes: Array[Byte],
      splitSerializer: org.apache.fluss.lake.serializer.SimpleVersionedSerializer[LakeSplit])
      : java.util.List[LakeSplit] = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bais)

    // Read serializer version
    val version = dis.readInt()
    // Read number of splits
    val numSplits = dis.readInt()
    val splits = new ArrayList[LakeSplit](numSplits)

    // Read each split
    for (_ <- 0 until numSplits) {
      val length = dis.readInt()
      val serialized = new Array[Byte](length)
      dis.readFully(serialized)
      splits.add(splitSerializer.deserialize(version, serialized))
    }

    splits
  }
}
