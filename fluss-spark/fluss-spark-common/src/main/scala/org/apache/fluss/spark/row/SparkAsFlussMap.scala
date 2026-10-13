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

package org.apache.fluss.spark.row

import org.apache.fluss.row.{InternalArray => FlussInternalArray, InternalMap => FlussInternalMap}

import org.apache.spark.sql.catalyst.util.{MapData => SparkMapData}
import org.apache.spark.sql.types.{DataType => SparkDataType, MapType => SparkMapType}

/** Wraps a Spark [[SparkMapData]] as a Fluss [[FlussInternalMap]]. */
class SparkAsFlussMap(mapData: SparkMapData, keyType: SparkDataType, valueType: SparkDataType)
  extends FlussInternalMap
  with Serializable {

  /** Returns the number of key-value mappings in this map. */
  override def size(): Int = mapData.numElements()

  /**
   * Returns an array view of the keys contained in this map.
   *
   * <p>A key-value pair has the same index in the key array and value array.
   */
  override def keyArray(): FlussInternalArray = {
    new SparkAsFlussArray(mapData.keyArray(), keyType)
  }

  /**
   * Returns an array view of the values contained in this map.
   *
   * <p>A key-value pair has the same index in the key array and value array.
   */
  override def valueArray(): FlussInternalArray = {
    new SparkAsFlussArray(mapData.valueArray(), valueType)
  }
}

object SparkAsFlussMap {
  def apply(mapData: SparkMapData, mapType: SparkMapType): SparkAsFlussMap =
    new SparkAsFlussMap(mapData, mapType.keyType, mapType.valueType)
}
