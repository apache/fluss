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

import org.apache.fluss.row.{InternalMap => FlussInternalMap}
import org.apache.fluss.types.{MapType => FlussMapType}
import org.apache.fluss.utils.InternalRowUtils

import org.apache.spark.sql.catalyst.util.{ArrayData => SparkArrayData, MapData => SparkMapData}

/** Wraps a Fluss [[FlussInternalMap]] as a Spark [[SparkMapData]]. */
class FlussAsSparkMap(mapType: FlussMapType) extends SparkMapData {

  var flussMap: FlussInternalMap = _

  def replace(map: FlussInternalMap): SparkMapData = {
    this.flussMap = map
    this
  }

  override def numElements(): Int = flussMap.size()

  override def copy(): SparkMapData = {
    new FlussAsSparkMap(mapType)
      .replace(InternalRowUtils.copyMap(flussMap, mapType.getKeyType, mapType.getValueType))
  }

  override def keyArray(): SparkArrayData = {
    val keyType = mapType.getKeyType
    new FlussAsSparkArray(keyType).replace(flussMap.keyArray())
  }

  override def valueArray(): SparkArrayData = {
    val valueType = mapType.getValueType
    new FlussAsSparkArray(valueType).replace(flussMap.valueArray())
  }
}
