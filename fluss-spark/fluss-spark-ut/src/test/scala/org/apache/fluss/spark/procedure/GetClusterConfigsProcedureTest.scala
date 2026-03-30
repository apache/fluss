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

package org.apache.fluss.spark.procedure

import org.apache.fluss.config.ConfigOptions
import org.apache.fluss.spark.FlussSparkTestBase

class GetClusterConfigsProcedureTest extends FlussSparkTestBase {

  test("get_cluster_configs: get all configurations") {
    val result = sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs()").collect()

    assert(result.length > 0)

    val firstRow = result.head
    assert(firstRow.length == 3)
    assert(
      firstRow.schema.fieldNames.sameElements(Array("config_key", "config_value", "config_source")))

    result.foreach {
      row =>
        assert(row.getString(0) != null)
        assert(row.getString(1) != null)
        assert(row.getString(2) != null)
    }

    val result2 =
      sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array())").collect()
    assertResult(result)(result2)
  }

  test("get_cluster_configs: get specific configuration") {
    val testKey = ConfigOptions.KV_SNAPSHOT_INTERVAL.key()

    val result = sql(
      s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$testKey'))").collect()

    assert(result.length == 1)
    assert(result.head.toString() == "[kv.snapshot.interval,1 s,STATIC]")
  }

  test("get_cluster_configs: get multiple configurations") {
    val key1 = ConfigOptions.KV_SNAPSHOT_INTERVAL.key()
    val key2 = ConfigOptions.BIND_LISTENERS.key()

    val result =
      sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$key1', '$key2'))")
        .collect()

    assert(result.length == 2)

    // convert the result into a map of key to value for easy verification, key is the first column
    val kvMap: Map[String, String] = result.map(r => r.getString(0) -> r.toString).toMap
    assert(kvMap.getOrElse(key1, "") == s"[$key1,1 s,STATIC]")
    assert(kvMap.getOrElse(key2, "") == s"[$key2,FLUSS://localhost:0,STATIC]")
  }

  test("get_cluster_configs: get non-existent configuration") {
    val nonExistentKey = "non.existent.config.key"

    val result =
      sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$nonExistentKey'))")
        .collect()

    assert(result.length == 0)
  }

  test("get_cluster_configs: mixed existent and non-existent configurations") {
    val existentKey = ConfigOptions.KV_SNAPSHOT_INTERVAL.key()
    val nonExistentKey = "non.existent.config.key"

    val result = sql(
      s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$existentKey', '$nonExistentKey'))")
      .collect()

    assert(result.length == 1)
    assert(result.head.getString(0) == existentKey)
  }

  test("get_cluster_configs: verify configuration source") {
    val testKey = ConfigOptions.KV_SNAPSHOT_INTERVAL.key()

    val result = sql(
      s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$testKey'))").collect()

    assert(result.length == 1)
    val row = result.head
    val source = row.getString(2)

    assert(source == "DYNAMIC" || source == "STATIC" || source == "DEFAULT")
  }

  test("get_cluster_configs: empty array parameter should return all configs") {
    val result =
      sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array())").collect()

    assert(result.length > 0)
  }
}
