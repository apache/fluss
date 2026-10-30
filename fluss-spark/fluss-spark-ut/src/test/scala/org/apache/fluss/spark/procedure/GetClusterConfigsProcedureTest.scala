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

import org.apache.spark.sql.Row

class GetClusterConfigsProcedureTest extends FlussSparkTestBase {

  test("get_cluster_configs: get all configurations") {
    val result = sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs()").collect()

    assert(result.length > 0)

    val firstRow = result.head
    assert(firstRow.length == 6)
    assert(
      firstRow.schema.fieldNames.sameElements(
        Array(
          "config_key",
          "config_value",
          "config_source",
          "default_value",
          "is_default",
          "description")))

    result.foreach {
      row =>
        assert(row.getString(0) != null)
        // config_value may be null for unset options, but source/default flag must be present
        assert(row.getString(2) != null)
        assert(!row.isNullAt(4))
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
    val row = result.head
    assert(row.getString(0) == "kv.snapshot.interval")
    assert(row.getString(2) == "STATIC")
    // The resolved value (1 s) differs from the declared default (10 min).
    assert(row.getString(3) == "10 min")
    assert(!row.getBoolean(4))
    assert(row.getString(5) != null)
  }

  test("get_cluster_configs: get multiple configurations") {
    val key1 = ConfigOptions.KV_SNAPSHOT_INTERVAL.key()
    val key2 = ConfigOptions.BIND_LISTENERS.key()

    val result =
      sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$key1', '$key2'))")
        .collect()

    assert(result.length == 2)

    // convert the result into a map of key to row for easy verification
    val rowMap: Map[String, org.apache.spark.sql.Row] =
      result.map(r => r.getString(0) -> r).toMap
    val kvRow = rowMap(key1)
    assert(kvRow.getString(2) == "STATIC")
    assert(kvRow.getString(3) == "10 min")
    assert(!kvRow.getBoolean(4))
    assert(kvRow.getString(5) != null)

    val bindRow = rowMap(key2)
    // bind.listeners has no declared default value in ConfigOptions.
    assert(bindRow.isNullAt(3))
    assert(bindRow.getBoolean(4))
    assert(bindRow.getString(5) != null)
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

  test("get_cluster_configs: default_value and is_default columns for an overridden key") {
    val testKey = ConfigOptions.KV_SNAPSHOT_INTERVAL.key()

    val result = sql(
      s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$testKey'))").collect()

    assert(result.length == 1)
    val row = result.head
    // KV_SNAPSHOT_INTERVAL declared default is 10 min but the test cluster overrides it to 1 s.
    assert(row.getString(3) == "10 min")
    assert(!row.getBoolean(4))
    assert(row.getString(5) != null)
  }

  test("get_cluster_configs: key with no default reports null default and is_default=true") {
    val testKey = ConfigOptions.BIND_LISTENERS.key()

    val result = sql(
      s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$testKey'))").collect()

    assert(result.length == 1)
    val row = result.head
    assert(row.isNullAt(3))
    assert(row.getBoolean(4))
    assert(row.getString(5) != null)
  }
}
