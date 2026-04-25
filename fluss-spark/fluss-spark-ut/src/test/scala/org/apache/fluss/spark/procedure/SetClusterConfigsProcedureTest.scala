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

class SetClusterConfigsProcedureTest extends FlussSparkTestBase {

  test("set_cluster_configs: set a single configuration") {
    val configKey = ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()
    val configValue = "200MB"

    val result = sql(
      s"CALL $DEFAULT_CATALOG.sys.set_cluster_configs(config_pairs => array('$configKey', '$configValue'))")
      .collect()

    assert(result.length == 1)
    assert(result.head.getString(0) == configKey)
    assert(result.head.getString(1) == configValue)
    assert(result.head.getString(2).contains("Successfully"))

    // Verify the configuration was actually set
    val getResult =
      sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$configKey'))")
        .collect()

    assert(getResult.length == 1)
    assert(getResult.head.getString(0) == configKey)
    assert(getResult.head.getString(1) == configValue)
    assert(getResult.head.getString(2) == "DYNAMIC")
  }

  test("set_cluster_configs: set multiple configurations") {
    val key1 = ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()
    val value1 = "100MB"
    val key2 = ConfigOptions.DATALAKE_FORMAT.key()
    val value2 = "paimon"

    val result = sql(
      s"CALL $DEFAULT_CATALOG.sys.set_cluster_configs(config_pairs => array('$key1', '$value1', '$key2', '$value2'))")
      .collect()

    assert(result.length == 2)
    assert(result(0).getString(0) == key1)
    assert(result(0).getString(1) == value1)
    assert(result(1).getString(0) == key2)
    assert(result(1).getString(1) == value2)
  }

  test("set_cluster_configs: empty config_pairs should fail") {
    val exception = intercept[RuntimeException] {
      sql(s"CALL $DEFAULT_CATALOG.sys.set_cluster_configs(config_pairs => array())").collect()
    }
    assert(exception.getMessage.contains("cannot be null or empty"))
  }

  test("set_cluster_configs: odd number of config_pairs should fail") {
    val exception = intercept[RuntimeException] {
      sql(s"CALL $DEFAULT_CATALOG.sys.set_cluster_configs(config_pairs => array('key1'))").collect()
    }
    assert(exception.getMessage.contains("must be set in pairs"))
  }
}
