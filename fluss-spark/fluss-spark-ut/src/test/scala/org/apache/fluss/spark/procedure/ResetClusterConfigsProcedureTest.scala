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

class ResetClusterConfigsProcedureTest extends FlussSparkTestBase {

  test("reset_cluster_configs: set and then reset a configuration") {
    val configKey = ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()
    val configValue = "300MB"

    // First, set a dynamic configuration
    sql(
      s"CALL $DEFAULT_CATALOG.sys.set_cluster_configs(config_pairs => array('$configKey', '$configValue'))")
      .collect()

    // Verify it was set
    val getResult =
      sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$configKey'))")
        .collect()
    assert(getResult.length == 1)
    assert(getResult.head.getString(2) == "DYNAMIC")

    // Reset the configuration
    val resetResult =
      sql(s"CALL $DEFAULT_CATALOG.sys.reset_cluster_configs(config_keys => array('$configKey'))")
        .collect()

    assert(resetResult.length == 1)
    assert(resetResult.head.getString(0) == configKey)
    assert(resetResult.head.getString(1).contains("Successfully"))

    // Verify it was reset (should no longer be DYNAMIC)
    val getResultAfterReset =
      sql(s"CALL $DEFAULT_CATALOG.sys.get_cluster_configs(config_keys => array('$configKey'))")
        .collect()
    assert(getResultAfterReset.length == 1)
    assert(getResultAfterReset.head.getString(2) != "DYNAMIC")
  }

  test("reset_cluster_configs: reset multiple configurations") {
    val key1 = ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key()
    val key2 = ConfigOptions.DATALAKE_FORMAT.key()

    // First, set dynamic configurations
    sql(
      s"CALL $DEFAULT_CATALOG.sys.set_cluster_configs(config_pairs => array('$key1', '100MB', '$key2', 'paimon'))")
      .collect()

    // Reset both configurations
    val result = sql(
      s"CALL $DEFAULT_CATALOG.sys.reset_cluster_configs(config_keys => array('$key1', '$key2'))")
      .collect()

    assert(result.length == 2)
    assert(result(0).getString(0) == key1)
    assert(result(1).getString(0) == key2)
  }

  test("reset_cluster_configs: empty config_keys should fail") {
    val exception = intercept[RuntimeException] {
      sql(s"CALL $DEFAULT_CATALOG.sys.reset_cluster_configs(config_keys => array())").collect()
    }
    assert(exception.getMessage.contains("cannot be null or empty"))
  }
}
