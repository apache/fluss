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

package org.apache.fluss.spark.read

import org.apache.fluss.client.initializer.{NoStoppingOffsetsInitializer, OffsetsInitializer}
import org.apache.fluss.config.Configuration
import org.apache.fluss.spark.SparkFlussConf

import org.apache.spark.sql.util.CaseInsensitiveStringMap

object FlussOffsetInitializers {
  def startOffsetsInitializer(
      options: CaseInsensitiveStringMap,
      flussConfig: Configuration): OffsetsInitializer = {
    val startupMode = options
      .getOrDefault(
        SparkFlussConf.SCAN_START_UP_MODE.key(),
        flussConfig.get(SparkFlussConf.SCAN_START_UP_MODE))
      .toUpperCase

    SparkFlussConf.StartUpMode.withName(startupMode) match {
      case SparkFlussConf.StartUpMode.EARLIEST => OffsetsInitializer.earliest()
      case SparkFlussConf.StartUpMode.FULL => OffsetsInitializer.full()
      case SparkFlussConf.StartUpMode.LATEST => OffsetsInitializer.latest()
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported scan start up mode: ${options.get(SparkFlussConf.SCAN_START_UP_MODE.key())}")
    }
  }

  def stoppingOffsetsInitializer(
      isBatch: Boolean,
      options: CaseInsensitiveStringMap,
      flussConfig: Configuration): OffsetsInitializer = {
    if (isBatch) {
      OffsetsInitializer.latest()
    } else {
      new NoStoppingOffsetsInitializer()
    }
  }
}
