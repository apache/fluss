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

package org.apache.fluss.spark.lake

import org.apache.fluss.spark.SparkFlussConf

import org.apache.spark.sql.DataFrame

/** Exercises the lake time-range contract through the DataFrame options entry point. */
@SparkLakeTest
class Spark4LakePaimonTimeRangeReadTest extends SparkLakePaimonTimeRangeReadTest {
  override protected def readTimeRange(tableName: String, start: Long, end: Long): DataFrame = {
    spark.read
      .option(SparkFlussConf.SCAN_INCREMENTAL_START_TIMESTAMP.key(), start.toString)
      .option(SparkFlussConf.SCAN_INCREMENTAL_END_TIMESTAMP.key(), end.toString)
      .table(s"$DEFAULT_DATABASE.$tableName")
      .orderBy("id")
  }
}
