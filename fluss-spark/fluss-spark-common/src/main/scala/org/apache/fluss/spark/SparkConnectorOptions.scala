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

package org.apache.fluss.spark

import org.apache.fluss.config.{ConfigBuilder, ConfigOption}

object SparkConnectorOptions {

  val PRIMARY_KEY: ConfigOption[String] =
    ConfigBuilder
      .key("primary.key")
      .stringType()
      .noDefaultValue()
      .withDescription("The primary keys of a Fluss table.")

  val BUCKET_KEY: ConfigOption[String] =
    ConfigBuilder
      .key("bucket.key")
      .stringType()
      .noDefaultValue()
      .withDescription(
        """
          |Specific the distribution policy of the Fluss table.
          |Data will be distributed to each bucket according to the hash value of bucket-key (It must be a subset of the primary keys excluding partition keys of the primary key table).
          |If you specify multiple fields, delimiter is ','.
          |If the table has a primary key and a bucket key is not specified, the bucket key will be used as primary key(excluding the partition key).
          |If the table has no primary key and the bucket key is not specified, the data will be distributed to each bucket randomly.
          |""".stripMargin)

  val BUCKET_NUMBER: ConfigOption[Integer] =
    ConfigBuilder
      .key("bucket.num")
      .intType()
      .noDefaultValue()
      .withDescription("The number of buckets of a Fluss table.")

  val COMMENT: ConfigOption[String] =
    ConfigBuilder
      .key("comment")
      .stringType()
      .noDefaultValue()
      .withDescription("The comment of a Fluss table.")

  val SPARK_TABLE_OPTIONS: Seq[String] =
    Seq(PRIMARY_KEY, BUCKET_KEY, BUCKET_NUMBER, COMMENT).map(_.key)
}
