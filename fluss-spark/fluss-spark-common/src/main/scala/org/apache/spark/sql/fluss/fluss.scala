/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import com.alibaba.fluss.spark.TableBucketInfo

import org.apache.spark.internal.config.ConfigBuilder

import java.util.concurrent.TimeUnit

package object fluss {
  // scalastyle:ignore
  // ^^ scalastyle:ignore is for ignoring warnings about digits in package name
  type BucketOffsetMap = Map[TableBucketInfo, Long]

}
