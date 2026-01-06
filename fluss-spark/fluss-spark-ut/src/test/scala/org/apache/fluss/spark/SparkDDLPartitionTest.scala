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

import org.apache.fluss.metadata.PartitionSpec

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

class SparkDDLPartitionTest extends FlussSparkTestBase {

  test("show partitions") {
    withCurrentCatalogAndNamespace {
      withTable("t") {
        sql(s"CREATE TABLE t (id int, name string, pt1 string, pt2 int) PARTITIONED BY (pt1, pt2)")
        sql(s"INSERT INTO t values(1, 'a', 'a', 1), (2, 'b', 'a', 2), (3, 'c', 'c', 3)")

        var expect = Seq(Row("pt1=a/pt2=1"), Row("pt1=a/pt2=2"), Row("pt1=c/pt2=3"))
        checkAnswer(sql(s"SHOW PARTITIONS t"), expect)
        expect = Seq(Row("pt1=a/pt2=1"), Row("pt1=a/pt2=2"))
        checkAnswer(sql(s"SHOW PARTITIONS t PARTITION (pt1 = 'a')"), expect)
      }
    }

  }

  test("add partition") {
    withCurrentCatalogAndNamespace {
      withTable("t") {
        sql("CREATE TABLE t (id int, name string, pt1 string, pt2 int) PARTITIONED BY (pt1, pt2)")

        // add from sparksql
        sql(s"ALTER TABLE t ADD PARTITION (pt1 = 'b', pt2 = 1)")
        var expect = Seq(Row("pt1=b/pt2=1"))
        checkAnswer(sql(s"SHOW PARTITIONS t"), expect)
        sql(s"ALTER TABLE t ADD IF NOT EXISTS PARTITION (pt1 = 'b', pt2 = 1)")
        checkAnswer(sql(s"SHOW PARTITIONS t"), expect)

        // add from fluss
        val map = Map("pt1" -> "b", "pt2" -> "2")
        admin.createPartition(createTablePath("t"), new PartitionSpec(map.asJava), false).get()
        expect = Seq(Row("pt1=b/pt2=1"), Row("pt1=b/pt2=2"))
        checkAnswer(sql(s"SHOW PARTITIONS t"), expect)
      }
    }
  }

  test("drop partition") {
    withCurrentCatalogAndNamespace {
      withTable("t") {
        sql("CREATE TABLE t (id int, name string, pt1 string, pt2 int) PARTITIONED BY (pt1, pt2)")
        sql(s"INSERT INTO t values(1, 'a', 'a', 1), (2, 'b', 'a', 2), (3, 'c', 'c', 3)")

        // drop from sparksql
        sql(s"ALTER TABLE t DROP PARTITION (pt1 = 'a', pt2 = 2)")
        var expect = Seq(Row("pt1=a/pt2=1"), Row("pt1=c/pt2=3"))
        checkAnswer(sql(s"SHOW PARTITIONS t"), expect)
        sql(s"ALTER TABLE t DROP IF EXISTS PARTITION (pt1 = 'a', pt2 = 2)")
        checkAnswer(sql(s"SHOW PARTITIONS t"), expect)

        // drop from fluss
        val map = Map("pt1" -> "c", "pt2" -> "3")
        admin.dropPartition(createTablePath("t"), new PartitionSpec(map.asJava), false).get()
        expect = Seq(Row("pt1=a/pt2=1"))
        checkAnswer(sql(s"SHOW PARTITIONS t"), expect)
      }
    }
  }
}
