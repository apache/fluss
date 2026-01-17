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

import org.apache.fluss.metadata._
import org.apache.fluss.types.{DataTypes, RowType}

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.PartitionsAlreadyExistException
import org.apache.spark.sql.connector.catalog.Identifier
import org.assertj.core.api.Assertions.{assertThat, assertThatList}

import scala.collection.JavaConverters._

class SparkCatalogTest extends FlussSparkTestBase {

  test("Catalog: namespaces") {
    // Always a default database 'fluss'.
    checkAnswer(sql("SHOW DATABASES"), Row(DEFAULT_DATABASE) :: Nil)

    sql("CREATE DATABASE testdb COMMENT 'created by spark'")
    checkAnswer(sql("SHOW DATABASES"), Row(DEFAULT_DATABASE) :: Row("testdb") :: Nil)

    checkAnswer(
      sql("DESC DATABASE testdb").filter("info_name != 'Owner'"),
      Row("Catalog Name", "fluss_catalog") :: Row("Namespace Name", "testdb") :: Row(
        "Comment",
        "created by spark") :: Nil
    )

    sql("DROP DATABASE testdb")
    checkAnswer(sql("SHOW DATABASES"), Row(DEFAULT_DATABASE) :: Nil)
  }

  test("Catalog: basic table") {
    sql(s"CREATE TABLE $DEFAULT_DATABASE.test_tbl (id int, name string) COMMENT 'my test table'")
    checkAnswer(sql("SHOW TABLES"), Row(DEFAULT_DATABASE, "test_tbl", false) :: Nil)
    checkAnswer(sql("DESC test_tbl"), Row("id", "int", null) :: Row("name", "string", null) :: Nil)

    val testTable = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, "test_tbl")).get()
    assertThat(testTable.getTablePath.getTableName).isEqualTo("test_tbl")
    assertThat(testTable.getComment.orElse(null)).isEqualTo("my test table")
    assertThat(testTable.getRowType).isEqualTo(
      RowType.builder().field("id", DataTypes.INT()).field("name", DataTypes.STRING()).build())

    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.test_pt_tbl (id int, name string, pt string)
           |PARTITIONED BY (pt)
           |TBLPROPERTIES("key" = "value")
           |""".stripMargin)

    val testPartitionedTable =
      admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, "test_pt_tbl")).get()
    assertThat(testPartitionedTable.getRowType).isEqualTo(
      RowType
        .builder()
        .field("id", DataTypes.INT())
        .field("name", DataTypes.STRING())
        .field("pt", DataTypes.STRING())
        .build())
    assertThat(testPartitionedTable.getPartitionKeys.get(0)).isEqualTo("pt")
    assertThat(testPartitionedTable.getCustomProperties.containsKey("key")).isEqualTo(true)
    assertThat(
      testPartitionedTable.getCustomProperties.getRawValue("key").get().asInstanceOf[String])
      .isEqualTo("value")

    sql("DROP TABLE test_tbl")
    sql("DROP TABLE test_pt_tbl")
    checkAnswer(sql("SHOW TABLES"), Nil)
  }

  test("Catalog: show tables") {
    withTable("test_tbl", "test_tbl1", "tbl_a") {
      sql(s"CREATE TABLE $DEFAULT_DATABASE.test_tbl (id int, name string) COMMENT 'my test table'")
      sql(
        s"CREATE TABLE $DEFAULT_DATABASE.test_tbl1 (id int, name string) COMMENT 'my test table1'")
      sql(s"CREATE TABLE $DEFAULT_DATABASE.tbl_a (id int, name string) COMMENT 'my table a'")

      checkAnswer(
        sql("SHOW TABLES"),
        Row("fluss", "test_tbl", false) :: Row("fluss", "test_tbl1", false) :: Row(
          "fluss",
          "tbl_a",
          false) :: Nil)

      checkAnswer(
        sql(s"SHOW TABLES in $DEFAULT_DATABASE"),
        Row("fluss", "test_tbl", false) :: Row("fluss", "test_tbl1", false) :: Row(
          "fluss",
          "tbl_a",
          false) :: Nil)

      checkAnswer(
        sql(s"SHOW TABLES from $DEFAULT_DATABASE"),
        Row("fluss", "test_tbl", false) :: Row("fluss", "test_tbl1", false) :: Row(
          "fluss",
          "tbl_a",
          false) :: Nil)

      checkAnswer(
        sql(s"SHOW TABLES from $DEFAULT_DATABASE like 'test_*'"),
        Row("fluss", "test_tbl", false) :: Row("fluss", "test_tbl1", false) :: Nil)
    }
  }

  test("Catalog: primary-key table") {
    sql(s"""
           |CREATE TABLE $DEFAULT_DATABASE.test_tbl (id int, name string, pt string)
           |PARTITIONED BY (pt)
           |TBLPROPERTIES("primary.key" = "id,pt")
           |""".stripMargin)

    val tbl1 = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, "test_tbl")).get()
    assertThatList(tbl1.getPrimaryKeys).hasSameElementsAs(Seq("id", "pt").toList.asJava)
    assertThat(tbl1.getNumBuckets).isEqualTo(1)
    assertThat(tbl1.getBucketKeys.contains("id")).isEqualTo(true)
    assertThat(tbl1.getPartitionKeys.contains("pt")).isEqualTo(true)

    sql(
      s"""
         |CREATE TABLE $DEFAULT_DATABASE.test_tbl2 (pk1 int, pk2 long, name string, pt1 string, pt2 string)
         |PARTITIONED BY (pt1, pt2)
         |TBLPROPERTIES("primary.key" = "pk1,pk2,pt1,pt2", "bucket.num" = 3, "bucket.key" = "pk1")
         |""".stripMargin)

    val tbl2 = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, "test_tbl2")).get()
    assertThatList(tbl2.getPrimaryKeys).hasSameElementsAs(
      Seq("pk1", "pk2", "pt1", "pt2").toList.asJava)
    assertThat(tbl2.getNumBuckets).isEqualTo(3)
    assertThatList(tbl2.getBucketKeys).hasSameElementsAs(Seq("pk1").toList.asJava)
  }

  test("Catalog: check namespace and table created by admin") {
    val dbName = "db_by_fluss_admin"
    val tblName = "tbl_by_fluss_admin"
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[SparkCatalog]

    // check namespace
    val dbDesc = DatabaseDescriptor.builder().comment("created by admin").build()
    admin.createDatabase(dbName, dbDesc, true).get()
    assert(catalog.namespaceExists(Array(dbName)))
    checkAnswer(sql("SHOW DATABASES"), Row(DEFAULT_DATABASE) :: Row(dbName) :: Nil)

    // check table
    val tablePath = TablePath.of(dbName, tblName)
    val rt = RowType
      .builder()
      .field("id", DataTypes.INT())
      .field("name", DataTypes.STRING())
      .field("pt", DataTypes.STRING())
      .build()
    val tableDesc = TableDescriptor
      .builder()
      .schema(Schema.newBuilder().fromRowType(rt).build())
      .partitionedBy("pt")
      .build()
    admin.createTable(tablePath, tableDesc, false).get()
    assert(
      catalog.tableExists(Identifier.of(Array(tablePath.getDatabaseName), tablePath.getTableName)))
    val expectDescTable = Seq(
      Row("id", "int", null),
      Row("name", "string", null),
      Row("pt", "string", null),
      Row("# Partition Information", "", ""),
      Row("# col_name", "data_type", "comment"),
      Row("pt", "string", null)
    )
    checkAnswer(
      sql(s"DESC $dbName.$tblName"),
      expectDescTable
    )

    admin.dropTable(tablePath, true).get()
    checkAnswer(sql(s"SHOW TABLES IN $dbName"), Nil)

    admin.dropDatabase(dbName, true, true).get()
    checkAnswer(sql("SHOW DATABASES"), Row(DEFAULT_DATABASE) :: Nil)
  }

  test("Partition: show partitions") {
    withTable("t") {
      sql(s"CREATE TABLE t (id int, name string, pt1 string, pt2 int) PARTITIONED BY (pt1, pt2)")
      sql(s"INSERT INTO t values(1, 'a', 'a', 1), (2, 'b', 'a', 2), (3, 'c', 'c', 3)")

      var expect = Seq(Row("pt1=a/pt2=1"), Row("pt1=a/pt2=2"), Row("pt1=c/pt2=3"))
      checkAnswer(sql(s"SHOW PARTITIONS t"), expect)
      expect = Seq(Row("pt1=a/pt2=1"), Row("pt1=a/pt2=2"))
      checkAnswer(sql(s"SHOW PARTITIONS t PARTITION (pt1 = 'a')"), expect)
    }
  }

  test("Partition: add partition") {
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

      intercept[AnalysisException](sql(s"ALTER TABLE t ADD PARTITION (pt1 = 'b', pt2 = 1)"))
      intercept[AnalysisException](sql(s"ALTER TABLE t ADD PARTITION (pt1 = 'b', pt3 = 1)"))
      intercept[PartitionsAlreadyExistException](
        sql(s"ALTER TABLE t ADD PARTITION (pt1 = 'b', pt2 = 1)"))
    }
  }

  test("Partition: drop partition") {
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

      // spark does not support drop partial partition
      intercept[AnalysisException](sql(s"ALTER TABLE t DROP PARTITION (pt1 = 'a')"))
    }
  }
}
