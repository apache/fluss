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

package org.apache.fluss.spark.lake.paimon

import org.apache.fluss.config.ConfigOptions
import org.apache.fluss.metadata._
import org.apache.fluss.metadata.TableDescriptor.{BUCKET_COLUMN_NAME, OFFSET_COLUMN_NAME, TIMESTAMP_COLUMN_NAME}
import org.apache.fluss.spark.SparkCatalog
import org.apache.fluss.spark.SparkConnectorOptions.{BUCKET_KEY, BUCKET_NUMBER}
import org.apache.fluss.types.{DataTypes, RowType}

import org.apache.paimon.CoreOptions
import org.apache.paimon.table.Table
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.Identifier
import org.assertj.core.api.Assertions.{assertThat, assertThatList}

import scala.collection.JavaConverters._

class SparkLakePaimonCatalogTest extends SparkLakePaimonTestBase {

  private def verifyPaimonTable(
      paimonTable: Table,
      flussTable: TableInfo,
      expectedPaimonRowType: org.apache.paimon.types.RowType,
      expectedBucketKey: String,
      bucketNum: Int): Unit = {
    if (!flussTable.hasPrimaryKey) {
      assert(paimonTable.primaryKeys.isEmpty)
    } else {
      assertResult(flussTable.getSchema.getPrimaryKey.get().getColumnNames, "primary key")(
        paimonTable.primaryKeys())
    }

    if (flussTable.isPartitioned) {
      assertResult(flussTable.getPartitionKeys, "partition keys")(paimonTable.partitionKeys())
    }

    assert(flussTable.getNumBuckets == bucketNum)
    assert(paimonTable.options().get(CoreOptions.BUCKET.key()).toInt == bucketNum)
    if (flussTable.hasBucketKey) {
      assertResult(expectedBucketKey)(flussTable.getBucketKeys.asScala.mkString(","))
      assertResult(expectedBucketKey)(paimonTable.options().get(CoreOptions.BUCKET_KEY.key()))
    }

    val paimonRowType = paimonTable.rowType
    assert(paimonRowType.equals(expectedPaimonRowType))
    assert(flussTable.getComment.equals(paimonTable.comment()))
  }

  test("Lake Catalog: basic table") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (id int, name string)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true, '${BUCKET_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 2, 'k1' = 'v1', 'paimon.file.format' = 'parquet')
             |""".stripMargin)

      val flussTable = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, "t")).get()
      val paimonTable =
        paimonCatalog.getTable(org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, "t"))
      verifyPaimonTable(
        paimonTable,
        flussTable,
        org.apache.paimon.types.RowType
          .of(
            Array.apply(
              org.apache.paimon.types.DataTypes.INT,
              org.apache.paimon.types.DataTypes.STRING,
              org.apache.paimon.types.DataTypes.INT,
              org.apache.paimon.types.DataTypes.BIGINT,
              org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS
            ),
            Array.apply("id", "name", BUCKET_COLUMN_NAME, OFFSET_COLUMN_NAME, TIMESTAMP_COLUMN_NAME)
          ),
        "id",
        2
      )
    }
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
}
