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

import org.apache.fluss.config.{ConfigOptions, Configuration, FlussConfigUtils}
import org.apache.fluss.exception.FlussRuntimeException
import org.apache.fluss.lake.paimon.utils.PaimonConversions
import org.apache.fluss.metadata._
import org.apache.fluss.metadata.TableDescriptor.{BUCKET_COLUMN_NAME, OFFSET_COLUMN_NAME, TIMESTAMP_COLUMN_NAME}
import org.apache.fluss.server.utils.LakeStorageUtils
import org.apache.fluss.spark.SparkCatalogTest
import org.apache.fluss.spark.SparkConnectorOptions.{BUCKET_KEY, BUCKET_NUMBER, PRIMARY_KEY}

import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}

import java.nio.file.Files

import scala.collection.JavaConverters._

class SparkLakePaimonCatalogTest extends SparkCatalogTest {

  private var paimonCatalog: org.apache.paimon.catalog.Catalog = _
  private var warehousePath: String = _

  override protected def lakeFormat: Option[DataLakeFormat] = Some(DataLakeFormat.PAIMON)

  override def flussConf: Configuration = {
    val conf = super.flussConf
    conf.setInt(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE)
    conf.setString("datalake.format", lakeFormat.get.toString)
    conf.setString("datalake.paimon.metastore", "filesystem")
    conf.setString("datalake.paimon.cache-enabled", "false")
    try {
      warehousePath =
        Files.createTempDirectory("fluss-testing-datalake-tiered").resolve("warehouse").toString
    } catch {
      case e: Exception =>
        throw new FlussRuntimeException("Failed to create warehouse path")
    }
    conf.setString("datalake.paimon.warehouse", warehousePath)
    paimonCatalog = org.apache.paimon.catalog.CatalogFactory.createCatalog(
      org.apache.paimon.catalog.CatalogContext.create(
        org.apache.paimon.options.Options.fromMap(LakeStorageUtils.extractLakeProperties(conf))))
    conf
  }

  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    super.withTable(tableNames: _*)(f)
    tableNames.foreach(
      tableName =>
        paimonCatalog.dropTable(
          org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, tableName),
          true))
  }

  private def verifyLakePaimonTable(
      paimonTable: org.apache.paimon.table.Table,
      flussTable: TableInfo,
      expectedPaimonRowType: org.apache.paimon.types.RowType,
      expectedBucketKey: String,
      bucketNum: Int): Unit = {
    if (!flussTable.hasPrimaryKey) {
      assert(paimonTable.primaryKeys.isEmpty)
    } else {
      assertResult(flussTable.getSchema.getPrimaryKey.get().getColumnNames, "check primary key")(
        paimonTable.primaryKeys())
    }

    if (flussTable.isPartitioned) {
      assertResult(flussTable.getPartitionKeys, "check partition keys")(paimonTable.partitionKeys())
    }

    assert(flussTable.getNumBuckets == bucketNum)

    if (expectedBucketKey != null) {
      assert(
        paimonTable
          .options()
          .asScala
          .getOrElse(
            org.apache.paimon.CoreOptions.BUCKET.key(),
            org.apache.paimon.CoreOptions.BUCKET.defaultValue().toString)
          .toInt == bucketNum)
    }

    if (flussTable.hasBucketKey) {
      assertResult(expectedBucketKey, "check fluss table bucket key")(
        flussTable.getBucketKeys.asScala.mkString(","))
      assertResult(expectedBucketKey, "check paimon table bucket key")(
        paimonTable.options().get(org.apache.paimon.CoreOptions.BUCKET_KEY.key()))
    }

    val expectedProperties =
      (flussTable.getProperties.toMap.asScala ++ flussTable.getCustomProperties.toMap.asScala)
        .filterNot(_._1.startsWith(FlussConfigUtils.TABLE_PREFIX + "datalake."))
        .map {
          case (k, v) =>
            if (k.startsWith("paimon.")) {
              (k.substring("paimon.".length), v)
            } else {
              (s"fluss.$k", v)
            }
        }
        .toMap
    paimonTable.options().asScala.should(contain).allElementsOf(expectedProperties)

    val paimonRowType = paimonTable.rowType
    assert(paimonRowType.getFieldCount == expectedPaimonRowType.getFieldCount)
    paimonRowType.getFields.asScala.zip(expectedPaimonRowType.getFields.asScala).foreach {
      case (actual, expect) =>
        assert(actual.equalsIgnoreFieldId(expect), s"check table schema: $actual vs $expect")
    }

    assert(flussTable.getComment.equals(paimonTable.comment()), "check table comments")
  }

  test("Lake Catalog: basic table") {
    // bucket log table
    var tableName = "bucket_log_table"
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id int, name string)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true, '${BUCKET_KEY.key()}' = 'id',
             |  '${BUCKET_NUMBER.key()}' = 2, 'k1' = 'v1', 'paimon.file.format' = 'parquet')
             |""".stripMargin)

      val flussTable = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, tableName)).get()
      val paimonTable =
        paimonCatalog.getTable(
          org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, tableName))
      verifyLakePaimonTable(
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

    // non-bucket log table
    tableName = "non_bucket_log_table"
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id int, name string)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${BUCKET_NUMBER.key()}' = 2, 'k1' = 'v1', 'paimon.file.format' = 'parquet')
             |""".stripMargin)

      val flussTable = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, tableName)).get()
      val paimonTable =
        paimonCatalog.getTable(
          org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, tableName))
      verifyLakePaimonTable(
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
        null,
        2
      )
    }

    // pk table
    tableName = "pk_table"
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id int, name string, pk1 string)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true, '${BUCKET_KEY.key()}' = 'id',
             |  '${PRIMARY_KEY.key()}' = 'pk1, id',
             |  '${BUCKET_NUMBER.key()}' = 2, 'k1' = 'v1', 'paimon.file.format' = 'parquet')
             |""".stripMargin)

      val flussTable = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, tableName)).get()
      val paimonTable =
        paimonCatalog.getTable(
          org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, tableName))
      verifyLakePaimonTable(
        paimonTable,
        flussTable,
        org.apache.paimon.types.RowType
          .of(
            Array.apply(
              org.apache.paimon.types.DataTypes.INT.notNull(),
              org.apache.paimon.types.DataTypes.STRING,
              org.apache.paimon.types.DataTypes.STRING.notNull(),
              org.apache.paimon.types.DataTypes.INT,
              org.apache.paimon.types.DataTypes.BIGINT,
              org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS
            ),
            Array.apply(
              "id",
              "name",
              "pk1",
              BUCKET_COLUMN_NAME,
              OFFSET_COLUMN_NAME,
              TIMESTAMP_COLUMN_NAME)
          ),
        "id",
        2
      )
    }

    // partitioned pk table
    tableName = "partitioned_pk_table"
    withTable(tableName) {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.$tableName (id int, name string, pk1 string, pt1 string)
             | PARTITIONED BY (pt1)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true, '${BUCKET_KEY.key()}' = 'id',
             |  '${PRIMARY_KEY.key()}' = 'pk1, id, pt1',
             |  '${BUCKET_NUMBER.key()}' = 2, 'k1' = 'v1', 'paimon.file.format' = 'parquet')
             |""".stripMargin)

      val flussTable = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, tableName)).get()
      val paimonTable =
        paimonCatalog.getTable(
          org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, tableName))
      verifyLakePaimonTable(
        paimonTable,
        flussTable,
        org.apache.paimon.types.RowType
          .of(
            Array.apply(
              org.apache.paimon.types.DataTypes.INT.notNull(),
              org.apache.paimon.types.DataTypes.STRING,
              org.apache.paimon.types.DataTypes.STRING.notNull(),
              org.apache.paimon.types.DataTypes.STRING.notNull(),
              org.apache.paimon.types.DataTypes.INT,
              org.apache.paimon.types.DataTypes.BIGINT,
              org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS
            ),
            Array.apply(
              "id",
              "name",
              "pk1",
              "pt1",
              BUCKET_COLUMN_NAME,
              OFFSET_COLUMN_NAME,
              TIMESTAMP_COLUMN_NAME)
          ),
        "id",
        2
      )
    }
  }

  test("Lake Catalog: table with all types") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t
             | (c1 boolean, c2 byte, c3 short, c4 int, c5 long, c6 float, c7 double, c8 date,
             |  c9 timestamp, c10 timestamp_ntz, c11 string, c12 binary, c13 decimal(10, 2),
             |  c14 array<int>, c15 struct<a int, b string>, c16 map<string, int>)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
             |  '${BUCKET_NUMBER.key()}' = 2, 'k1' = 'v1', 'paimon.file.format' = 'parquet')
             |""".stripMargin)

      val flussTable = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, "t")).get()
      val paimonTable =
        paimonCatalog.getTable(org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, "t"))
      verifyLakePaimonTable(
        paimonTable,
        flussTable,
        org.apache.paimon.types.RowType
          .of(
            Array.apply(
              org.apache.paimon.types.DataTypes.BOOLEAN,
              org.apache.paimon.types.DataTypes.TINYINT,
              org.apache.paimon.types.DataTypes.SMALLINT,
              org.apache.paimon.types.DataTypes.INT,
              org.apache.paimon.types.DataTypes.BIGINT,
              org.apache.paimon.types.DataTypes.FLOAT,
              org.apache.paimon.types.DataTypes.DOUBLE,
              org.apache.paimon.types.DataTypes.DATE,
              org.apache.paimon.types.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
              org.apache.paimon.types.DataTypes.TIMESTAMP,
              org.apache.paimon.types.DataTypes.STRING,
              org.apache.paimon.types.DataTypes.BYTES,
              org.apache.paimon.types.DataTypes.DECIMAL(10, 2),
              org.apache.paimon.types.DataTypes.ARRAY(org.apache.paimon.types.DataTypes.INT),
              org.apache.paimon.types.DataTypes.ROW(
                org.apache.paimon.types.DataTypes
                  .FIELD(0, "a", org.apache.paimon.types.DataTypes.INT),
                org.apache.paimon.types.DataTypes
                  .FIELD(1, "b", org.apache.paimon.types.DataTypes.STRING)
              ),
              org.apache.paimon.types.DataTypes.MAP(
                org.apache.paimon.types.DataTypes.STRING.notNull(),
                org.apache.paimon.types.DataTypes.INT),
              org.apache.paimon.types.DataTypes.INT,
              org.apache.paimon.types.DataTypes.BIGINT,
              org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS
            ),
            Array.apply(
              "c1",
              "c2",
              "c3",
              "c4",
              "c5",
              "c6",
              "c7",
              "c8",
              "c9",
              "c10",
              "c11",
              "c12",
              "c13",
              "c14",
              "c15",
              "c16",
              BUCKET_COLUMN_NAME,
              OFFSET_COLUMN_NAME,
              TIMESTAMP_COLUMN_NAME)
          ),
        null,
        2
      )
    }
  }

  test("Lake Catalog: unsettable properties") {
    withTable("t") {
      val unsettableProperties =
        PaimonConversions.PAIMON_UNSETTABLE_OPTIONS.asScala.map(e => s"'$e' = 'v'").mkString(", ")

      intercept[java.util.concurrent.ExecutionException] {
        sql(s"""
               |CREATE TABLE $DEFAULT_DATABASE.t (id int, name string)
               | TBLPROPERTIES (
               |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true,
               |  $unsettableProperties,
               |  '${BUCKET_NUMBER.key()}' = 2, 'k1' = 'v1', 'paimon.file.format' = 'parquet')
               |""".stripMargin)
      }.getCause.shouldBe(a[org.apache.fluss.exception.InvalidConfigException])
    }
  }

  test("Lake Catalog: alter table with lake enabled") {
    withTable("t") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.t (id int, name string)
             | TBLPROPERTIES (
             |  '${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = false,
             |  '${BUCKET_NUMBER.key()}' = 2, 'k1' = 'v1', 'paimon.file.format' = 'parquet')
             |""".stripMargin)
      intercept[org.apache.paimon.catalog.Catalog.TableNotExistException] {
        paimonCatalog.getTable(org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, "t"))
      }

      sql(
        s"ALTER TABLE $DEFAULT_DATABASE.t SET TBLPROPERTIES ('${ConfigOptions.TABLE_DATALAKE_ENABLED.key()}' = true)")

      val flussTable = admin.getTableInfo(TablePath.of(DEFAULT_DATABASE, "t")).get()
      val paimonTable =
        paimonCatalog.getTable(org.apache.paimon.catalog.Identifier.create(DEFAULT_DATABASE, "t"))
      verifyLakePaimonTable(
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
        null,
        2
      )
    }
  }
}
