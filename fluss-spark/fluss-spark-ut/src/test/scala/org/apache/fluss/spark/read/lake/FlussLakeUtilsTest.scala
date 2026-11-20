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

package org.apache.fluss.spark.read.lake

import org.apache.fluss.config.ConfigOptions
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.utils.FileUtils

import org.apache.paimon.CoreOptions
import org.apache.paimon.catalog.{CatalogContext, CatalogFactory, Identifier}
import org.apache.paimon.data.{BinaryString, GenericRow}
import org.apache.paimon.options.Options
import org.apache.paimon.schema.Schema
import org.apache.paimon.table.FileStoreTable
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import java.util.{Collections, HashMap}

import scala.collection.JavaConverters._

/** Tests for creating branch-aware lake sources from Spark table properties. */
class FlussLakeUtilsTest extends AnyFunSuite {

  test("lake source reads the configured branch using its snapshot") {
    val warehouse = Files.createTempDirectory("fluss-spark-lake-branch")
    val catalogOptions = new Options()
    catalogOptions.setString("warehouse", warehouse.toUri.toString)
    val catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions))
    try {
      val tablePath = TablePath.of("lake_db", "events")
      val identifier = Identifier.create(tablePath.getDatabaseName, tablePath.getTableName)
      catalog.createDatabase(tablePath.getDatabaseName, true)
      catalog.createTable(
        identifier,
        Schema
          .newBuilder()
          .column("id", org.apache.paimon.types.DataTypes.INT())
          .column("payload", org.apache.paimon.types.DataTypes.STRING())
          .option(CoreOptions.BUCKET.key(), "-1")
          .build(),
        false
      )
      val main = catalog.getTable(identifier).asInstanceOf[FileStoreTable]
      main.branchManager().createBranch("rt")
      val branch = catalog
        .getTable(new Identifier(tablePath.getDatabaseName, tablePath.getTableName, "rt"))
        .asInstanceOf[FileStoreTable]
      Seq((main, 1, "main"), (branch, 2, "rt")).foreach {
        case (table, id, value) =>
          val builder = table.newBatchWriteBuilder()
          val write = builder.newWrite()
          val commit = builder.newCommit()
          try {
            write.write(GenericRow.of(Int.box(id), BinaryString.fromString(value)))
            commit.commit(write.prepareCommit())
          } finally {
            write.close()
            commit.close()
          }
      }

      // Both branches have snapshot 1, so reading main would return unrelated data rather than fail.
      val snapshotId = branch.snapshotManager().latestSnapshotId().longValue()
      assert(main.snapshotManager().latestSnapshotId().longValue() == snapshotId)
      val tableProperties = new HashMap[String, String]()
      tableProperties.put(ConfigOptions.TABLE_DATALAKE_FORMAT.key(), "paimon")
      tableProperties.put(ConfigOptions.TABLE_DATALAKE_BRANCH.key(), "rt")
      tableProperties.put("table.datalake.paimon.warehouse", warehouse.toUri.toString)
      val source = FlussLakeUtils.createLakeSource(
        Collections.emptyMap[String, String](),
        tableProperties,
        tablePath)
      val ids = source.createPlanner(() => snapshotId).plan().asScala.flatMap {
        split =>
          val records = source.createRecordReader(() => split).read()
          try {
            records.asScala.map(_.getRow.getInt(0)).toList
          } finally {
            records.close()
          }
      }
      assert(ids.toList == List(2))
    } finally {
      try {
        catalog.close()
      } finally {
        FileUtils.deleteDirectory(warehouse.toFile)
      }
    }
  }
}
