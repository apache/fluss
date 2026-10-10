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

package org.apache.fluss.spark.utils

import org.apache.fluss.client.table.writer.{AppendResult, TableWriter}
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{TableBucket, TablePath}
import org.apache.fluss.spark.{FlussSparkTestBase, SparkConversions, SparkWriteTest}
import org.apache.fluss.spark.read.{AppendPlanner, FlussAppendInputPartition, FlussAppendPartitionReader, FlussPartitionReader}
import org.apache.fluss.spark.row.SparkAsFlussRow
import org.apache.fluss.spark.write.{FlussAppendDataWriter, FlussDataWriter}
import org.apache.fluss.types.RowType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}

import java.io.IOException
import java.util.Collections
import java.util.concurrent.CompletableFuture

/** Exercises connection ownership with real Fluss writers, readers and driver planners. */
class FlussConnectionReuseTest extends FlussSparkTestBase {

  test("a completed writer returns a connection while another task continues writing") {
    val path = createTablePath("shared_writers")
    createFlussTable(path, SparkWriteTest.logTableDescriptor)
    val config = flussServer.getClientConfig
    val first = writer(path, config)
    val second = writer(path, config)
    val witness = FlussConnectionCache.acquire(config)
    try {
      first.write(row(1))
      first.commit()
      first.close()
      first.close()
      val reused = FlussConnectionCache.acquire(config)
      try {
        assertThat(reused.connection).isSameAs(witness.connection)
      } finally {
        reused.close()
      }
      second.write(row(2))
      second.commit()
      second.close()
      val table = loadFlussTable(path)
      try {
        assertThat(getRowsWithChangeType(table).length).isEqualTo(2)
      } finally {
        table.close()
      }
    } finally {
      first.close()
      second.close()
      witness.close()
    }
  }

  test("aborting one writer replaces the cached connection without interrupting an active writer") {
    val path = createTablePath("aborted_writer")
    createFlussTable(path, SparkWriteTest.logTableDescriptor)
    val config = flussServer.getClientConfig
    val aborted = writer(path, config)
    val surviving = writer(path, config)
    val witness = FlussConnectionCache.acquire(config)
    try {
      aborted.abort()
      aborted.close()
      val replacement = FlussConnectionCache.acquire(config)
      try {
        assertThat(replacement.connection).isNotSameAs(witness.connection)
        surviving.write(row(1))
        surviving.commit()
        surviving.close()
        assertThat(replacement.connection.getAdmin.getTableInfo(path).get().getTablePath)
          .isEqualTo(path)
      } finally {
        replacement.close()
      }
    } finally {
      aborted.close()
      surviving.close()
      witness.close()
    }
  }

  test("a writer for a recreated table uses a fresh connection") {
    val path = createTablePath("recreated_writer")
    createFlussTable(path, SparkWriteTest.logTableDescriptor)
    val config = flussServer.getClientConfig
    val first = writer(path, config)
    val witness = FlussConnectionCache.acquire(config)
    try {
      first.write(row(1))
      first.commit()
      first.close()
      admin.dropTable(path, false).get()
      createFlussTable(path, SparkWriteTest.logTableDescriptor)
      val second = writer(path, config)
      val replacement = FlussConnectionCache.acquire(config)
      try {
        assertThat(replacement.connection).isNotSameAs(witness.connection)
        second.write(row(2))
        second.commit()
        second.close()
        val table = loadFlussTable(path)
        try {
          assertThat(getRowsWithChangeType(table).length).isEqualTo(1)
        } finally {
          table.close()
        }
      } finally {
        second.close()
        replacement.close()
      }
    } finally {
      first.close()
      witness.close()
    }
  }

  test("an asynchronous write failure remains invalid after its exception is consumed") {
    val path = createTablePath("failed_callback")
    createFlussTable(path, SparkWriteTest.logTableDescriptor)
    val config = flussServer.getClientConfig
    val result = new CompletableFuture[AppendResult]()
    val failed = new FlussDataWriter[AppendResult](
      path,
      SparkConversions.toSparkDataType(SparkWriteTest.logSchema.getRowType),
      config) {
      override val writer: TableWriter = table.newAppend().createWriter()
      override def writeRow(record: SparkAsFlussRow): CompletableFuture[AppendResult] = result
    }
    val witness = FlussConnectionCache.acquire(config)
    try {
      failed.write(row(1))
      val failure = new IOException("write failed")
      result.completeExceptionally(failure)
      assertThatThrownBy(() => failed.commit())
        .isInstanceOf(classOf[IOException])
        .hasCause(failure)
      failed.close()
      val replacement = FlussConnectionCache.acquire(config)
      try {
        assertThat(replacement.connection).isNotSameAs(witness.connection)
      } finally {
        replacement.close()
      }
    } finally {
      failed.close()
      witness.close()
    }
  }

  test("reader construction failures and interruptions return their connection borrow") {
    val path = createTablePath("failed_reader")
    createFlussTable(path, SparkWriteTest.logTableDescriptor)
    val info = admin.getTableInfo(path).get()
    val config = flussServer.getClientConfig
    val failures: Seq[(() => FlussPartitionReader, Class[_ <: Throwable])] = Seq(
      (
        () =>
          new FlussAppendPartitionReader(
            path,
            Array(0, 1, 2, 3),
            None,
            None,
            FlussAppendInputPartition(new TableBucket(info.getTableId, 0), 0, 0),
            config),
        classOf[IllegalArgumentException]),
      (
        () =>
          new FlussPartitionReader(path, config, None) {
            initializeResources {
              require(table != null)
              throw new InterruptedException("reader interrupted")
            }
            override def next0(): Boolean = false
            override def close0(): Unit = ()
            override protected def projectedRowType: RowType = rowType
          },
        classOf[InterruptedException])
    )
    for ((construct, errorType) <- failures) {
      val witness = FlussConnectionCache.acquire(config)
      try {
        assertThatThrownBy(() => construct()).isInstanceOf(errorType)
        witness.close()
        FlussConnectionCache.clearIdleConnections()
        assertThatThrownBy(() => witness.connection.getTable(path))
          .isInstanceOf(classOf[Exception])
      } finally {
        witness.close()
      }
    }
  }

  test("writer construction failures and interruptions invalidate and return their borrow") {
    val path = createTablePath("failed_writer")
    createFlussTable(path, SparkWriteTest.logTableDescriptor)
    val config = flussServer.getClientConfig
    val schema = SparkConversions.toSparkDataType(SparkWriteTest.logSchema.getRowType)
    val failures: Seq[(() => FlussDataWriter[AppendResult], Class[_ <: Throwable])] = Seq(
      (() => writer(createTablePath("missing_table"), config), classOf[Exception]),
      (
        () =>
          new FlussDataWriter[AppendResult](path, schema, config) {
            override val writer: TableWriter = initializeWriter {
              require(table != null)
              throw new InterruptedException("writer interrupted")
            }
            override def writeRow(record: SparkAsFlussRow): CompletableFuture[AppendResult] =
              new CompletableFuture[AppendResult]()
          },
        classOf[InterruptedException])
    )
    for ((construct, errorType) <- failures) {
      val witness = FlussConnectionCache.acquire(config)
      try {
        assertThatThrownBy(() => construct()).isInstanceOf(errorType)
        witness.close()
        // An invalidated connection closes after its last borrow, without idle eviction.
        assertThatThrownBy(() => witness.connection.getTable(path))
          .isInstanceOf(classOf[Exception])
      } finally {
        witness.close()
      }
    }
  }

  test("driver planning returns the lease without closing the shared connection") {
    val path = createTablePath("shared_planners")
    createFlussTable(path, SparkWriteTest.logTableDescriptor)
    val info = admin.getTableInfo(path).get()
    val config = flussServer.getClientConfig
    val witness = FlussConnectionCache.acquire(config)
    try {
      for (_ <- 0 until 2) {
        val planner = new AppendPlanner(
          path,
          info,
          None,
          None,
          Array(0, 1, 2, 3),
          new CaseInsensitiveStringMap(Collections.emptyMap[String, String]()),
          config)
        planner.plan()
        planner.close()
        assertThat(witness.connection.getAdmin.getTableInfo(path).get().getTablePath)
          .isEqualTo(path)
      }
      witness.close()
      FlussConnectionCache.clearIdleConnections()
      assertThatThrownBy(() => witness.connection.getTable(path))
        .isInstanceOf(classOf[Exception])
    } finally {
      witness.close()
    }
  }

  private def writer(path: TablePath, config: Configuration): FlussAppendDataWriter = {
    new FlussAppendDataWriter(
      path,
      SparkConversions.toSparkDataType(SparkWriteTest.logSchema.getRowType),
      config)
  }

  private def row(id: Long): InternalRow = {
    InternalRow(id, id, 100, UTF8String.fromString("address"))
  }
}
