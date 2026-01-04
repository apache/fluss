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

package org.apache.fluss.spark.write

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.table.Table
import org.apache.fluss.client.table.writer.{AppendResult, AppendWriter, TableWriter, UpsertResult, UpsertWriter}
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.spark.row.SparkAsFlussRow

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.io.IOException
import java.util.concurrent.CompletableFuture

/**
 * A fluss implementation of Spark [[WriterCommitMessage]]. Fluss, as a service, accepts data and
 * commit inside of it, so client does nothing.
 */
case class FlussWriterCommitMessage() extends WriterCommitMessage

/** An abstract class to Spark [[DataWriter]]. */
abstract class FlussDataWriter[T](
    tablePath: TablePath,
    dataSchema: StructType,
    flussConfig: Configuration)
  extends DataWriter[InternalRow]
  with Logging {

  private lazy val conn: Connection = ConnectionFactory.createConnection(flussConfig)

  lazy val table: Table = conn.getTable(tablePath)

  val writer: TableWriter

  protected val flussRow = new SparkAsFlussRow(dataSchema)

  @volatile
  private var asyncWriterException: Option[Throwable] = _

  def writeRow(record: SparkAsFlussRow): CompletableFuture[T]

  override def write(record: InternalRow): Unit = {
    checkAsyncException()

    writeRow(flussRow.replace(record)).whenComplete {
      (_, exception) =>
        if (exception != null && asyncWriterException.isEmpty) {
          asyncWriterException = Some(exception)
        }
    }
  }

  override def commit(): WriterCommitMessage = {
    writer.flush()
    checkAsyncException()

    FlussWriterCommitMessage()
  }

  override def abort(): Unit = this.close()

  override def close(): Unit = {
    if (table != null) {
      table.close()
    }
    if (conn != null) {
      conn.close()
    }

    // Rethrow exception for the case in which close is called before write() and commit().
    checkAsyncException()

    logInfo("Finished closing Fluss data write.")
  }

  @throws[IOException]
  private def checkAsyncException(): Unit = {
    val throwable = asyncWriterException
    throwable match {
      case Some(exception) =>
        asyncWriterException = None
        logError("Exception occurs while write row to fluss.", exception)
        throw new IOException(
          "One or more Fluss Writer send requests have encountered exception",
          exception)
      case _ =>
    }
  }

}

/** Spark-Fluss Append Data Writer. */
case class FlussAppendDataWriter(
    tablePath: TablePath,
    dataSchema: StructType,
    flussConfig: Configuration)
  extends FlussDataWriter[AppendResult](tablePath, dataSchema, flussConfig) {

  override val writer: AppendWriter = table.newAppend().createWriter()

  override def writeRow(record: SparkAsFlussRow): CompletableFuture[AppendResult] = {
    writer.append(record)
  }
}

/** Spark-Fluss Upsert Data Writer. */
case class FlussUpsertDataWriter(
    tablePath: TablePath,
    dataSchema: StructType,
    flussConfig: Configuration)
  extends FlussDataWriter[UpsertResult](tablePath, dataSchema, flussConfig) {

  override val writer: UpsertWriter = table.newUpsert().createWriter()

  override def writeRow(record: SparkAsFlussRow): CompletableFuture[UpsertResult] = {
    writer.upsert(record)
  }
}
