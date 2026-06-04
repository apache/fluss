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

package org.apache.fluss.spark.tiering

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.admin.Admin
import org.apache.fluss.client.tiering.{FlussTableLakeSnapshotCommitter, TableBucketWriteResult, TieringCommitter}
import org.apache.fluss.config.Configuration
import org.apache.fluss.lake.committer.TieringStats
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp
import org.apache.fluss.lake.writer.LakeTieringFactory
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.spark.tiering.SparkTieringJobRunner.submit

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration => ScalaDuration}

/**
 * Main driver loop for the Spark tiering service.
 *
 * Orchestrates a long-running loop: heartbeat -> request table -> generate splits -> parallel RDD
 * job -> commit. Processes one table at a time. The loop runs asynchronously via [[startAsync()]]
 * on a dedicated daemon thread and terminates cooperatively when [[stop()]] is called.
 *
 * The [[LakeTieringFactory]] is created internally from `dataLakeFormat` and `lakeConfig`. Tiering
 * options (poll interval, heartbeat interval, poll timeout, max heartbeat failures) are read from
 * `flussConfig` via [[SparkTieringOptions]].
 */
class SparkTieringJobRunner(
    spark: SparkSession,
    flussConfig: Configuration,
    lakeTieringConfig: Configuration,
    dataLakeFormat: String,
    lakeConfig: Configuration,
    coordinatorFactory: Configuration => TieringCoordinator = new TieringCoordinator(_),
    snapshotCommitterFactory: Configuration => FlussTableLakeSnapshotCommitter =
      new FlussTableLakeSnapshotCommitter(_),
    connectionFactory: Configuration => Connection = ConnectionFactory.createConnection,
    splitGeneratorFactory: Admin => TieringSplitGenerator = new TieringSplitGenerator(_))
  extends Logging {

  // Read tiering options
  private val pollIntervalMs =
    flussConfig.get(SparkTieringOptions.POLL_TIERING_TABLE_INTERVAL).toMillis
  private val heartbeatIntervalMs =
    flussConfig.get(SparkTieringOptions.HEARTBEAT_INTERVAL).toMillis
  private val pollTimeoutMs =
    flussConfig.get(SparkTieringOptions.POLL_TIMEOUT).toMillis
  private val maxHeartbeatFailures =
    flussConfig.get(SparkTieringOptions.MAX_HEARTBEAT_FAILURES).intValue()

  private val lakeStoragePlugin = LakeStoragePluginSetUp.fromDataLakeFormat(dataLakeFormat, null)
  private val lakeStorage = lakeStoragePlugin.createLakeStorage(lakeConfig)
  private val lakeTieringFactory = lakeStorage
    .createLakeTieringFactory()
    .asInstanceOf[LakeTieringFactory[AnyRef, AnyRef]]

  private val stopped: AtomicBoolean = new AtomicBoolean(false)
  private val terminationPromise: Promise[Unit] = Promise[Unit]()

  private val loopExecutionContext: ExecutionContext = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(
      (r: Runnable) => {
        val t = new Thread(r, "SparkTiering-MainLoop")
        t.setDaemon(true)
        t
      })
  )

  /**
   * Starts the tiering service loop asynchronously on a dedicated single-thread ExecutionContext.
   *
   * @return
   *   a Future that completes when the loop exits normally or fails on exception
   */
  def startAsync(): Future[Unit] = {
    val f = Future {
      run()
    }(loopExecutionContext)
    terminationPromise.completeWith(f)
    terminationPromise.future
  }

  /**
   * Blocks until the tiering loop finishes or the timeout expires.
   *
   * @throws java.util.concurrent.TimeoutException
   *   if the loop has not terminated within the given timeout
   */
  def stop(timeout: ScalaDuration = ScalaDuration(60, TimeUnit.SECONDS)): Unit = {
    stopped.set(true)
    try {
      Await.result(terminationPromise.future, timeout)
    } catch {
      case _: Exception =>
        logWarning("Timed out waiting for tiering loop to finish.")
    }
  }

  private def run(): Unit = {
    val sc = spark.sparkContext
    val coordinator = coordinatorFactory(flussConfig)
    val snapshotCommitter = snapshotCommitterFactory(flussConfig)
    var connection: Connection = null
    var admin: Admin = null

    try {
      coordinator.open()
      snapshotCommitter.open()
      connection = connectionFactory(flussConfig)
      admin = connection.getAdmin

      val splitGenerator = splitGeneratorFactory(admin)
      val consecutiveHeartbeatFailures = new AtomicInteger(0)

      // Register shutdown hook to report all in-progress tables as failed
      val shutdownHook = new Thread("SparkTiering-ShutdownHook") {
        override def run(): Unit = {
          logInfo("Shutdown hook triggered, stopping tiering service...")
          stopped.set(true)
          coordinator.close()
        }
      }
      Runtime.getRuntime.addShutdownHook(shutdownHook)

      logInfo("Spark tiering service started. Entering main loop.")

      while (!stopped.get()) {
        try {
          val tableAssignment = coordinator.heartbeatAndRequestTable()
          consecutiveHeartbeatFailures.set(0)

          tableAssignment match {
            case Some((tableId, tieringEpoch, tablePath)) =>
              processTieringTable(
                sc,
                coordinator,
                snapshotCommitter,
                admin,
                splitGenerator,
                consecutiveHeartbeatFailures,
                tableId,
                tieringEpoch,
                tablePath)

            case None =>
              logInfo(s"No tiering table available, poll later after ${pollIntervalMs}ms.")
              if (stopped.get()) {
                logInfo("Stop was called while polling for table, exiting loop.")
              }
              Thread.sleep(pollIntervalMs)
          }
        } catch {
          case _: InterruptedException =>
            logInfo("Main loop interrupted, stopping.")
            stopped.set(true)
          case e: Exception =>
            val failures = consecutiveHeartbeatFailures.incrementAndGet()
            logWarning(s"Heartbeat failed ($failures/$maxHeartbeatFailures)", e)
            if (failures >= maxHeartbeatFailures) {
              logError(
                s"Max consecutive heartbeat failures ($maxHeartbeatFailures) exceeded." +
                  " Failing job.")
              throw new IllegalStateException(
                s"Spark tiering job failed: exceeded max heartbeat failures ($maxHeartbeatFailures)",
                e)
            }
            Thread.sleep(pollIntervalMs)
        }
      }

      logInfo("Spark tiering service main loop exited.")
    } finally {
      closeQuietly(snapshotCommitter, "FlussTableLakeSnapshotCommitter")
      closeQuietly(coordinator, "TieringCoordinator")
      closeQuietly(admin, "Admin")
      closeQuietly(connection, "Connection")
    }
  }

  private def processTieringTable(
      sc: SparkContext,
      coordinator: TieringCoordinator,
      snapshotCommitter: FlussTableLakeSnapshotCommitter,
      admin: Admin,
      splitGenerator: TieringSplitGenerator,
      consecutiveHeartbeatFailures: AtomicInteger,
      tableId: Long,
      tieringEpoch: Long,
      tablePath: TablePath): Unit = {
    logInfo(s"Starting tiering for table: tableId=$tableId, epoch=$tieringEpoch, path=$tablePath")

    try {
      // Get table info
      val tableInfo = admin.getTableInfo(tablePath).get()

      // Generate splits
      val rawSplits = splitGenerator.generateTableSplits(tableInfo)

      if (rawSplits.isEmpty) {
        logInfo(s"No splits generated for table $tablePath, marking as finished.")
        coordinator.markTableFinished(tableId, tieringEpoch, TieringStats.UNKNOWN)
        return
      }

      // Populate numberOfSplits on each split
      val totalSplits = rawSplits.size
      val splits = rawSplits.map {
        case s: TieringLogSplit => s.copy(numberOfSplits = totalSplits)
        case s: TieringSnapshotSplit => s.copy(numberOfSplits = totalSplits)
      }

      // Register tiering in coordinator state
      coordinator.registerTiering(tableId, tieringEpoch)

      // Run RDD job with background heartbeat
      val results =
        runTieringRddJob(sc, coordinator, consecutiveHeartbeatFailures, splits, tablePath, tableId)

      // Check if table was cancelled (dropped/recreated detected by heartbeat thread)
      if (coordinator.tableCancelled) {
        logWarning(s"Table $tablePath was cancelled during tiering (likely dropped/recreated).")
        coordinator.tableCancelled = false
        coordinator.markTableFailed(tableId, tieringEpoch)
        return
      }

      // Commit results
      val tieringCommitter = new TieringCommitter[AnyRef, AnyRef]()
      val stats = tieringCommitter.commitWriteResults(
        admin,
        tableId,
        tablePath,
        flussConfig,
        lakeTieringConfig,
        lakeTieringFactory,
        snapshotCommitter,
        results.asJava)

      logInfo(s"Tiering completed for table $tablePath, stats=$stats")
      coordinator.markTableFinished(tableId, tieringEpoch, stats.stats)

    } catch {
      case e: Exception =>
        logError(s"Tiering failed for table $tablePath (epoch=$tieringEpoch)", e)
        coordinator.markTableFailed(tableId, tieringEpoch)
    }
  }

  /**
   * Runs the tiering RDD job with a background heartbeat thread.
   *
   * Delegates to [[SparkTieringJobRunner.submit()]] to create the RDD. Configs are captured in the
   * closure and serialized to executors. Each executor task re-initializes the
   * [[LakeTieringFactory]] from `dataLakeFormat` and `lakeConfig`. A background scheduled thread
   * sends keepalive heartbeats to the coordinator during the RDD job; if max failures are exceeded,
   * it cancels the Spark job group.
   */
  private def runTieringRddJob(
      sc: SparkContext,
      coordinator: TieringCoordinator,
      consecutiveHeartbeatFailures: AtomicInteger,
      splits: Seq[TieringSplit],
      tablePath: TablePath,
      tableId: Long): Seq[TableBucketWriteResult[AnyRef]] = {
    val jobGroupId = s"tiering-${tablePath.getDatabaseName}-${tablePath.getTableName}-$tableId"
    sc.setJobGroup(jobGroupId, s"Tiering table $tablePath")

    // Start background heartbeat thread
    val heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
      (r: Runnable) => {
        val t = new Thread(r, "SparkTiering-Heartbeat")
        t.setDaemon(true)
        t
      })
    val heartbeatFuture = startBackgroundHeartbeat(
      heartbeatScheduler,
      coordinator,
      consecutiveHeartbeatFailures,
      sc,
      jobGroupId,
      tablePath,
      tableId)

    try {
      submit(sc, splits, flussConfig, dataLakeFormat, lakeConfig, pollTimeoutMs)
        .collect()
        .toSeq
    } finally {
      // Stop background heartbeat
      heartbeatFuture.cancel(false)
      heartbeatScheduler.shutdownNow()
      sc.clearJobGroup()
    }
  }

  /**
   * Starts a background scheduled task that sends keepalive heartbeats and checks for table
   * drop/recreation during RDD job execution.
   */
  private def startBackgroundHeartbeat(
      scheduler: ScheduledExecutorService,
      coordinator: TieringCoordinator,
      consecutiveHeartbeatFailures: AtomicInteger,
      sc: SparkContext,
      jobGroupId: String,
      tablePath: TablePath,
      expectedTableId: Long): ScheduledFuture[_] = {
    coordinator.tableCancelled = false

    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            coordinator.sendKeepAliveHeartbeat()
            consecutiveHeartbeatFailures.set(0)
          } catch {
            case e: Exception =>
              val failures = consecutiveHeartbeatFailures.incrementAndGet()
              logWarning(s"Background heartbeat failed ($failures/$maxHeartbeatFailures)", e)
              if (failures >= maxHeartbeatFailures) {
                logError("Max heartbeat failures exceeded during RDD job. Cancelling job group.")
                coordinator.tableCancelled = true
                sc.cancelJobGroup(jobGroupId)
                return
              }
          }
        }
      },
      heartbeatIntervalMs,
      heartbeatIntervalMs,
      TimeUnit.MILLISECONDS
    )
  }
}

object SparkTieringJobRunner {
  def submit(
      sparkContext: SparkContext,
      splits: Seq[TieringSplit],
      flussConfig: Configuration,
      dataLakeFormat: String,
      lakeConfig: Configuration,
      pollTimeoutMs: Long): RDD[TableBucketWriteResult[AnyRef]] = {
    sparkContext.parallelize(splits, splits.size).map {
      split => TieringTask.process(split, flussConfig, dataLakeFormat, lakeConfig, pollTimeoutMs)
    }
  }
}
