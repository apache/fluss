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
import org.apache.fluss.client.tiering.{FlussTableLakeSnapshotCommitter, TableBucketWriteResult, TieringCommitter, TieringSplit, TieringSplitGenerator}
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.lake.committer.TieringStats
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp
import org.apache.fluss.lake.writer.{LakeTieringFactory, TieringTableValidator}
import org.apache.fluss.metadata.{PartitionInfo, TablePath}
import org.apache.fluss.utils.IOUtils.closeQuietly
import org.apache.fluss.utils.Preconditions.checkState

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.File
import java.util.{Collections, UUID}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration => ScalaDuration}
import scala.util.control.NonFatal

/**
 * Runs one table tiering round at a time, keeping its assignment alive through split generation,
 * executor writes and the lake/Fluss commit. The service stops on repeated heartbeat failures.
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
    splitGeneratorFactory: Admin => TieringSplitGenerator = new TieringSplitGenerator(_),
    lakeTieringFactoryProvider: (String, Configuration) => LakeTieringFactory[AnyRef, AnyRef] =
      SparkTieringJobRunner.createLakeTieringFactory)
  extends Logging {

  private val pollIntervalMs =
    flussConfig.get(SparkTieringOptions.POLL_TIERING_TABLE_INTERVAL).toMillis
  private val heartbeatIntervalMs =
    flussConfig.get(SparkTieringOptions.HEARTBEAT_INTERVAL).toMillis
  private val pollTimeoutMs = flussConfig.get(SparkTieringOptions.POLL_TIMEOUT).toMillis
  private val maxHeartbeatFailures =
    flussConfig.get(SparkTieringOptions.MAX_HEARTBEAT_FAILURES).intValue()
  require(pollIntervalMs > 0, "Tiering table poll interval must be positive")
  require(heartbeatIntervalMs > 0, "Tiering heartbeat interval must be positive")
  require(pollTimeoutMs > 0, "Tiering log poll timeout must be positive")
  require(maxHeartbeatFailures > 0, "Maximum heartbeat failures must be positive")

  private val lakeTieringFactory = lakeTieringFactoryProvider(dataLakeFormat, lakeConfig)

  private val ioTmpDirs = {
    val configured = lakeTieringConfig.getOptional(ConfigOptions.LAKE_TIERING_IO_TMP_DIRS)
    if (configured.isPresent) {
      configured
        .get()
        .split(",|" + Pattern.quote(File.pathSeparator))
        .map(_.trim)
        .filter(_.nonEmpty)
    } else {
      null
    }
  }

  private val started = new AtomicBoolean(false)
  private val stopped = new AtomicBoolean(false)
  private val stopSignal = new Object
  private val terminationPromise = Promise[Unit]()
  @volatile private var activeJobGroup: String = _
  @volatile private var heartbeatFailure: Throwable = _

  private val loopExecutionContext = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(
      (r: Runnable) => {
        val thread = new Thread(r, "SparkTiering-MainLoop")
        thread.setDaemon(true)
        thread
      }))

  private val heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
    (r: Runnable) => {
      val thread = new Thread(r, "SparkTiering-Heartbeat")
      thread.setDaemon(true)
      thread
    })

  /** Starts the service once and returns its termination future on subsequent calls. */
  def startAsync(): Future[Unit] = synchronized {
    if (started.compareAndSet(false, true)) {
      terminationPromise.completeWith(Future {
        try run()
        finally loopExecutionContext.shutdown()
      }(loopExecutionContext))
    }
    terminationPromise.future
  }

  /** Cancels the active Spark job and waits for the service to release its resources. */
  def stop(timeout: ScalaDuration = ScalaDuration(60, TimeUnit.SECONDS)): Unit = {
    synchronized {
      stopped.set(true)
      if (started.compareAndSet(false, true)) {
        heartbeatScheduler.shutdownNow()
        loopExecutionContext.shutdown()
        terminationPromise.trySuccess(())
      }
    }
    stopSignal.synchronized {
      stopSignal.notifyAll()
    }
    val jobGroup = activeJobGroup
    if (jobGroup != null) {
      spark.sparkContext.cancelJobGroup(jobGroup)
    }
    Await.ready(terminationPromise.future, timeout)
  }

  private def pause(): Unit = stopSignal.synchronized {
    if (!stopped.get()) {
      stopSignal.wait(pollIntervalMs)
    }
  }

  private def run(): Unit = {
    var coordinator: TieringCoordinator = null
    var snapshotCommitter: FlussTableLakeSnapshotCommitter = null
    var connection: Connection = null
    var admin: Admin = null
    try {
      coordinator = coordinatorFactory(flussConfig)
      snapshotCommitter = snapshotCommitterFactory(flussConfig)
      coordinator.open()
      snapshotCommitter.open()
      connection = connectionFactory(flussConfig)
      admin = connection.getAdmin
      val committer = new TieringCommitter[AnyRef, AnyRef](
        admin,
        flussConfig,
        lakeTieringConfig,
        lakeTieringFactory,
        snapshotCommitter)
      val splitGenerator = splitGeneratorFactory(admin)
      val failures = new AtomicInteger(0)

      while (!stopped.get()) {
        try {
          val assignment = coordinator.heartbeatAndRequestTable()
          failures.set(0)
          assignment match {
            case Some((tableId, tieringEpoch, tablePath)) =>
              processTieringTable(
                coordinator,
                committer,
                admin,
                splitGenerator,
                failures,
                tableId,
                tieringEpoch,
                tablePath)
            case None => pause()
          }
        } catch {
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            stopped.set(true)
            throw new IllegalStateException("Spark tiering was interrupted", e)
          case NonFatal(e) =>
            val count = failures.incrementAndGet()
            if (count >= maxHeartbeatFailures) {
              throw new IllegalStateException(
                "Spark tiering exceeded maximum heartbeat failures",
                e)
            }
            logWarning(s"Tiering heartbeat failed ($count/$maxHeartbeatFailures)", e)
            pause()
        }
        if (heartbeatFailure != null) {
          throw new IllegalStateException(
            "Spark tiering exceeded maximum background heartbeat failures",
            heartbeatFailure)
        }
      }
    } finally {
      try {
        heartbeatScheduler.shutdownNow()
        if (!heartbeatScheduler.awaitTermination(60, TimeUnit.SECONDS)) {
          logWarning("Spark tiering heartbeat executor did not terminate within 60 seconds")
        }
      } finally {
        closeQuietly(coordinator, "TieringCoordinator")
        closeQuietly(snapshotCommitter, "FlussTableLakeSnapshotCommitter")
        closeQuietly(admin, "Admin")
        closeQuietly(connection, "Connection")
      }
    }
  }

  private def processTieringTable(
      coordinator: TieringCoordinator,
      committer: TieringCommitter[AnyRef, AnyRef],
      admin: Admin,
      splitGenerator: TieringSplitGenerator,
      failures: AtomicInteger,
      tableId: Long,
      tieringEpoch: Long,
      tablePath: TablePath): Unit = {
    val sc = spark.sparkContext
    val jobGroupId = s"fluss-tiering-$tableId-${UUID.randomUUID()}"
    coordinator.registerTiering(tableId, tieringEpoch)
    activeJobGroup = jobGroupId
    sc.setJobGroup(jobGroupId, s"Tiering table $tablePath", interruptOnCancel = true)
    val heartbeatLock = new Object
    val heartbeatStopped = new AtomicBoolean(false)
    val heartbeat = heartbeatScheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = heartbeatLock.synchronized {
          if (!heartbeatStopped.get() && !coordinator.tableCancelled && !stopped.get()) {
            try {
              coordinator.sendKeepAliveHeartbeat()
              failures.set(0)
            } catch {
              case NonFatal(e) =>
                val count = failures.incrementAndGet()
                logWarning(s"Background heartbeat failed ($count/$maxHeartbeatFailures)", e)
                if (count >= maxHeartbeatFailures) {
                  heartbeatFailure = e
                  coordinator.tableCancelled = true
                }
            }
            if (coordinator.tableCancelled || heartbeatFailure != null) {
              sc.cancelJobGroup(jobGroupId)
            }
          }
        }
      },
      heartbeatIntervalMs,
      heartbeatIntervalMs,
      TimeUnit.MILLISECONDS
    )

    var finished = false
    var stats = TieringStats.UNKNOWN
    try {
      val tableInfo = admin.getTableInfo(tablePath).get()
      require(tableInfo.getTableId == tableId, s"Table $tablePath was dropped and recreated")
      lakeTieringFactory match {
        case validator: TieringTableValidator => validator.validateTable(tableInfo)
        case _ =>
      }
      val partitionInfos = if (tableInfo.isPartitioned) {
        admin.listPartitionInfos(tablePath, true).get()
      } else {
        Collections.emptyList[PartitionInfo]()
      }
      val rawSplits = splitGenerator.generateTableSplits(tableInfo, partitionInfos).asScala
      val roundTimestamp = System.currentTimeMillis()
      val splits = rawSplits.zipWithIndex.map {
        case (split, index) => split.copy(rawSplits.size, index, roundTimestamp)
      }.toSeq
      checkNotCancelled(coordinator)
      val results = if (splits.isEmpty) {
        Seq.empty[TableBucketWriteResult[AnyRef]]
      } else {
        SparkTieringJobRunner
          .submit(
            sc,
            withPartitionBucketCounts(splits, partitionInfos),
            flussConfig,
            dataLakeFormat,
            lakeConfig,
            pollTimeoutMs,
            ioTmpDirs)
          .collect()
          .toSeq
          .map(_.toWriteResult(lakeTieringFactory.getWriteResultSerializer))
      }
      // Confirm ownership immediately before committing, including for empty rounds.
      coordinator.sendKeepAliveHeartbeat()
      checkNotCancelled(coordinator)
      val result = committer.commitWriteResults(tableId, tablePath, results.asJava)
      stats = Option(result.getStats).getOrElse(TieringStats.UNKNOWN)
      finished = true
    } catch {
      case NonFatal(e) => logWarning(s"Tiering failed for $tablePath (epoch=$tieringEpoch)", e)
    } finally {
      // Wait for any in-flight heartbeat and prevent it from racing the final status check.
      heartbeatLock.synchronized {
        try {
          try {
            checkNotCancelled(coordinator)
          } catch {
            case NonFatal(e) =>
              finished = false
              logWarning(s"Tiering cancelled for $tablePath (epoch=$tieringEpoch)", e)
          }
          if (finished) {
            coordinator.markTableFinished(tableId, tieringEpoch, stats)
          } else {
            coordinator.markTableFailed(tableId, tieringEpoch)
          }
        } finally {
          // A queued heartbeat must observe this flag before touching shared round state.
          heartbeatStopped.set(true)
          heartbeat.cancel(false)
          activeJobGroup = null
          sc.clearJobGroup()
        }
      }
    }
  }

  private def withPartitionBucketCounts(
      splits: Seq[TieringSplit],
      partitionInfos: java.util.List[PartitionInfo]): Seq[(TieringSplit, java.lang.Integer)] = {
    val partitionBucketCounts =
      partitionInfos.asScala.map(info => info.getPartitionId -> info.getBucketCount).toMap
    splits.map {
      split =>
        val partitionId = split.getTableBucket.getPartitionId
        val bucketCount: java.lang.Integer = if (partitionId == null) {
          null
        } else {
          Int.box(
            partitionBucketCounts.getOrElse(
              partitionId.longValue(),
              throw new IllegalStateException(
                s"Partition for ${split.getTableBucket} no longer exists")))
        }
        (split, bucketCount)
    }
  }

  private def checkNotCancelled(coordinator: TieringCoordinator): Unit = {
    checkState(!stopped.get(), "Spark tiering service is stopping": Any)
    checkState(heartbeatFailure == null, "Spark tiering heartbeat failed": Any)
    checkState(!coordinator.tableCancelled, "Spark tiering assignment was cancelled": Any)
  }
}

/** Creates executor jobs without capturing driver-side resources in the Spark closure. */
object SparkTieringJobRunner {
  private[tiering] def createLakeTieringFactory(
      dataLakeFormat: String,
      lakeConfig: Configuration): LakeTieringFactory[AnyRef, AnyRef] = {
    LakeStoragePluginSetUp
      .fromDataLakeFormat(dataLakeFormat, null)
      .createLakeStorage(lakeConfig)
      .createLakeTieringFactory()
      .asInstanceOf[LakeTieringFactory[AnyRef, AnyRef]]
  }

  def submit(
      sparkContext: SparkContext,
      splits: Seq[(TieringSplit, java.lang.Integer)],
      flussConfig: Configuration,
      dataLakeFormat: String,
      lakeConfig: Configuration,
      pollTimeoutMs: Long,
      ioTmpDirs: Array[String] = null): RDD[SerializedTaskResult] = {
    sparkContext.parallelize(splits, splits.size).map {
      case (split, partitionBucketCount) =>
        TieringTask.process(
          split,
          flussConfig,
          dataLakeFormat,
          lakeConfig,
          pollTimeoutMs,
          partitionBucketCount,
          ioTmpDirs)
    }
  }
}
