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
import org.apache.fluss.client.metadata.MetadataUpdater
import org.apache.fluss.config.Configuration
import org.apache.fluss.lake.committer.TieringStats
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.metrics.registry.MetricRegistry
import org.apache.fluss.rpc.{GatewayClientProxy, RpcClient}
import org.apache.fluss.rpc.gateway.CoordinatorGateway
import org.apache.fluss.rpc.messages._
import org.apache.fluss.rpc.metrics.ClientMetricGroup

import org.apache.spark.internal.Logging

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

// TODO: This logic is duplicated from
//  org.apache.fluss.flink.tiering.source.TieringSourceFunction (heartbeat protocol).
//  Consider extracting to a shared module (e.g., fluss-tiering-common) in the future.

/** Manages heartbeat communication with the Fluss coordinator for the Spark tiering service. */
class TieringCoordinator(flussConfig: Configuration) extends AutoCloseable with Logging {

  import TieringCoordinator._

  private var connection: Connection = _
  private var rpcClient: RpcClient = _
  private var coordinatorGateway: CoordinatorGateway = _
  private var coordinatorEpoch: Int = 0

  // State maps (immutable maps, updated via var)
  private var tieringTableEpochs: Map[Long, Long] = Map.empty
  private var finishedTables: Map[Long, TieringFinishInfo] = Map.empty
  private var failedTableEpochs: Map[Long, Long] = Map.empty

  @volatile var tableCancelled: Boolean = false

  def open(): Unit = {
    connection = ConnectionFactory.createConnection(flussConfig)
    val metricRegistry = MetricRegistry.create(flussConfig, null)
    val clientMetricGroup = new ClientMetricGroup(metricRegistry, "SparkLakeTieringService")
    rpcClient = RpcClient.create(flussConfig, clientMetricGroup)
    val metadataUpdater = new MetadataUpdater(flussConfig, rpcClient)
    coordinatorGateway = GatewayClientProxy.createGatewayProxy(
      () => metadataUpdater.getCoordinatorServer,
      rpcClient,
      classOf[CoordinatorGateway])

    logInfo("Registering Spark Tiering Service with Fluss Coordinator...")
    val response = waitHeartbeatResponse(coordinatorGateway.lakeTieringHeartbeat(basicHeartBeat()))
    coordinatorEpoch = response.getCoordinatorEpoch
    logInfo(s"Registered with Fluss Coordinator (epoch=$coordinatorEpoch).")
  }

  /**
   * Sends a heartbeat reporting finished/failed tables and requests a new table assignment.
   *
   * @return
   *   Some((tableId, tieringEpoch, tablePath)) if a table is assigned, None otherwise
   */
  def heartbeatAndRequestTable(): Option[(Long, Long, TablePath)] = {
    val currentFinished = finishedTables
    val currentFailed = failedTableEpochs

    val request =
      buildTieringHeartBeat(tieringTableEpochs, currentFinished, currentFailed, coordinatorEpoch)
    request.setRequestTable(true)

    logInfo(
      s"Heartbeat: tiering=$tieringTableEpochs, finished=$currentFinished, " +
        s"failed=$currentFailed")

    val response = waitHeartbeatResponse(coordinatorGateway.lakeTieringHeartbeat(request))

    // Clear reported finished/failed tables
    finishedTables = finishedTables -- currentFinished.keys
    failedTableEpochs = failedTableEpochs -- currentFailed.keys

    if (response.hasTieringTable) {
      val tieringTable = response.getTieringTable
      val tablePath = TablePath.of(
        tieringTable.getTablePath.getDatabaseName,
        tieringTable.getTablePath.getTableName)
      val tableId = tieringTable.getTableId
      val tieringEpoch = tieringTable.getTieringEpoch
      logInfo(s"Assigned tiering table: tableId=$tableId, epoch=$tieringEpoch, path=$tablePath")
      Some((tableId, tieringEpoch, tablePath))
    } else {
      logDebug("No tiering table available.")
      None
    }
  }

  /** Sends a keepalive heartbeat with currently tiering tables (no table request). */
  def sendKeepAliveHeartbeat(): Unit = {
    val request =
      buildTieringHeartBeat(tieringTableEpochs, finishedTables, failedTableEpochs, coordinatorEpoch)
    waitHeartbeatResponse(coordinatorGateway.lakeTieringHeartbeat(request))
  }

  def registerTiering(tableId: Long, tieringEpoch: Long): Unit = {
    tieringTableEpochs = tieringTableEpochs + (tableId -> tieringEpoch)
  }

  def markTableFinished(tableId: Long, tieringEpoch: Long, stats: TieringStats): Unit = {
    tieringTableEpochs = tieringTableEpochs - tableId
    finishedTables = finishedTables + (tableId -> TieringFinishInfo(tieringEpoch, stats))
  }

  def markTableFailed(tableId: Long, tieringEpoch: Long): Unit = {
    tieringTableEpochs = tieringTableEpochs - tableId
    failedTableEpochs = failedTableEpochs + (tableId -> tieringEpoch)
  }

  override def close(): Unit = {
    // Report all tiering tables as failed before shutdown
    if (tieringTableEpochs.nonEmpty) {
      failedTableEpochs = failedTableEpochs ++ tieringTableEpochs
      tieringTableEpochs = Map.empty
      try {
        val request = basicHeartBeat()
        addFailedTables(request, failedTableEpochs, coordinatorEpoch)
        waitHeartbeatResponse(coordinatorGateway.lakeTieringHeartbeat(request))
        logInfo(s"Reported failed tables on shutdown: $failedTableEpochs")
      } catch {
        case e: Exception =>
          logWarning("Failed to report failed tables on shutdown", e)
      }
    }

    closeQuietly(rpcClient, "RpcClient")
    closeQuietly(connection, "Connection")
  }
}

/** Heartbeat helper functions and constants for [[TieringCoordinator]]. */
object TieringCoordinator extends Logging {

  private val HEARTBEAT_TIMEOUT_MINUTES = 3L

  private[tiering] def basicHeartBeat(): LakeTieringHeartbeatRequest = {
    new LakeTieringHeartbeatRequest()
  }

  private[tiering] def buildTieringHeartBeat(
      tiering: Map[Long, Long],
      finished: Map[Long, TieringFinishInfo],
      failed: Map[Long, Long],
      epoch: Int): LakeTieringHeartbeatRequest = {
    val request = basicHeartBeat()

    if (tiering.nonEmpty) {
      request.addAllTieringTables(toPbHeartbeatReqForTable(tiering, epoch).asJava)
    }

    if (finished.nonEmpty) {
      val finishedReqs = finished.map {
        case (tableId, finishInfo) =>
          val req = new PbHeartbeatReqForTable()
            .setTableId(tableId)
            .setCoordinatorEpoch(epoch)
            .setTieringEpoch(finishInfo.tieringEpoch)
          val stats = finishInfo.stats
          if (stats != null && stats.isAvailableStats) {
            val pbStats = new PbLakeTieringStats()
            if (stats.getFileSize != null) pbStats.setFileSize(stats.getFileSize)
            if (stats.getRecordCount != null) pbStats.setRecordCount(stats.getRecordCount)
            req.setLakeTieringStats(pbStats)
          }
          req
      }.toSeq
      request.addAllFinishedTables(finishedReqs.asJava)
    }

    addFailedTables(request, failed, epoch)
    request
  }

  private[tiering] def addFailedTables(
      request: LakeTieringHeartbeatRequest,
      failed: Map[Long, Long],
      epoch: Int): Unit = {
    if (failed.nonEmpty) {
      request.addAllFailedTables(toPbHeartbeatReqForTable(failed, epoch).asJava)
    }
  }

  private def toPbHeartbeatReqForTable(
      tableEpochs: Map[Long, Long],
      coordinatorEpoch: Int): Set[PbHeartbeatReqForTable] = {
    tableEpochs.map {
      case (tableId, tieringEpoch) =>
        new PbHeartbeatReqForTable()
          .setTableId(tableId)
          .setCoordinatorEpoch(coordinatorEpoch)
          .setTieringEpoch(tieringEpoch)
    }.toSet
  }

  private[tiering] def waitHeartbeatResponse(
      future: java.util.concurrent.CompletableFuture[LakeTieringHeartbeatResponse])
      : LakeTieringHeartbeatResponse = {
    try {
      future.get(HEARTBEAT_TIMEOUT_MINUTES, TimeUnit.MINUTES)
    } catch {
      case e: Exception =>
        logError("Failed to wait heartbeat response", e)
        throw new IllegalStateException("Failed to wait heartbeat response", e)
    }
  }
}

/** Finish info for a tiering table. */
private[tiering] case class TieringFinishInfo(tieringEpoch: Long, stats: TieringStats)
