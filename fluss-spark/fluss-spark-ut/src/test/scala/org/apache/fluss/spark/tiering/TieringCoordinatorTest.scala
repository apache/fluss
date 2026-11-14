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

import org.apache.fluss.config.Configuration
import org.apache.fluss.exception.FencedTieringEpochException
import org.apache.fluss.lake.committer.TieringStats
import org.apache.fluss.rpc.gateway.CoordinatorGateway
import org.apache.fluss.rpc.messages.{LakeTieringHeartbeatRequest, LakeTieringHeartbeatResponse}
import org.apache.fluss.rpc.protocol.ApiError

import org.assertj.core.api.Assertions.assertThat
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.CompletableFuture

/** Tests fencing and final status reporting in the tiering heartbeat protocol. */
class TieringCoordinatorTest extends AnyFunSuite {

  test("a per-table heartbeat error cancels the assignment") {
    val gateway = mock(classOf[CoordinatorGateway])
    val registered = new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1)
    val fenced = new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1)
    fenced
      .addTieringTableResp()
      .setTableId(10L)
      .setError(
        ApiError
          .fromThrowable(new FencedTieringEpochException("expired assignment"))
          .toErrorResponse)
    when(gateway.lakeTieringHeartbeat(any(classOf[LakeTieringHeartbeatRequest])))
      .thenReturn(CompletableFuture.completedFuture(registered))
      .thenReturn(CompletableFuture.completedFuture(fenced))
    val coordinator = new TieringCoordinator(new Configuration(), gateway)
    try {
      coordinator.open()
      coordinator.registerTiering(10L, 2L)
      coordinator.sendKeepAliveHeartbeat()
      assertThat(coordinator.tableCancelled).isTrue()
    } finally {
      coordinator.close()
    }
  }

  test("coordinator failover fences old work without reporting it in the new epoch") {
    val gateway = mock(classOf[CoordinatorGateway])
    when(gateway.lakeTieringHeartbeat(any(classOf[LakeTieringHeartbeatRequest])))
      .thenReturn(CompletableFuture.completedFuture(
        new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1)))
      .thenReturn(CompletableFuture.completedFuture(
        new LakeTieringHeartbeatResponse().setCoordinatorEpoch(2)))
    val coordinator = new TieringCoordinator(new Configuration(), gateway)
    try {
      coordinator.open()
      coordinator.registerTiering(10L, 3L)
      coordinator.sendKeepAliveHeartbeat()
      assertThat(coordinator.tableCancelled).isTrue()
      coordinator.markTableFailed(10L, 3L)
      coordinator.heartbeatAndRequestTable()
      val requests = ArgumentCaptor.forClass(classOf[LakeTieringHeartbeatRequest])
      verify(gateway, times(3)).lakeTieringHeartbeat(requests.capture())
      val request = requests.getValue
      assertThat(request.getTieringTablesCount).isZero()
      assertThat(request.getFailedTablesCount).isZero()
      assertThat(request.getFinishedTablesCount).isZero()
    } finally {
      coordinator.close()
    }
  }

  test("close reports both completed and in-progress tables exactly once") {
    val gateway = mock(classOf[CoordinatorGateway])
    when(gateway.lakeTieringHeartbeat(any(classOf[LakeTieringHeartbeatRequest])))
      .thenAnswer(
        _ =>
          CompletableFuture.completedFuture(
            new LakeTieringHeartbeatResponse().setCoordinatorEpoch(4)))
    val coordinator = new TieringCoordinator(new Configuration(), gateway)
    coordinator.open()
    coordinator.registerTiering(10L, 1L)
    coordinator.markTableFinished(10L, 1L, new TieringStats(100L, 5L))
    coordinator.registerTiering(20L, 2L)
    coordinator.close()
    coordinator.close()
    val requests = ArgumentCaptor.forClass(classOf[LakeTieringHeartbeatRequest])
    verify(gateway, times(2)).lakeTieringHeartbeat(requests.capture())
    val request = requests.getValue
    assertThat(request.hasRequestTable).isFalse()
    assertThat(request.getFinishedTableAt(0).getTableId).isEqualTo(10L)
    assertThat(request.getFinishedTableAt(0).getCoordinatorEpoch).isEqualTo(4)
    assertThat(request.getFinishedTableAt(0).getLakeTieringStats.getRecordCount).isEqualTo(5L)
    assertThat(request.getFailedTableAt(0).getTableId).isEqualTo(20L)
    assertThat(request.getFailedTableAt(0).getTieringEpoch).isEqualTo(2L)
  }
}
