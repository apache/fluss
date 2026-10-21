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

import org.apache.fluss.client.admin.OffsetSpec
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.metadata.TableBucket
import org.apache.fluss.spark.read.{AppendPlanner, FlussAppendInputPartition, FlussAppendPartitionReader}
import org.apache.fluss.testutils.common.CommonTestUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.assertj.core.api.Assertions.assertThat

import java.time.Duration
import java.util.{Collections, Optional}

/** Spark SQL regressions for log buckets emptied by retention. */
class SparkLogTableRetentionReadTest extends FlussSparkTestBase {

  for (partitioned <- Seq(false, true)) {
    test(s"Spark Read: expired log table, partitioned=$partitioned") {
      withTable("retained_log") {
        val partitionClause = if (partitioned) "PARTITIONED BY (pt)" else ""
        sql(s"""
               |CREATE TABLE $DEFAULT_DATABASE.retained_log (id INT, pt STRING)
               |$partitionClause
               |TBLPROPERTIES (
               |  'bucket.num' = '1',
               |  'table.replication.factor' = '1',
               |  'table.log.ttl' = '1 ms')
               |""".stripMargin)
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (1, 'expired')")

        val path = createTablePath("retained_log")
        val tableId = admin.getTableInfo(path).get().getTableId
        val partition = if (partitioned) {
          Some(admin.listPartitionInfos(path).get().get(0))
        } else {
          None
        }
        val bucket = new TableBucket(
          tableId,
          partition.map(p => java.lang.Long.valueOf(p.getPartitionId)).orNull,
          0)
        val leader = flussServer.getZooKeeperClient.getLeaderAndIsr(bucket).get().leader()
        val logTablet = flussServer
          .getTabletServerById(leader)
          .getReplicaManager
          .getReplicaOrException(bucket)
          .getLogTablet
        val endOffset = logTablet.getHighWatermark
        assertThat(endOffset).isPositive
        logTablet.roll(Optional.empty[java.lang.Long]())
        CommonTestUtils.retry(
          Duration.ofSeconds(10),
          () => {
            logTablet.deleteExpiredSegments()
            assertThat(logTablet.logStartOffset()).isEqualTo(endOffset)
          })

        for (spec <- Seq[OffsetSpec](new OffsetSpec.EarliestSpec(), new OffsetSpec.LatestSpec())) {
          val offsets = partition match {
            case Some(info) =>
              admin.listOffsets(
                path,
                info.getPartitionName,
                Collections.singletonList[Integer](0),
                spec)
            case None => admin.listOffsets(path, Collections.singletonList[Integer](0), spec)
          }
          assertThat(offsets.bucketResult(0).get()).isEqualTo(endOffset)
        }

        checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.retained_log"), Nil)
        checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.retained_log LIMIT 100"), Nil)

        // A new partition also exercises a retained-empty partition alongside a readable one.
        sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (2, 'current')")
        checkAnswer(sql(s"SELECT * FROM $DEFAULT_DATABASE.retained_log"), Row(2, "current") :: Nil)
        checkAnswer(
          sql(s"SELECT * FROM $DEFAULT_DATABASE.retained_log LIMIT 100"),
          Row(2, "current") :: Nil)
      }
    }
  }

  test("Spark Read: retention advances between planning and task execution") {
    withTable("retained_log") {
      sql(s"""
             |CREATE TABLE $DEFAULT_DATABASE.retained_log (id INT, pt STRING)
             |TBLPROPERTIES (
             |  'bucket.num' = '1',
             |  'table.replication.factor' = '1',
             |  'table.log.ttl' = '1 ms')
             |""".stripMargin)
      sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (1, 'expired')")

      val path = createTablePath("retained_log")
      val info = admin.getTableInfo(path).get()
      val bucket = new TableBucket(info.getTableId, 0)
      val leader = flussServer.getZooKeeperClient.getLeaderAndIsr(bucket).get().leader()
      val logTablet = flussServer
        .getTabletServerById(leader)
        .getReplicaManager
        .getReplicaOrException(bucket)
        .getLogTablet
      val retainedStart = logTablet.getHighWatermark
      logTablet.roll(Optional.empty[java.lang.Long]())
      sql(s"INSERT INTO $DEFAULT_DATABASE.retained_log VALUES (2, 'current')")
      val config = flussServer.getClientConfig
      val planner = new AppendPlanner(
        path,
        info,
        None,
        None,
        Array(0, 1),
        new CaseInsensitiveStringMap(Collections.emptyMap[String, String]()),
        config)
      val partitions = planner.plan()
      assertThat(partitions.length).isEqualTo(1)

      // Advance earliest after the plan is fixed, while leaving a record before its stop offset.
      CommonTestUtils.retry(
        Duration.ofSeconds(10),
        () => {
          logTablet.deleteExpiredSegments()
          assertThat(logTablet.logStartOffset()).isEqualTo(retainedStart)
        })
      val reader = new FlussAppendPartitionReader(
        path,
        Array(0, 1),
        None,
        None,
        partitions.head.asInstanceOf[FlussAppendInputPartition],
        config)
      try {
        assertThat(reader.next()).isTrue
        assertThat(reader.get().getInt(0)).isEqualTo(2)
        assertThat(reader.get().getUTF8String(1).toString).isEqualTo("current")
        assertThat(reader.next()).isFalse
      } finally {
        reader.close()
      }
    }
  }

  override protected def flussConf: Configuration = {
    val conf = super.flussConf
    conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO)
    conf.set(ConfigOptions.LOG_RETENTION_CHECK_INTERVAL, Duration.ofDays(1))
    conf
  }
}
