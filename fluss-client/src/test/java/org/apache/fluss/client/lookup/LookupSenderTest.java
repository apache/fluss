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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LookupSender}. */
public class LookupSenderTest {

    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID_PK, 0);

    private TestingMetadataUpdater metadataUpdater;
    private LookupSender lookupSender;

    @BeforeEach
    public void setup() {
        metadataUpdater = initializeMetadataUpdater();
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE, 5);
        conf.set(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE, 10);
        lookupSender = new LookupSender(metadataUpdater, new LookupQueue(conf), 5);
    }

    @Test
    void testSendLookupRequestWithNotLeaderOrFollowerException() {
        assertThat(metadataUpdater.getBucketLocation(tb1))
                .hasValue(
                        new BucketLocation(
                                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                                tb1,
                                1,
                                new int[] {1, 2, 3}));

        // send LookupRequest to serverId 1, which will respond with NotLeaderOrFollowerException
        // as responseLogicId=1 do.
        metadataUpdater.setResponseLogicId(1, 1);
        LookupQuery lookupQuery = new LookupQuery(tb1, new byte[0]);
        CompletableFuture<byte[]> result = lookupQuery.future();
        assertThat(result).isNotDone();
        lookupSender.sendLookups(1, LookupType.LOOKUP, Collections.singletonList(lookupQuery));

        assertThat(result.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(result::get)
                .rootCause()
                .isInstanceOf(NotLeaderOrFollowerException.class)
                .hasMessage("mock not leader or follower exception.");
        // When NotLeaderOrFollowerException is received, the bucketLocation will be removed from
        // metadata updater to trigger get the latest bucketLocation in next lookup round.
        assertThat(metadataUpdater.getBucketLocation(tb1)).isNotPresent();
    }

    @Test
    void testSendPrefixLookupRequestWithNotLeaderOrFollowerException() {
        assertThat(metadataUpdater.getBucketLocation(tb1))
                .hasValue(
                        new BucketLocation(
                                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                                tb1,
                                1,
                                new int[] {1, 2, 3}));

        // send PrefixLookupRequest to serverId 1, which will respond with
        // NotLeaderOrFollowerException as responseLogicId=1 do.
        metadataUpdater.setResponseLogicId(1, 1);
        PrefixLookupQuery prefixLookupQuery = new PrefixLookupQuery(tb1, new byte[0]);
        CompletableFuture<List<byte[]>> future = prefixLookupQuery.future();
        assertThat(future).isNotDone();
        lookupSender.sendLookups(
                1, LookupType.PREFIX_LOOKUP, Collections.singletonList(prefixLookupQuery));

        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(future::get)
                .rootCause()
                .isInstanceOf(NotLeaderOrFollowerException.class)
                .hasMessage("mock not leader or follower exception.");
        // When NotLeaderOrFollowerException is received, the bucketLocation will be removed from
        // metadata updater to trigger get the latest bucketLocation in next lookup round.
        assertThat(metadataUpdater.getBucketLocation(tb1)).isNotPresent();
    }

    private TestingMetadataUpdater initializeMetadataUpdater() {
        return new TestingMetadataUpdater(
                Collections.singletonMap(DATA1_TABLE_PATH_PK, DATA1_TABLE_INFO_PK));
    }
}
