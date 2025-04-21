/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.metrics;

/** Collection of metric names. */
public class MetricNames {

    // --------------------------------------------------------------------------------------------
    // metrics for requests
    // --------------------------------------------------------------------------------------------
    public static final String REQUEST_QUEUE_SIZE = "requestQueueSize";
    public static final String REQUESTS_RATE = "requestsPerSecond";
    public static final String ERRORS_RATE = "errorsPerSecond";
    public static final String REQUEST_BYTES = "requestBytes";
    public static final String REQUEST_QUEUE_TIME_MS = "requestQueueTimeMs";
    public static final String REQUEST_PROCESS_TIME_MS = "requestProcessTimeMs";
    public static final String RESPONSE_SEND_TIME_MS = "responseSendTimeMs";
    public static final String REQUEST_TOTAL_TIME_MS = "totalTimeMs";

    // --------------------------------------------------------------------------------------------
    // metrics for coordinator server
    // --------------------------------------------------------------------------------------------
    public static final String ACTIVE_COORDINATOR_COUNT = "activeCoordinatorCount";
    public static final String ACTIVE_TABLET_SERVER_COUNT = "activeTabletServerCount";
    public static final String OFFLINE_BUCKET_COUNT = "offlineBucketCount";
    public static final String TABLE_COUNT = "tableCount";
    public static final String BUCKET_COUNT = "bucketCount";

    // for coordinator event processor
    public static final String EVENT_QUEUE_SIZE = "eventQueueSize";
    public static final String EVENT_PROCESS_TIME_MS = "eventProcessTimeMs";

    // --------------------------------------------------------------------------------------------
    // metrics for tablet server
    // --------------------------------------------------------------------------------------------
    public static final String REPLICATION_IN_RATE = "replicationBytesInPerSecond";
    public static final String REPLICATION_OUT_RATE = "replicationBytesOutPerSecond";
    public static final String REPLICA_LEADER_COUNT = "leaderCount";
    public static final String REPLICA_COUNT = "replicaCount";
    public static final String WRITE_ID_COUNT = "writerIdCount";
    public static final String DELAYED_WRITE_COUNT = "delayedWriteCount";
    public static final String DELAYED_WRITE_EXPIRES_RATE = "delayedWriteExpiresPerSecond";
    public static final String DELAYED_FETCH_COUNT = "delayedFetchCount";
    public static final String DELAYED_FETCH_FROM_FOLLOWER_EXPIRES_RATE =
            "delayedFetchFromFollowerExpiresPerSecond";
    public static final String DELAYED_FETCH_FROM_CLIENT_EXPIRES_RATE =
            "delayedFetchFromClientExpiresPerSecond";

    // --------------------------------------------------------------------------------------------
    // metrics for table
    // --------------------------------------------------------------------------------------------
    public static final String MESSAGES_IN_RATE = "messagesInPerSecond";
    public static final String BYTES_IN_RATE = "bytesInPerSecond";
    public static final String BYTES_OUT_RATE = "bytesOutPerSecond";

    public static final String TOTAL_FETCH_LOG_REQUESTS_RATE = "totalFetchLogRequestsPerSecond";
    public static final String FAILED_FETCH_LOG_REQUESTS_RATE = "failedFetchLogRequestsPerSecond";
    public static final String TOTAL_PRODUCE_FETCH_LOG_REQUESTS_RATE =
            "totalProduceLogRequestsPerSecond";
    public static final String FAILED_PRODUCE_FETCH_LOG_REQUESTS_RATE =
            "failedProduceLogRequestsPerSecond";

    public static final String REMOTE_LOG_COPY_BYTES_RATE = "remoteLogCopyBytesPerSecond";
    public static final String REMOTE_LOG_COPY_REQUESTS_RATE = "remoteLogCopyRequestsPerSecond";
    public static final String REMOTE_LOG_COPY_ERROR_RATE = "remoteLogCopyErrorPerSecond";
    public static final String REMOTE_LOG_DELETE_REQUESTS_RATE = "remoteLogDeleteRequestsPerSecond";
    public static final String REMOTE_LOG_DELETE_ERROR_RATE = "remoteLogDeleteErrorPerSecond";

    public static final String TOTAL_LOOKUP_REQUESTS_RATE = "totalLookupRequestsPerSecond";
    public static final String FAILED_LOOKUP_REQUESTS_RATE = "failedLookupRequestsPerSecond";
    public static final String TOTAL_PUT_KV_REQUESTS_RATE = "totalPutKvRequestsPerSecond";
    public static final String FAILED_PUT_KV_REQUESTS_RATE = "failedPutKvRequestsPerSecond";
    public static final String TOTAL_LIMIT_SCAN_REQUESTS_RATE = "totalLimitScanRequestsPerSecond";
    public static final String FAILED_LIMIT_SCAN_REQUESTS_RATE = "failedLimitScanRequestsPerSecond";
    public static final String TOTAL_PREFIX_LOOKUP_REQUESTS_RATE =
            "totalPrefixLookupRequestsPerSecond";
    public static final String FAILED_PREFIX_LOOKUP_REQUESTS_RATE =
            "failedPrefixLookupRequestsPerSecond";

    // --------------------------------------------------------------------------------------------
    // metrics for table bucket
    // --------------------------------------------------------------------------------------------

    // for replica
    public static final String IN_SYNC_REPLICAS = "inSyncReplicasCount";
    public static final String UNDER_MIN_ISR = "underMinIsr";
    public static final String AT_MIN_ISR = "atMinIsr";
    public static final String ISR_EXPANDS_RATE = "isrExpandsPerSecond";
    public static final String ISR_SHRINKS_RATE = "isrShrinksPerSecond";
    public static final String FAILED_ISR_UPDATES_RATE = "failedIsrUpdatesPerSecond";

    // for log tablet
    public static final String LOG_NUM_SEGMENTS = "numSegments";
    public static final String LOG_END_OFFSET = "endOffset";
    public static final String LOG_SIZE = "size";
    public static final String LOG_FLUSH_RATE = "flushPerSecond";
    public static final String LOG_FLUSH_LATENCY_MS = "flushLatencyMs";

    // for kv tablet
    public static final String KV_LATEST_SNAPSHOT_SIZE = "latestSnapshotSize";
    public static final String KV_PRE_WRITE_BUFFER_TRUNCATE_AS_DUPLICATED_RATE =
            "preWriteBufferTruncateAsDuplicatedPerSecond";
    public static final String KV_PRE_WRITE_BUFFER_TRUNCATE_AS_ERROR_RATE =
            "preWriteBufferTruncateAsErrorPerSecond";
    public static final String KV_PRE_WRITE_BUFFER_FLUSH_RATE = "preWriteBufferFlushPerSecond";
    public static final String KV_PRE_WRITE_BUFFER_FLUSH_LATENCY_MS =
            "preWriteBufferFlushLatencyMs";

    // --------------------------------------------------------------------------------------------
    // metrics for rpc client
    // --------------------------------------------------------------------------------------------
    public static final String CLIENT_REQUESTS_RATE = "requestsPerSecond";
    public static final String CLIENT_RESPONSES_RATE = "responsesPerSecond";
    public static final String CLIENT_BYTES_IN_RATE = "bytesInPerSecond";
    public static final String CLIENT_BYTES_OUT_RATE = "bytesOutPerSecond";
    public static final String CLIENT_REQUEST_LATENCY_MS = "requestLatencyMs";
    public static final String CLIENT_REQUESTS_IN_FLIGHT = "requestsInFlight";

    // --------------------------------------------------------------------------------------------
    // metrics for client
    // --------------------------------------------------------------------------------------------

    // for writer
    public static final String WRITER_BUFFER_TOTAL_BYTES = "bufferTotalBytes";
    public static final String WRITER_BUFFER_AVAILABLE_BYTES = "bufferAvailableBytes";
    public static final String WRITER_BUFFER_WAITING_THREADS = "bufferWaitingThreads";
    public static final String WRITER_BATCH_QUEUE_TIME_MS = "batchQueueTimeMs";
    public static final String WRITER_RECORDS_RETRY_RATE = "recordsRetryPerSecond";
    public static final String WRITER_RECORDS_SEND_RATE = "recordSendPerSecond";
    public static final String WRITER_BYTES_SEND_RATE = "bytesSendPerSecond";
    public static final String WRITER_BYTES_PER_BATCH = "bytesPerBatch";
    public static final String WRITER_RECORDS_PER_BATCH = "recordsPerBatch";
    public static final String WRITER_SEND_LATENCY_MS = "sendLatencyMs";

    // for scanner
    public static final String SCANNER_TIME_MS_BETWEEN_POLL = "timeMsBetweenPoll";
    public static final String SCANNER_LAST_POLL_SECONDS_AGO = "lastPoolSecondsAgo";
    public static final String SCANNER_POLL_IDLE_RATIO = "pollIdleRatio";
    public static final String SCANNER_FETCH_LATENCY_MS = "fetchLatencyMs";
    public static final String SCANNER_FETCH_RATE = "fetchRequestsPerSecond";
    public static final String SCANNER_BYTES_PER_REQUEST = "bytesPerRequest";
    public static final String SCANNER_REMOTE_FETCH_BYTES_RATE = "remoteFetchBytesPerSecond";
    public static final String SCANNER_REMOTE_FETCH_RATE = "remoteFetchRequestsPerSecond";
    public static final String SCANNER_REMOTE_FETCH_ERROR_RATE = "remoteFetchErrorPerSecond";

    // for netty
    public static final String NETTY_USED_DIRECT_MEMORY = "usedDirectMemory";
    public static final String NETTY_NUM_DIRECT_ARENAS = "numDirectArenas";
    public static final String NETTY_NUM_ALLOCATIONS_PER_SECONDS = "numAllocationsPerSecond";
    public static final String NETTY_NUM_HUGE_ALLOCATIONS_PER_SECONDS =
            "numHugeAllocationsPerSecond";
}
