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

package org.apache.fluss.server.index;

import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.rpc.messages.PbPutIndexReqForBucket;
import org.apache.fluss.rpc.messages.PutIndexRequest;
import org.apache.fluss.utils.ProtoCodecUtils;

import java.util.List;

import static org.apache.fluss.rpc.protocol.MessageCodec.REQUEST_HEADER_LENGTH;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Builds index replication requests and computes their exact framed sizes from the same fields. */
final class IndexRequestFactory {

    /** Timeout used by replication requests and their prepublication size check. */
    static final long DEFAULT_REQUEST_TIMEOUT_MS = 30_000L;

    private static final int DEFAULT_ACKS = -1;

    private static final long MAX_CODEC_FRAMED_BYTES = Integer.MAX_VALUE;
    private static final int LENGTH_PREFIX_BYTES = Integer.BYTES;
    private static final int PUT_INDEX_BUCKETS_REQ_FIELD_NUMBER = 5;
    private static final int BUCKETS_REQ_TAG_SIZE =
            ProtoCodecUtils.computeVarIntSize(
                    (PUT_INDEX_BUCKETS_REQ_FIELD_NUMBER << ProtoCodecUtils.TAG_TYPE_BITS)
                            | ProtoCodecUtils.WIRETYPE_LENGTH_DELIMITED);

    private IndexRequestFactory() {}

    static PutIndexRequest build(long tableId, List<IndexBatch> batches, long requestTimeoutMs) {
        long sourceTableId = batches.get(0).sourceBucket().getTableId();
        PutIndexRequest req = newPutIndexRequest(tableId, sourceTableId, (int) requestTimeoutMs);
        for (IndexBatch batch : batches) {
            checkArgument(
                    batch.sourceBucket().getTableId() == sourceTableId,
                    "A PutIndex request cannot mix source tables");
            fillBucketRequest(req.addBucketsReq(), batch);
        }
        return req;
    }

    private static void fillBucketRequest(PbPutIndexReqForBucket request, IndexBatch batch) {
        request.setBucketId(batch.targetBucket().getBucket())
                .setSourceBucketId(batch.sourceBucket().getBucket())
                .setSourceEndOffset(batch.sourceEndOffset())
                .setProgressKey(batch.progressKey())
                .setRecordsBytesView(batch.encoded());
        if (batch.sourceBucket().getPartitionId() != null) {
            request.setSourcePartitionId(batch.sourceBucket().getPartitionId());
        }
    }

    private static PutIndexRequest newPutIndexRequest(
            long tableId, long sourceTableId, int timeoutMs) {
        return new PutIndexRequest()
                .setTableId(tableId)
                .setSourceTableId(sourceTableId)
                .setAcks(DEFAULT_ACKS)
                .setTimeoutMs(timeoutMs);
    }

    /** Incremental codec-derived size of one index PutIndex request. */
    static final class SizeAccumulator {
        private final PbPutIndexReqForBucket bucketSizer;
        private long bodyBytes;
        private boolean codecRepresentable;

        SizeAccumulator(long tableId, long sourceTableId, long requestTimeoutMs) {
            this.bucketSizer = new PbPutIndexReqForBucket();
            this.bodyBytes =
                    newPutIndexRequest(tableId, sourceTableId, (int) requestTimeoutMs).totalSize();
            this.codecRepresentable = bodyBytes >= 0;
        }

        private SizeAccumulator(SizeAccumulator source) {
            this.bucketSizer = source.bucketSizer;
            this.bodyBytes = source.bodyBytes;
            this.codecRepresentable = source.codecRepresentable;
        }

        void addBucket(IndexBatch batch) {
            if (!codecRepresentable) {
                return;
            }
            bucketSizer.clear();
            fillBucketRequest(bucketSizer, batch);
            int bucketBodyBytes = bucketSizer.totalSize();
            if (bucketBodyBytes < 0) {
                codecRepresentable = false;
                return;
            }
            bodyBytes +=
                    (long) BUCKETS_REQ_TAG_SIZE
                            + ProtoCodecUtils.computeVarIntSize(bucketBodyBytes)
                            + bucketBodyBytes;
            codecRepresentable = bodyBytes <= Integer.MAX_VALUE;
        }

        long framedBytes() {
            return bodyBytes + REQUEST_HEADER_LENGTH + LENGTH_PREFIX_BYTES;
        }

        boolean codecRepresentable() {
            return codecRepresentable && framedBytes() <= MAX_CODEC_FRAMED_BYTES;
        }

        boolean exceedsLimit(long maxTransportRequestBytes) {
            return !codecRepresentable() || framedBytes() > maxTransportRequestBytes;
        }

        RecordTooLargeException oversizedFailure(IndexBatch batch, long maxTransportRequestBytes) {
            String prefix =
                    "Index PutIndex request from "
                            + batch.sourceBucket()
                            + " to "
                            + batch.targetBucket()
                            + " at source end offset "
                            + batch.sourceEndOffset();
            long limit = codecRepresentable() ? maxTransportRequestBytes : MAX_CODEC_FRAMED_BYTES;
            String option =
                    codecRepresentable()
                            ? "netty.server.max-request-size="
                            : "codec maximum framed size=";
            return new RecordTooLargeException(
                    prefix + " is " + framedBytes() + " bytes, exceeding " + option + limit);
        }

        SizeAccumulator copy() {
            return new SizeAccumulator(this);
        }
    }
}
