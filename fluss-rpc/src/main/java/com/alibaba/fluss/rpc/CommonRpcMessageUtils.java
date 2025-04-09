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

package com.alibaba.fluss.rpc;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.remote.RemoteLogFetchInfo;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbRemoteLogFetchInfo;
import com.alibaba.fluss.rpc.messages.PbRemoteLogSegment;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Utils for making rpc request/response from inner object or convert inner class to rpc
 * request/response for common using.
 */
public class CommonRpcMessageUtils {
    public static FetchLogResultForBucket getFetchLogResultForBucket(
            TableBucket tb, TablePath tp, PbFetchLogRespForBucket respForBucket) {
        FetchLogResultForBucket fetchLogResultForBucket;
        if (respForBucket.hasErrorCode()) {
            fetchLogResultForBucket =
                    new FetchLogResultForBucket(tb, ApiError.fromErrorMessage(respForBucket));
        } else {
            if (respForBucket.hasRemoteLogFetchInfo()) {
                PbRemoteLogFetchInfo pbRlfInfo = respForBucket.getRemoteLogFetchInfo();
                String partitionName =
                        pbRlfInfo.hasPartitionName() ? pbRlfInfo.getPartitionName() : null;
                PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tp, partitionName);
                List<RemoteLogSegment> remoteLogSegmentList = new ArrayList<>();
                for (PbRemoteLogSegment pbRemoteLogSegment : pbRlfInfo.getRemoteLogSegmentsList()) {
                    RemoteLogSegment remoteLogSegment =
                            RemoteLogSegment.Builder.builder()
                                    .tableBucket(tb)
                                    .physicalTablePath(physicalTablePath)
                                    .remoteLogSegmentId(
                                            UUID.fromString(
                                                    pbRemoteLogSegment.getRemoteLogSegmentId()))
                                    .remoteLogEndOffset(pbRemoteLogSegment.getRemoteLogEndOffset())
                                    .remoteLogStartOffset(
                                            pbRemoteLogSegment.getRemoteLogStartOffset())
                                    .segmentSizeInBytes(pbRemoteLogSegment.getSegmentSizeInBytes())
                                    .maxTimestamp(-1L) // not use.
                                    .build();
                    remoteLogSegmentList.add(remoteLogSegment);
                }
                RemoteLogFetchInfo rlFetchInfo =
                        new RemoteLogFetchInfo(
                                pbRlfInfo.getRemoteLogTabletDir(),
                                pbRlfInfo.hasPartitionName() ? pbRlfInfo.getPartitionName() : null,
                                remoteLogSegmentList,
                                pbRlfInfo.getFirstStartPos());
                fetchLogResultForBucket =
                        new FetchLogResultForBucket(
                                tb, rlFetchInfo, respForBucket.getHighWatermark());
            } else {
                ByteBuffer recordsBuffer = toByteBuffer(respForBucket.getRecordsSlice());
                LogRecords records =
                        respForBucket.hasRecords()
                                ? MemoryLogRecords.pointToByteBuffer(recordsBuffer)
                                : MemoryLogRecords.EMPTY;
                fetchLogResultForBucket =
                        new FetchLogResultForBucket(tb, records, respForBucket.getHighWatermark());
            }
        }

        return fetchLogResultForBucket;
    }

    public static ByteBuffer toByteBuffer(ByteBuf buf) {
        if (buf.isDirect()) {
            return buf.nioBuffer();
        } else if (buf.hasArray()) {
            int offset = buf.arrayOffset() + buf.readerIndex();
            int length = buf.readableBytes();
            return ByteBuffer.wrap(buf.array(), offset, length);
        } else {
            // fallback to deep copy
            byte[] bytes = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), bytes);
            return ByteBuffer.wrap(bytes);
        }
    }
}
