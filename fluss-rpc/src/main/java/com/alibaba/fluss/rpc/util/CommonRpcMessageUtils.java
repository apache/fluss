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

package com.alibaba.fluss.rpc.util;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.remote.RemoteLogFetchInfo;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.messages.PbAclFilter;
import com.alibaba.fluss.rpc.messages.PbAclInfo;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbKeyValue;
import com.alibaba.fluss.rpc.messages.PbPartitionSpec;
import com.alibaba.fluss.rpc.messages.PbRemoteLogFetchInfo;
import com.alibaba.fluss.rpc.messages.PbRemoteLogSegment;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.security.acl.AccessControlEntry;
import com.alibaba.fluss.security.acl.AccessControlEntryFilter;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceFilter;
import com.alibaba.fluss.security.acl.ResourceType;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Utils for making rpc request/response from inner object or convert inner class to rpc
 * request/response for client and server.
 */
public class CommonRpcMessageUtils {

    public static List<PbAclInfo> toPbAclInfos(Collection<AclBinding> aclBindings) {
        return aclBindings.stream()
                .map(CommonRpcMessageUtils::toPbAclInfo)
                .collect(Collectors.toList());
    }

    public static List<AclBinding> toAclBindings(Collection<PbAclInfo> pbAclInfos) {
        return pbAclInfos.stream()
                .map(CommonRpcMessageUtils::toAclBinding)
                .collect(Collectors.toList());
    }

    public static List<PbAclFilter> toPbAclBindingFilters(
            Collection<AclBindingFilter> aclBindingFilters) {
        return aclBindingFilters.stream()
                .map(CommonRpcMessageUtils::toPbAclFilter)
                .collect(Collectors.toList());
    }

    public static List<AclBindingFilter> toAclBindingFilters(Collection<PbAclFilter> pbAclFilters) {
        return pbAclFilters.stream()
                .map(CommonRpcMessageUtils::toAclFilter)
                .collect(Collectors.toList());
    }

    public static AclBinding toAclBinding(PbAclInfo pbAclInfo) {
        return new AclBinding(
                new Resource(
                        ResourceType.fromCode((byte) pbAclInfo.getResourceType()),
                        pbAclInfo.getResourceName()),
                new AccessControlEntry(
                        new FlussPrincipal(
                                pbAclInfo.getPrincipalName(), pbAclInfo.getPrincipalType()),
                        pbAclInfo.getHost(),
                        OperationType.fromCode((byte) pbAclInfo.getOperationType()),
                        PermissionType.fromCode((byte) pbAclInfo.getPermissionType())));
    }

    public static PbAclInfo toPbAclInfo(AclBinding aclBinding) {
        return new PbAclInfo()
                .setResourceType(aclBinding.getResource().getType().getCode())
                .setResourceName(aclBinding.getResource().getName())
                .setPrincipalName(aclBinding.getAccessControlEntry().getPrincipal().getName())
                .setPrincipalType(aclBinding.getAccessControlEntry().getPrincipal().getType())
                .setHost(aclBinding.getAccessControlEntry().getHost())
                .setOperationType(aclBinding.getAccessControlEntry().getOperationType().getCode())
                .setPermissionType(
                        aclBinding.getAccessControlEntry().getPermissionType().getCode());
    }

    public static PbAclFilter toPbAclFilter(AclBindingFilter aclBindingFilter) {
        AccessControlEntryFilter accessControlEntryFilter = aclBindingFilter.getEntryFilter();
        ResourceFilter resourceFilter = aclBindingFilter.getResourceFilter();

        PbAclFilter pbAclFilter = new PbAclFilter();
        pbAclFilter
                .setResourceType(resourceFilter.getType().getCode())
                .setOperationType(accessControlEntryFilter.getOperationType().getCode())
                .setPermissionType(accessControlEntryFilter.getPermissionType().getCode());
        if (resourceFilter.getName() != null) {
            pbAclFilter.setResourceName(resourceFilter.getName());
        }

        if (accessControlEntryFilter.getPrincipal() != null
                && accessControlEntryFilter.getPrincipal().getName() != null) {
            pbAclFilter.setPrincipalName(accessControlEntryFilter.getPrincipal().getName());
        }

        if (accessControlEntryFilter.getPrincipal() != null
                && accessControlEntryFilter.getPrincipal().getType() != null) {
            pbAclFilter.setPrincipalType(accessControlEntryFilter.getPrincipal().getType());
        }

        if (accessControlEntryFilter.getHost() != null) {
            pbAclFilter.setHost(accessControlEntryFilter.getHost());
        }
        return pbAclFilter;
    }

    public static AclBindingFilter toAclFilter(PbAclFilter pbAclFilter) {
        return new AclBindingFilter(
                new ResourceFilter(
                        ResourceType.fromCode((byte) pbAclFilter.getResourceType()),
                        pbAclFilter.hasResourceName() ? pbAclFilter.getResourceName() : null),
                new AccessControlEntryFilter(
                        pbAclFilter.hasPrincipalName() && pbAclFilter.hasPrincipalType()
                                ? new FlussPrincipal(
                                        pbAclFilter.getPrincipalName(),
                                        pbAclFilter.getPrincipalType())
                                : null,
                        pbAclFilter.hasHost() ? pbAclFilter.getHost() : null,
                        OperationType.fromCode((byte) pbAclFilter.getOperationType()),
                        PermissionType.fromCode((byte) pbAclFilter.getPermissionType())));
    }

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

    public static ResolvedPartitionSpec toResolvedPartitionSpec(PbPartitionSpec pbPartitionSpec) {
        List<String> partitionKeys = new ArrayList<>();
        List<String> partitionValues = new ArrayList<>();
        for (PbKeyValue pbKeyValue : pbPartitionSpec.getPartitionKeyValuesList()) {
            partitionKeys.add(pbKeyValue.getKey());
            partitionValues.add(pbKeyValue.getValue());
        }
        return new ResolvedPartitionSpec(partitionKeys, partitionValues);
    }
}
