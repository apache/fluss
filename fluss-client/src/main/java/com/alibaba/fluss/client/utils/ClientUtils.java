/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.utils;

import com.alibaba.fluss.client.lakehouse.LakeTableBucketAssigner;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.table.getter.PartitionGetter;
import com.alibaba.fluss.client.write.HashBucketAssigner;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.exception.IllegalConfigurationException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Utils for Fluss Client. */
public final class ClientUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ClientUtils.class);

    private static final Pattern HOST_PORT_PATTERN =
            Pattern.compile(".*?\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)");

    private ClientUtils() {}

    // todo: may add DnsLookup
    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls) {
        if (urls == null) {
            throw new IllegalConfigurationException(
                    ConfigOptions.BOOTSTRAP_SERVERS.key() + " should be set.");
        }
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (String url : urls) {
            if (url != null && !url.isEmpty()) {
                try {
                    String host = getHost(url);
                    Integer port = getPort(url);
                    if (host == null || port == null) {
                        throw new IllegalConfigurationException(
                                "Invalid url in "
                                        + ConfigOptions.BOOTSTRAP_SERVERS.key()
                                        + ": "
                                        + url);
                    }
                    InetSocketAddress address = new InetSocketAddress(host, port);
                    if (address.isUnresolved()) {
                        LOG.warn(
                                "Couldn't resolve server {} from {} as DNS resolution failed for {}",
                                url,
                                ConfigOptions.BOOTSTRAP_SERVERS.key(),
                                host);
                    } else {
                        addresses.add(address);
                    }
                } catch (IllegalArgumentException e) {
                    throw new IllegalConfigurationException(
                            "Invalid port in "
                                    + ConfigOptions.BOOTSTRAP_SERVERS.key()
                                    + ": "
                                    + url);
                }
            }
        }
        if (addresses.isEmpty()) {
            throw new IllegalConfigurationException(
                    "No resolvable bootstrap urls given in "
                            + ConfigOptions.BOOTSTRAP_SERVERS.key());
        }
        return addresses;
    }

    /**
     * Extracts the hostname from a "host:port" address string.
     *
     * @param address address string to parse
     * @return hostname or null if the given address is incorrect
     */
    public static String getHost(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? matcher.group(1) : null;
    }

    /**
     * Extracts the port number from a "host:port" address string.
     *
     * @param address address string to parse
     * @return port number or null if the given address is incorrect
     */
    public static Integer getPort(String address) {
        Matcher matcher = HOST_PORT_PATTERN.matcher(address);
        return matcher.matches() ? Integer.parseInt(matcher.group(2)) : null;
    }

    /**
     * Return the id of the partition the row belongs to. It'll try to update the metadata if the
     * partition doesn't exist. If the partition doesn't exist yet after update metadata, it'll
     * throw {@link PartitionNotExistException}.
     */
    public static Long getPartitionId(
            InternalRow row,
            PartitionGetter partitionGetter,
            TablePath tablePath,
            MetadataUpdater metadataUpdater) {
        checkNotNull(partitionGetter, "partitionGetter shouldn't be null.");
        String partitionName = partitionGetter.getPartition(row);
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);
        metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        return metadataUpdater.getCluster().getPartitionIdOrElseThrow(physicalTablePath);
    }

    public static int getBucketId(
            byte[] keyBytes,
            InternalRow key,
            LakeTableBucketAssigner lakeTableBucketAssigner,
            boolean isDataLakeEnable,
            int numBuckets,
            MetadataUpdater metadataUpdater) {
        if (!isDataLakeEnable) {
            return HashBucketAssigner.bucketForRowKey(keyBytes, numBuckets);
        } else {
            return lakeTableBucketAssigner.assignBucket(
                    keyBytes, key, metadataUpdater.getCluster());
        }
    }
}
