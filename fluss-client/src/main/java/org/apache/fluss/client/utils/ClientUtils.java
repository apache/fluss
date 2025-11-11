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

package org.apache.fluss.client.utils;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.getter.PartialPartitionGetter;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

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
            MetadataUpdater metadataUpdater)
            throws PartitionNotExistException {
        checkNotNull(partitionGetter, "partitionGetter shouldn't be null.");
        String partitionName = partitionGetter.getPartition(row);
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);
        metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        return metadataUpdater.getCluster().getPartitionIdOrElseThrow(physicalTablePath);
    }

    /**
     * Return all partition ids for the given table. If partialPartitionGetter is provided, only
     * return partition ids that match the partial partition specification.
     *
     * <p>This method now uses the cached partition information from MetadataUpdater.getPartitions()
     * instead of directly accessing cluster metadata, providing better performance and consistency.
     *
     * @param tablePath the table path
     * @param metadataUpdater the metadata updater
     * @param prefixKey the prefix key to extract partial partition values from
     * @param partialPartitionGetter optional partial partition getter for filtering
     * @return list of partition ids that match the criteria
     */
    public static List<Long> getPartitionIds(
            TablePath tablePath,
            MetadataUpdater metadataUpdater,
            InternalRow prefixKey,
            @Nullable PartialPartitionGetter partialPartitionGetter) {
        checkNotNull(tablePath, "tablePath shouldn't be null.");
        checkNotNull(metadataUpdater, "metadataUpdater shouldn't be null.");

        // Get partition information using the new cached method
        List<PartitionInfo> partitionInfos = metadataUpdater.getPartitions(tablePath);

        if (partitionInfos.isEmpty()) {
            LOG.debug("No partitions found for table: {}", tablePath);
            return Collections.emptyList();
        }

        // Convert PartitionInfo to partition IDs
        List<Long> candidatePartitionIds =
                partitionInfos.stream()
                        .map(PartitionInfo::getPartitionId)
                        .collect(Collectors.toList());

        // If no partial partition filtering is needed, return all partitions
        if (partialPartitionGetter == null) {
            return candidatePartitionIds;
        }

        // Extract partial partition values from prefix key
        Map<String, String> partialPartitionValues =
                partialPartitionGetter.getPartialPartitionSpec(prefixKey);

        if (partialPartitionValues.isEmpty()) {
            return candidatePartitionIds;
        }

        // Get table info to extract partition keys
        TableInfo tableInfo = metadataUpdater.getTableInfoOrElseThrow(tablePath);
        List<String> partitionKeys = tableInfo.getPartitionKeys();

        // Filter partitions based on partial partition values
        return partitionInfos.stream()
                .filter(
                        partitionInfo -> {
                            try {
                                String partitionName = partitionInfo.getPartitionName();
                                ResolvedPartitionSpec partitionSpec =
                                        ResolvedPartitionSpec.fromPartitionName(
                                                partitionKeys, partitionName);
                                return matchesPartialPartitionSpec(
                                        partitionSpec, partialPartitionValues);
                            } catch (Exception e) {
                                LOG.warn(
                                        "Failed to check partition {} for filtering, excluding from results",
                                        partitionInfo.getPartitionId(),
                                        e);
                                return false;
                            }
                        })
                .map(PartitionInfo::getPartitionId)
                .collect(Collectors.toList());
    }

    private static boolean matchesPartialPartitionSpec(
            ResolvedPartitionSpec partitionSpec, Map<String, String> partialPartitionValues) {
        Map<String, String> specMap = partitionSpec.toPartitionSpec().getSpecMap();
        for (Map.Entry<String, String> entry : partialPartitionValues.entrySet()) {
            if (!entry.getValue().equals(specMap.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
