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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.hudi.utils.HudiTableInfo;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.metadata.TablePath;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** Planner for creating Hudi splits. */
public class HudiSplitPlanner implements Planner<HudiSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(HudiSplitPlanner.class);

    private final Configuration hudiConfig;
    private final TablePath tablePath;
    private final long snapshotId;

    public HudiSplitPlanner(Configuration hudiConfig, TablePath tablePath, long snapshotId) {
        this.hudiConfig = hudiConfig;
        this.tablePath = tablePath;
        this.snapshotId = snapshotId;
    }

    @Override
    public List<HudiSplit> plan() throws IOException {
        String snapshotTime = toHudiInstantTime(snapshotId);
        try (HudiTableInfo hudiTableInfo = HudiTableInfo.create(tablePath, hudiConfig)) {
            if (!hudiTableInfo.getCompletedTimeline().containsInstant(snapshotTime)) {
                throw new IOException(
                        String.format(
                                "Hudi instant time %s does not exist in table %s.",
                                snapshotTime, tablePath));
            }

            List<String> partitionPaths =
                    FSUtils.getAllPartitionPaths(
                            hudiTableInfo.getEngineContext(), hudiTableInfo.getMetaClient(), false);
            if (partitionPaths.isEmpty()) {
                if (hudiTableInfo.isPartitioned()) {
                    LOG.info(
                            "Planned no Hudi splits for partitioned table {} with table type {} at instant {} because no Hudi partition paths were discovered.",
                            tablePath,
                            hudiTableInfo.getTableType(),
                            snapshotTime);
                    return Collections.emptyList();
                } else {
                    partitionPaths = Collections.singletonList("");
                }
            }

            List<HudiSplit> splits = new ArrayList<>();
            for (String partitionPath : partitionPaths) {
                splits.addAll(planPartition(hudiTableInfo, snapshotTime, partitionPath));
            }
            if (splits.isEmpty()) {
                LOG.info(
                        "Planned no Hudi splits for table {} at instant {} across {} Hudi partition paths.",
                        tablePath,
                        snapshotTime,
                        partitionPaths.size());
            } else {
                LOG.debug(
                        "Planned {} Hudi splits for table {} at instant {}.",
                        splits.size(),
                        tablePath,
                        snapshotTime);
            }
            return splits;
        }
    }

    @VisibleForTesting
    static String toHudiInstantTime(long snapshotId) {
        return String.format(Locale.ROOT, "%017d", snapshotId);
    }

    private List<HudiSplit> planPartition(
            HudiTableInfo hudiTableInfo, String snapshotTime, String partitionPath)
            throws IOException {
        HoodieTableFileSystemView fileSystemView = hudiTableInfo.getFileSystemView();
        List<HudiSplit> splits = new ArrayList<>();
        if (hudiTableInfo.getTableType() == HoodieTableType.MERGE_ON_READ) {
            List<FileSlice> fileSlices =
                    fileSystemView
                            .getLatestMergedFileSlicesBeforeOrOn(partitionPath, snapshotTime)
                            .collect(Collectors.toList());
            for (FileSlice fileSlice : fileSlices) {
                splits.add(toHudiSplit(hudiTableInfo, partitionPath, fileSlice));
            }
            return splits;
        } else if (hudiTableInfo.getTableType() == HoodieTableType.COPY_ON_WRITE) {
            List<HoodieBaseFile> baseFiles =
                    fileSystemView
                            .getLatestBaseFilesBeforeOrOn(partitionPath, snapshotTime)
                            .collect(Collectors.toList());
            for (HoodieBaseFile baseFile : baseFiles) {
                splits.add(
                        toHudiSplit(
                                hudiTableInfo,
                                partitionPath,
                                toFileSlice(partitionPath, baseFile)));
            }
            return splits;
        }
        throw new IOException(
                String.format(
                        "Unsupported Hudi table type %s for table %s.",
                        hudiTableInfo.getTableType(), tablePath));
    }

    private FileSlice toFileSlice(String partitionPath, HoodieBaseFile baseFile) {
        return new FileSlice(
                new HoodieFileGroupId(partitionPath, baseFile.getFileId()),
                baseFile.getCommitTime(),
                baseFile,
                Collections.emptyList());
    }

    private HudiSplit toHudiSplit(
            HudiTableInfo hudiTableInfo, String partitionPath, FileSlice fileSlice)
            throws IOException {
        return new HudiSplit(
                fileSlice,
                extractBucket(hudiTableInfo, fileSlice),
                hudiTableInfo.partitionValues(partitionPath));
    }

    private int extractBucket(HudiTableInfo hudiTableInfo, FileSlice fileSlice) throws IOException {
        if (!hudiTableInfo.isBucketAware()) {
            return -1;
        }
        String fileId = fileSlice.getFileGroupId().getFileId();
        if (fileId == null || fileId.isEmpty()) {
            throw new IOException(
                    String.format(
                            "Failed to extract Hudi bucket id for bucket-aware table %s because file id is empty.",
                            tablePath));
        }
        try {
            return BucketIdentifier.bucketIdFromFileId(fileId);
        } catch (RuntimeException e) {
            throw new IOException(
                    String.format(
                            "Failed to extract Hudi bucket id from file id '%s' for bucket-aware table %s.",
                            fileId, tablePath),
                    e);
        }
    }
}
