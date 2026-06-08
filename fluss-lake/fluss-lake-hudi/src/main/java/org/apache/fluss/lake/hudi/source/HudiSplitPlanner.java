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
        String snapshotTime = String.valueOf(snapshotId);
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
                partitionPaths = Collections.singletonList("");
            }

            List<HudiSplit> splits = new ArrayList<>();
            for (String partitionPath : partitionPaths) {
                splits.addAll(planPartition(hudiTableInfo, snapshotTime, partitionPath));
            }
            LOG.debug(
                    "Planned {} Hudi splits for table {} at instant {}.",
                    splits.size(),
                    tablePath,
                    snapshotTime);
            return splits;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to plan Hudi splits for table " + tablePath + ".", e);
        }
    }

    private List<HudiSplit> planPartition(
            HudiTableInfo hudiTableInfo, String snapshotTime, String partitionPath) {
        HoodieTableFileSystemView fileSystemView = hudiTableInfo.getFileSystemView();
        if (hudiTableInfo.getTableType() == HoodieTableType.MERGE_ON_READ) {
            return fileSystemView
                    .getLatestMergedFileSlicesBeforeOrOn(partitionPath, snapshotTime)
                    .map(fileSlice -> toHudiSplit(hudiTableInfo, partitionPath, fileSlice))
                    .collect(Collectors.toList());
        }

        return fileSystemView
                .getLatestBaseFilesBeforeOrOn(partitionPath, snapshotTime)
                .map(baseFile -> toFileSlice(partitionPath, baseFile))
                .map(fileSlice -> toHudiSplit(hudiTableInfo, partitionPath, fileSlice))
                .collect(Collectors.toList());
    }

    private FileSlice toFileSlice(String partitionPath, HoodieBaseFile baseFile) {
        return new FileSlice(
                new HoodieFileGroupId(partitionPath, baseFile.getFileId()),
                baseFile.getCommitTime(),
                baseFile,
                Collections.emptyList());
    }

    private HudiSplit toHudiSplit(
            HudiTableInfo hudiTableInfo, String partitionPath, FileSlice fileSlice) {
        return new HudiSplit(
                fileSlice,
                extractBucket(hudiTableInfo, fileSlice),
                hudiTableInfo.partitionValues(partitionPath));
    }

    private int extractBucket(HudiTableInfo hudiTableInfo, FileSlice fileSlice) {
        if (!hudiTableInfo.isBucketAware()) {
            return -1;
        }
        String fileId = fileSlice.getFileGroupId().getFileId();
        if (fileId == null || fileId.isEmpty()) {
            return -1;
        }
        return BucketIdentifier.bucketIdFromFileId(fileId);
    }
}
