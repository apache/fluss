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
import org.apache.fluss.lake.hudi.HudiLakeCatalog;
import org.apache.fluss.lake.hudi.utils.HudiTableInfo;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HudiSplitPlanner}. */
class HudiSplitPlannerTest {

    private static final String INSTANT_TIME = "20260608010101000";
    private static final String LEADING_ZERO_INSTANT_TIME = "02026060801010100";
    private static final String DATABASE = "db1";
    private static final String TABLE = "table1";

    @TempDir private File tempWarehouseDir;

    private Configuration hudiConfig;

    @BeforeEach
    void setUp() {
        hudiConfig = new Configuration();
        hudiConfig.setString("catalog.path", tempWarehouseDir.toURI().toString());
        hudiConfig.setString("mode", "dfs");
    }

    @Test
    void testPlanCopyOnWriteSplitsForCompletedInstant() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, TABLE);
        createCowTable(tablePath);
        String partitionPath = "";
        int bucket = 1;
        String fileId = BucketIdentifier.newBucketFileIdPrefix(bucket);
        String fileName = createBaseFile(tablePath, partitionPath, INSTANT_TIME, fileId);
        createCompletedCommit(tablePath, partitionPath, INSTANT_TIME, fileId, fileName);
        assertHudiFixtureVisible(tablePath, partitionPath);

        List<HudiSplit> splits =
                new HudiSplitPlanner(hudiConfig, tablePath, Long.parseLong(INSTANT_TIME)).plan();

        assertThat(splits).hasSize(1);
        HudiSplit split = splits.get(0);
        assertThat(split.bucket()).isEqualTo(bucket);
        assertThat(split.partition()).isEmpty();
        assertThat(split.getFileSlice().getPartitionPath()).isEqualTo(partitionPath);
        assertThat(split.getFileSlice().getFileId()).isEqualTo(fileId);
        assertThat(split.getFileSlice().getBaseFile().isPresent()).isTrue();
    }

    @Test
    void testPlanFailsWhenInstantDoesNotExist() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, TABLE);
        createCowTable(tablePath);

        assertThatThrownBy(
                        () ->
                                new HudiSplitPlanner(
                                                hudiConfig, tablePath, Long.parseLong(INSTANT_TIME))
                                        .plan())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void testPlanLeadingZeroInstantTime() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, TABLE);
        createCowTable(tablePath);
        String partitionPath = "";
        int bucket = 1;
        String fileId = BucketIdentifier.newBucketFileIdPrefix(bucket);
        String fileName =
                createBaseFile(tablePath, partitionPath, LEADING_ZERO_INSTANT_TIME, fileId);
        createCompletedCommit(
                tablePath, partitionPath, LEADING_ZERO_INSTANT_TIME, fileId, fileName);

        List<HudiSplit> splits =
                new HudiSplitPlanner(
                                hudiConfig, tablePath, Long.parseLong(LEADING_ZERO_INSTANT_TIME))
                        .plan();

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getFileSlice().getBaseInstantTime())
                .isEqualTo(LEADING_ZERO_INSTANT_TIME);
    }

    @Test
    void testPlanPartitionedTableWithoutDiscoveredPartitionsReturnsEmptySplits() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, TABLE);
        createPartitionedCowTable(tablePath);
        createCompletedCommit(tablePath, INSTANT_TIME);

        List<HudiSplit> splits =
                new HudiSplitPlanner(hudiConfig, tablePath, Long.parseLong(INSTANT_TIME)).plan();

        assertThat(splits).isEmpty();
    }

    @Test
    void testPlanBucketUnawareCopyOnWriteSplitsAsBucketMinusOne() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, TABLE);
        createCowTable(tablePath, false);
        String partitionPath = "";
        String fileId = "plainfileid";
        String fileName = createBaseFile(tablePath, partitionPath, INSTANT_TIME, fileId);
        createCompletedCommit(tablePath, partitionPath, INSTANT_TIME, fileId, fileName);
        assertHudiFixtureVisible(tablePath, partitionPath);

        List<HudiSplit> splits =
                new HudiSplitPlanner(hudiConfig, tablePath, Long.parseLong(INSTANT_TIME)).plan();

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).bucket()).isEqualTo(-1);
    }

    @Test
    void testPlanFailsForBucketAwareTableWithUnparsableFileId() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, TABLE);
        createCowTable(tablePath);
        String partitionPath = "";
        String fileId = "plainfileid";
        String fileName = createBaseFile(tablePath, partitionPath, INSTANT_TIME, fileId);
        createCompletedCommit(tablePath, partitionPath, INSTANT_TIME, fileId, fileName);
        assertHudiFixtureVisible(tablePath, partitionPath);

        assertThatThrownBy(
                        () ->
                                new HudiSplitPlanner(
                                                hudiConfig, tablePath, Long.parseLong(INSTANT_TIME))
                                        .plan())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to extract Hudi bucket id")
                .hasMessageContaining(fileId);
    }

    private void createCowTable(TablePath tablePath) throws Exception {
        createCowTable(tablePath, true);
    }

    private void createCowTable(TablePath tablePath, boolean bucketAware) throws Exception {
        Schema schema = Schema.newBuilder().column("id", DataTypes.BIGINT()).build();
        TableDescriptor.Builder tableDescriptorBuilder =
                TableDescriptor.builder()
                        .schema(schema)
                        .property("hudi.hoodie.datasource.write.recordkey.field", "id");
        if (bucketAware) {
            tableDescriptorBuilder.distributedBy(4, "id");
        } else {
            tableDescriptorBuilder.distributedBy(4);
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        try (HudiLakeCatalog catalog = new HudiLakeCatalog(hudiConfig)) {
            catalog.createTable(tablePath, tableDescriptor, new TestingLakeCatalogContext());
        }
    }

    private void createPartitionedCowTable(TablePath tablePath) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("dt")
                        .distributedBy(4, "id")
                        .property("hudi.hoodie.datasource.write.recordkey.field", "id")
                        .build();
        try (HudiLakeCatalog catalog = new HudiLakeCatalog(hudiConfig)) {
            catalog.createTable(tablePath, tableDescriptor, new TestingLakeCatalogContext());
        }
    }

    private String createBaseFile(
            TablePath tablePath, String partitionPath, String instantTime, String fileId)
            throws IOException {
        File partitionDir =
                partitionPath.isEmpty()
                        ? basePath(tablePath)
                        : new File(basePath(tablePath), partitionPath);
        Files.createDirectories(partitionDir.toPath());
        String fileName =
                FSUtils.makeBaseFileName(
                        instantTime,
                        FSUtils.makeWriteToken(0, 0, 1),
                        fileId,
                        HoodieFileFormat.PARQUET.getFileExtension());
        Files.write(new File(partitionDir, fileName).toPath(), new byte[] {1});
        return fileName;
    }

    private void createCompletedCommit(
            TablePath tablePath,
            String partitionPath,
            String instantTime,
            String fileId,
            String fileName) {
        HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPartitionPath(partitionPath);
        writeStat.setFileId(fileId);
        writeStat.setPath(partitionPath.isEmpty() ? fileName : partitionPath + "/" + fileName);
        writeStat.setTotalWriteBytes(1);
        writeStat.setFileSizeInBytes(1);
        writeStat.setNumWrites(1);
        commitMetadata.addWriteStat(partitionPath, writeStat);

        createCompletedCommit(tablePath, instantTime, commitMetadata);
    }

    private void createCompletedCommit(TablePath tablePath, String instantTime) {
        createCompletedCommit(tablePath, instantTime, new HoodieCommitMetadata());
    }

    private void createCompletedCommit(
            TablePath tablePath, String instantTime, HoodieCommitMetadata commitMetadata) {
        HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder()
                        .setBasePath(basePath(tablePath).getAbsolutePath())
                        .setConf(
                                new HadoopStorageConfiguration(
                                        HudiTableInfo.getHadoopConfiguration(hudiConfig)))
                        .build();
        HoodieInstant inflightInstant =
                metaClient.createNewInstant(
                        HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime);
        metaClient.getActiveTimeline().createNewInstant(inflightInstant);

        HoodieInstant instant =
                metaClient
                        .getActiveTimeline()
                        .saveAsComplete(inflightInstant, Option.of(commitMetadata));
        assertThat(instant.isCompleted()).isTrue();
        metaClient.reloadActiveTimeline();
    }

    private void assertHudiFixtureVisible(TablePath tablePath, String partitionPath)
            throws IOException {
        try (HudiTableInfo hudiTableInfo = HudiTableInfo.create(tablePath, hudiConfig)) {
            assertThat(
                            FSUtils.getAllPartitionPaths(
                                    hudiTableInfo.getEngineContext(),
                                    hudiTableInfo.getMetaClient(),
                                    false))
                    .isEmpty();
            List<HoodieBaseFile> baseFiles =
                    hudiTableInfo
                            .getFileSystemView()
                            .getLatestBaseFilesBeforeOrOn(partitionPath, INSTANT_TIME)
                            .collect(Collectors.toList());
            assertThat(baseFiles).hasSize(1);
        }
    }

    private File basePath(TablePath tablePath) {
        return new File(
                new File(tempWarehouseDir, tablePath.getDatabaseName()), tablePath.getTableName());
    }
}
