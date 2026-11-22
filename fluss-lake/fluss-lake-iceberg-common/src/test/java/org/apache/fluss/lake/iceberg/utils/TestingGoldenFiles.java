/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.source.IcebergSplit;
import org.apache.fluss.lake.iceberg.tiering.IcebergCommittable;
import org.apache.fluss.lake.iceberg.tiering.IcebergWriteResult;

import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden files of the Iceberg serializers. A golden file is the output of a serializer version for
 * a fixed input. It must not change once released, since it guards the state of existing jobs.
 *
 * <p>To regenerate the golden files of a serializer test, run it with {@code
 * -Dfluss.iceberg.regenerate-golden-files=true}. Only regenerate a version when its format changes
 * on purpose.
 */
public class TestingGoldenFiles {

    private static final String REGENERATE_PROPERTY = "fluss.iceberg.regenerate-golden-files";

    /** The Iceberg version that wrote the golden files. */
    private static final String GOLDEN_ICEBERG_VERSION = "1.10.";

    /** The schema of the golden table: a primary key {@code id} and a partition column. */
    private static final Schema SCHEMA =
            new Schema(
                    required(1, "id", Types.IntegerType.get()),
                    optional(2, "name", Types.StringType.get()),
                    required(3, "dt", Types.StringType.get()));

    /** The spec of the golden table: the Fluss partition column and the Fluss bucket. */
    private static final PartitionSpec SPEC =
            PartitionSpec.builderFor(SCHEMA).identity("dt").bucket("id", 4).build();

    /** The specs of the golden table by spec id. */
    public static final Map<Integer, PartitionSpec> SPECS =
            Collections.singletonMap(SPEC.specId(), SPEC);

    /** The golden directory of {@link IcebergSplit}. */
    public static final String SPLIT_DIR = "golden/iceberg-split";

    /** The golden directory of {@link IcebergWriteResult}. */
    public static final String WRITE_RESULT_DIR = "golden/iceberg-write-result";

    /** The golden directory of {@link IcebergCommittable}. */
    public static final String COMMITTABLE_DIR = "golden/iceberg-committable";

    private static final String PARTITION_PATH = "dt=2026-10-15/id_bucket=2";

    private TestingGoldenFiles() {}

    /**
     * Returns whether the golden files should be regenerated. They are only regenerated with the
     * Iceberg version that wrote them.
     */
    public static boolean shouldRegenerate() {
        return Boolean.parseBoolean(System.getProperty(REGENERATE_PROPERTY))
                && isGoldenIcebergVersion();
    }

    /** Returns whether the tests run with the Iceberg version that wrote the golden files. */
    public static boolean isGoldenIcebergVersion() {
        return IcebergBuild.version().startsWith(GOLDEN_ICEBERG_VERSION);
    }

    /**
     * Returns the first golden file version that the running Iceberg version can read. Version 1 is
     * Java serialization of the Iceberg classes that wrote it.
     */
    public static int firstReadableVersion() {
        return isGoldenIcebergVersion() ? 1 : 2;
    }

    /** Reads the golden file of the given version from the test resources. */
    public static byte[] read(String dir, int version) throws IOException {
        String resource = dir + "/" + fileName(version);
        try (InputStream in =
                TestingGoldenFiles.class.getClassLoader().getResourceAsStream(resource)) {
            assertThat(in).as("golden file %s", resource).isNotNull();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        }
    }

    /** Writes the golden file of the given version to the test resources of this module. */
    public static void write(String dir, int version, byte[] bytes) throws IOException {
        Path path = Paths.get("src", "test", "resources", dir);
        Files.createDirectories(path);
        Files.write(path.resolve(fileName(version)), bytes);
    }

    /**
     * Creates the golden split. It covers a partition spec with a bucket transform, an equality
     * delete file and a residual filter, as planned for a partitioned primary-key table.
     */
    public static IcebergSplit goldenSplit() {
        FileScanTask task =
                new BaseFileScanTask(
                        dataFile("data-0.parquet", 10L),
                        new DeleteFile[] {equalityDeleteFile("eq-delete-0.parquet", 2L)},
                        SchemaParser.toJson(SCHEMA),
                        PartitionSpecParser.toJson(SPEC),
                        ResidualEvaluator.of(SPEC, Expressions.greaterThan("id", 5), true));
        return new IcebergSplit(task, 2, Collections.singletonList("2026-10-15"));
    }

    /**
     * Creates the golden write result. It covers data files, equality and position delete files,
     * referenced data files, rewritten delete files and a rewrite result.
     */
    public static IcebergWriteResult goldenWriteResult() {
        DataFile dataFile1 = dataFile("data-1.parquet", 10L);
        WriteResult writeResult =
                WriteResult.builder()
                        .addDataFiles(dataFile1, dataFile("data-2.parquet", 20L))
                        .addDeleteFiles(
                                equalityDeleteFile("eq-delete-1.parquet", 2L),
                                positionDeleteFile("pos-delete-1.parquet", 3L))
                        .addReferencedDataFiles(dataFile1.location())
                        .addRewrittenDeleteFiles(positionDeleteFile("pos-delete-0.parquet", 1L))
                        .build();
        return new IcebergWriteResult(writeResult, SPECS, goldenRewriteDataFileResult());
    }

    /**
     * Creates the golden committable. It covers data files, equality and position delete files and
     * a rewrite result.
     */
    public static IcebergCommittable goldenCommittable() {
        return IcebergCommittable.builder()
                .addSpecs(SPECS)
                .addDataFile(dataFile("data-1.parquet", 10L))
                .addDataFile(dataFile("data-2.parquet", 20L))
                .addDeleteFile(equalityDeleteFile("eq-delete-1.parquet", 2L))
                .addDeleteFile(positionDeleteFile("pos-delete-1.parquet", 3L))
                .addRewriteDataFileResult(goldenRewriteDataFileResult())
                .build();
    }

    /** Asserts that two splits are equal. */
    public static void assertSplitEquals(IcebergSplit actual, IcebergSplit expected) {
        assertThat(actual.bucket()).isEqualTo(expected.bucket());
        assertThat(actual.partition()).isEqualTo(expected.partition());

        FileScanTask actualTask = actual.fileScanTask();
        FileScanTask expectedTask = expected.fileScanTask();
        assertThat(actualTask.schema().asStruct()).isEqualTo(expectedTask.schema().asStruct());
        assertThat(actualTask.spec()).isEqualTo(expectedTask.spec());
        assertThat(actualTask.start()).isEqualTo(expectedTask.start());
        assertThat(actualTask.length()).isEqualTo(expectedTask.length());
        assertThat(actualTask.residual().toString()).isEqualTo(expectedTask.residual().toString());
        assertContentFileEquals(actualTask.file(), expectedTask.file());
        assertContentFilesEqual(actualTask.deletes(), expectedTask.deletes());
    }

    /** Asserts that two write results are equal. */
    public static void assertWriteResultEquals(
            IcebergWriteResult actual, IcebergWriteResult expected) {
        WriteResult actualResult = actual.getWriteResult();
        WriteResult expectedResult = expected.getWriteResult();
        assertContentFilesEqual(
                Arrays.asList(actualResult.dataFiles()), Arrays.asList(expectedResult.dataFiles()));
        assertContentFilesEqual(
                Arrays.asList(actualResult.deleteFiles()),
                Arrays.asList(expectedResult.deleteFiles()));
        assertThat(toStrings(actualResult.referencedDataFiles()))
                .isEqualTo(toStrings(expectedResult.referencedDataFiles()));
        assertContentFilesEqual(
                Arrays.asList(actualResult.rewrittenDeleteFiles()),
                Arrays.asList(expectedResult.rewrittenDeleteFiles()));
        assertRewriteDataFileResultEquals(
                actual.rewriteDataFileResult(), expected.rewriteDataFileResult());
    }

    /** Asserts that two committables are equal. */
    public static void assertCommittableEquals(
            IcebergCommittable actual, IcebergCommittable expected) {
        assertContentFilesEqual(actual.getDataFiles(), expected.getDataFiles());
        assertContentFilesEqual(actual.getDeleteFiles(), expected.getDeleteFiles());
        List<RewriteDataFileResult> actualRewrites = actual.rewriteDataFileResults();
        List<RewriteDataFileResult> expectedRewrites = expected.rewriteDataFileResults();
        assertThat(actualRewrites).hasSameSizeAs(expectedRewrites);
        for (int i = 0; i < expectedRewrites.size(); i++) {
            assertRewriteDataFileResultEquals(actualRewrites.get(i), expectedRewrites.get(i));
        }
    }

    /** Creates a data file of the golden table. */
    private static DataFile dataFile(String name, long recordCount) {
        return DataFiles.builder(SPEC)
                .withPath(location(name))
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath(PARTITION_PATH)
                .withFileSizeInBytes(recordCount * 100)
                .withRecordCount(recordCount)
                .build();
    }

    /** Creates an equality delete file on {@code id} of the golden table. */
    private static DeleteFile equalityDeleteFile(String name, long recordCount) {
        return FileMetadata.deleteFileBuilder(SPEC)
                .ofEqualityDeletes(1)
                .withPath(location(name))
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath(PARTITION_PATH)
                .withFileSizeInBytes(recordCount * 100)
                .withRecordCount(recordCount)
                .build();
    }

    /** Creates a position delete file of the golden table. */
    private static DeleteFile positionDeleteFile(String name, long recordCount) {
        return FileMetadata.deleteFileBuilder(SPEC)
                .ofPositionDeletes()
                .withPath(location(name))
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath(PARTITION_PATH)
                .withFileSizeInBytes(recordCount * 100)
                .withRecordCount(recordCount)
                .build();
    }

    /** Asserts that two collections of data or delete files are equal, in iteration order. */
    private static void assertContentFilesEqual(
            Collection<? extends ContentFile<?>> actual,
            Collection<? extends ContentFile<?>> expected) {
        assertThat(actual).hasSameSizeAs(expected);
        Iterator<? extends ContentFile<?>> actualIterator = actual.iterator();
        for (ContentFile<?> expectedFile : expected) {
            assertContentFileEquals(actualIterator.next(), expectedFile);
        }
    }

    /** Asserts that two data or delete files are equal. */
    private static void assertContentFileEquals(ContentFile<?> actual, ContentFile<?> expected) {
        assertThat(actual.location()).isEqualTo(expected.location());
        assertThat(actual.content()).isEqualTo(expected.content());
        assertThat(actual.format()).isEqualTo(expected.format());
        assertThat(actual.specId()).isEqualTo(expected.specId());
        assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
        assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
        assertThat(actual.equalityFieldIds()).isEqualTo(expected.equalityFieldIds());
        assertThat(actual.partition().size()).isEqualTo(expected.partition().size());
        for (int i = 0; i < expected.partition().size(); i++) {
            assertThat(actual.partition().get(i, Object.class))
                    .isEqualTo(expected.partition().get(i, Object.class));
        }
    }

    private static void assertRewriteDataFileResultEquals(
            RewriteDataFileResult actual, RewriteDataFileResult expected) {
        if (expected == null) {
            assertThat(actual).isNull();
            return;
        }
        assertThat(actual).isNotNull();
        assertThat(actual.snapshotId()).isEqualTo(expected.snapshotId());
        assertContentFilesEqual(actual.deletedDataFiles(), expected.deletedDataFiles());
        assertContentFilesEqual(actual.addedDataFiles(), expected.addedDataFiles());
    }

    private static RewriteDataFileResult goldenRewriteDataFileResult() {
        return new RewriteDataFileResult(
                42L,
                DataFileSet.of(Collections.singletonList(dataFile("data-0.parquet", 5L))),
                DataFileSet.of(Collections.singletonList(dataFile("compacted-0.parquet", 5L))));
    }

    private static List<String> toStrings(CharSequence[] values) {
        return Arrays.stream(values).map(CharSequence::toString).collect(Collectors.toList());
    }

    /** Version 1 is Java serialization, later versions are JSON documents. */
    private static String fileName(int version) {
        return version == 1 ? "v1.bin" : "v" + version + ".json";
    }

    private static String location(String name) {
        return "file:/fluss/golden/" + PARTITION_PATH + "/" + name;
    }
}
