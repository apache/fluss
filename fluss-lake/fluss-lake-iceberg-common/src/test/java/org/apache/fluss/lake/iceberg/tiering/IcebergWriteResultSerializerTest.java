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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.WRITE_RESULT_DIR;
import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.assertWriteResultEquals;
import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.goldenWriteResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** UT for {@link IcebergWriteResultSerializer}. */
class IcebergWriteResultSerializerTest {

    private IcebergWriteResultSerializer serializer;
    private Schema schema;
    private PartitionSpec spec;
    private GenericRecord partitionData;

    @BeforeEach
    void setUp() {
        serializer = new IcebergWriteResultSerializer();
        schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "category", Types.StringType.get()));
        spec = PartitionSpec.builderFor(schema).identity("category").build();
        partitionData = GenericRecord.create(spec.partitionType());
        partitionData.setField("category", "A");
    }

    @Test
    void testSerializeAndDeserializeWithAllFiles() throws IOException {
        // 1. Arrange: Create a complex write result
        DataFile dataFile = createDataFile("/data/file1.parquet", 100L);
        DeleteFile deleteFile = createDeleteFile("/data/deletes1.parquet", 10L);

        // no rewrite result
        WriteResult writeResult =
                WriteResult.builder().addDataFiles(dataFile).addDeleteFiles(deleteFile).build();

        IcebergWriteResult originalResult = new IcebergWriteResult(writeResult, specs(), null);

        // 2. Act: Serialize and then deserialize the object
        byte[] serializedData = serializer.serialize(originalResult);
        IcebergWriteResult deserializedResult =
                serializer.deserialize(serializer.getVersion(), serializedData);
        assertThat(deserializedResult.toString()).isEqualTo(originalResult.toString());

        // with rewrite result
        RewriteDataFileResult rewriteDataFileResult =
                new RewriteDataFileResult(
                        1L,
                        DataFileSet.of(Collections.singletonList(dataFile)),
                        DataFileSet.of(Collections.singletonList(dataFile)));
        originalResult = new IcebergWriteResult(writeResult, specs(), rewriteDataFileResult);
        serializedData = serializer.serialize(originalResult);
        deserializedResult = serializer.deserialize(serializer.getVersion(), serializedData);
        assertThat(deserializedResult.toString()).isEqualTo(originalResult.toString());
    }

    @Test
    void testDeserializeGoldenFiles() throws Exception {
        IcebergWriteResult expected = goldenWriteResult();
        for (int version = TestingGoldenFiles.firstReadableVersion();
                version <= serializer.getVersion();
                version++) {
            IcebergWriteResult actual =
                    serializer.deserialize(
                            version, TestingGoldenFiles.read(WRITE_RESULT_DIR, version));
            assertWriteResultEquals(actual, expected);
        }
    }

    @Test
    void testCurrentVersionMatchesGoldenFile() throws Exception {
        // other Iceberg versions write the Iceberg objects in the JSON differently
        assumeThat(TestingGoldenFiles.isGoldenIcebergVersion())
                .as("the golden files were written with another Iceberg version")
                .isTrue();
        // the current format must not change without bumping the version
        assertThat(serializer.serialize(goldenWriteResult()))
                .isEqualTo(TestingGoldenFiles.read(WRITE_RESULT_DIR, serializer.getVersion()));
    }

    @Test
    void testRegenerateGoldenFiles() throws Exception {
        assumeThat(TestingGoldenFiles.shouldRegenerate())
                .as("golden files are only regenerated on request")
                .isTrue();
        IcebergWriteResult goldenWriteResult = goldenWriteResult();
        TestingGoldenFiles.write(WRITE_RESULT_DIR, 1, serializeV1(goldenWriteResult));
        TestingGoldenFiles.write(
                WRITE_RESULT_DIR, serializer.getVersion(), serializer.serialize(goldenWriteResult));
    }

    @Test
    void testSerializeWithoutSpecs() {
        IcebergWriteResult withoutSpecs =
                new IcebergWriteResult(goldenWriteResult().getWriteResult(), null, null);
        assertThatThrownBy(() -> serializer.serialize(withoutSpecs))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without partition specs");
    }

    @Test
    void testDeserializeInvalidData() throws Exception {
        byte[] serialized = serializer.serialize(goldenWriteResult());
        assertThatThrownBy(() -> serializer.deserialize(2, "{}".getBytes()))
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("Missing field");
        assertThatThrownBy(() -> serializer.deserialize(2, "invalid".getBytes()))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> serializer.deserialize(3, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported IcebergWriteResult serializer version 3");
    }

    /** Serializes a write result like version 1 of {@link IcebergWriteResultSerializer} did. */
    private static byte[] serializeV1(IcebergWriteResult writeResult) throws IOException {
        byte[] writeResultBytes = InstantiationUtils.serializeObject(writeResult.getWriteResult());
        byte[] rewriteResultBytes =
                InstantiationUtils.serializeObject(writeResult.rewriteDataFileResult());
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(writeResultBytes.length);
            dos.write(writeResultBytes);
            dos.writeBoolean(true);
            dos.writeInt(rewriteResultBytes.length);
            dos.write(rewriteResultBytes);
            dos.flush();
            return baos.toByteArray();
        }
    }

    private Map<Integer, PartitionSpec> specs() {
        return Collections.singletonMap(spec.specId(), spec);
    }

    private DeleteFile createDeleteFile(String path, long recordCount) {
        return FileMetadata.deleteFileBuilder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withPartition(partitionData)
                .withRecordCount(recordCount)
                .withFormat(FileFormat.PARQUET)
                .ofPositionDeletes()
                .build();
    }

    private DataFile createDataFile(String path, long recordCount) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(4096)
                .withPartition(partitionData)
                .withRecordCount(recordCount)
                .withFormat(FileFormat.PARQUET)
                .build();
    }
}
