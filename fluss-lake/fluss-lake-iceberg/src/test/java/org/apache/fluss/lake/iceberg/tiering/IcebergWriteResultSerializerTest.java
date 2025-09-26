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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

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

        IcebergWriteResult originalResult = new IcebergWriteResult(writeResult, null);

        // 2. Act: Serialize and then deserialize the object
        byte[] serializedData = serializer.serialize(originalResult);
        IcebergWriteResult deserializedResult =
                serializer.deserialize(serializer.getVersion(), serializedData);
        assertThat(deserializedResult.toString()).isEqualTo(originalResult.toString());

        // with rewrite result
        RewriteDataFileResult rewriteDataFileResult =
                new RewriteDataFileResult(
                        1L,
                        Collections.singletonList(dataFile),
                        Collections.singletonList(dataFile));
        originalResult = new IcebergWriteResult(writeResult, rewriteDataFileResult);
        serializedData = serializer.serialize(originalResult);
        deserializedResult = serializer.deserialize(serializer.getVersion(), serializedData);
        assertThat(deserializedResult.toString()).isEqualTo(originalResult.toString());
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
