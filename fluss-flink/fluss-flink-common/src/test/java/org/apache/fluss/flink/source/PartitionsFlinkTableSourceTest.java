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

package org.apache.fluss.flink.source;

import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionsFlinkTableSource}. */
class PartitionsFlinkTableSourceTest {

    @Test
    void testConvertPartitionInfoToInternalRow() {
        PartitionInfo partitionInfo =
                new PartitionInfo(
                        10L,
                        new ResolvedPartitionSpec(
                                Collections.singletonList("region"),
                                Collections.singletonList("cn")),
                        null,
                        4);

        RowData row = PartitionsSource.toRowData(partitionInfo);

        assertThat(row.getArity()).isEqualTo(3);
        assertThat(row.getLong(0)).isEqualTo(10L);
        assertThat(row.getString(1).toString()).isEqualTo("cn");
        assertThat(row.getInt(2)).isEqualTo(4);
    }

    @Test
    void testLoadsFreshMetadataForEveryReaderExecution() throws Exception {
        AtomicInteger loadCount = new AtomicInteger();
        PartitionsSource source =
                new PartitionsSource(
                        () ->
                                Collections.singletonList(
                                        GenericRowData.of(loadCount.incrementAndGet())),
                        TypeInformation.of(RowData.class));

        assertThat(loadCount).hasValue(0);
        assertThat(readSingleValue(source)).isEqualTo(1);
        assertThat(loadCount).hasValue(1);
        assertThat(readSingleValue(source)).isEqualTo(2);
        assertThat(loadCount).hasValue(2);
    }

    private int readSingleValue(PartitionsSource source) throws Exception {
        try (SourceReader<RowData, SingleSplitMetadataSource.MetadataSplit> reader =
                source.createReader(null)) {
            reader.addSplits(
                    Collections.singletonList(new SingleSplitMetadataSource.MetadataSplit(0)));
            reader.notifyNoMoreSplits();
            TestingReaderOutput<RowData> output = new TestingReaderOutput<>();

            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
            assertThat(output.getEmittedRecords()).hasSize(1);
            return output.getEmittedRecords().get(0).getInt(0);
        }
    }
}
