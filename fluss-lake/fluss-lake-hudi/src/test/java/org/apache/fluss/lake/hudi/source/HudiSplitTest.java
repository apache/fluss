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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HudiSplit}. */
class HudiSplitTest {

    private static final String PARTITION_PATH = "dt=20260608";
    private static final String FILE_ID = "00000000-0000-0000-0000-000000000001";
    private static final String INSTANT_TIME = "20260608010101000";

    @Test
    void testEqualsUsesStableFileSliceFields() {
        HudiSplit first = new HudiSplit(fileSlice("/tmp/base-1.parquet", 1), 1, partition());
        HudiSplit same = new HudiSplit(fileSlice("/tmp/base-1.parquet", 1), 1, partition());
        HudiSplit differentBaseFile =
                new HudiSplit(fileSlice("/tmp/base-2.parquet", 1), 1, partition());
        HudiSplit differentLogFile =
                new HudiSplit(fileSlice("/tmp/base-1.parquet", 2), 1, partition());

        assertThat(first).isEqualTo(same);
        assertThat(first).hasSameHashCodeAs(same);
        assertThat(first).isNotEqualTo(differentBaseFile);
        assertThat(first).isNotEqualTo(differentLogFile);
    }

    private static FileSlice fileSlice(String baseFilePath, int logVersion) {
        HoodieFileGroupId fileGroupId = new HoodieFileGroupId(PARTITION_PATH, FILE_ID);
        FileSlice fileSlice =
                new FileSlice(
                        fileGroupId,
                        INSTANT_TIME,
                        new HoodieBaseFile(baseFilePath),
                        Collections.<HoodieLogFile>emptyList());
        fileSlice.addLogFile(
                new HoodieLogFile(
                        "/tmp/"
                                + FSUtils.makeLogFileName(
                                        FILE_ID,
                                        HoodieLogFile.DELTA_EXTENSION,
                                        INSTANT_TIME,
                                        logVersion,
                                        FSUtils.makeWriteToken(0, 0, 1))));
        return fileSlice;
    }

    private static List<String> partition() {
        return Collections.singletonList("20260608");
    }
}
