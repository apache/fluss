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

import org.apache.fluss.metadata.BucketInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BucketsFlinkTableSource}. */
class BucketsFlinkTableSourceTest {

    @Test
    void testConvertBucketInfoToInternalRow() {
        BucketInfo bucketInfo =
                new BucketInfo(
                        TablePath.of("test_db", "test_table"),
                        10L,
                        null,
                        null,
                        0,
                        null,
                        null,
                        null,
                        Arrays.asList(1, 2, 3),
                        Collections.emptyList());

        RowData row = BucketsFlinkTableSource.toRowData(bucketInfo);

        assertThat(row.getArity()).isEqualTo(10);
        assertThat(row.getString(0).toString()).isEqualTo("test_db.test_table");
        assertThat(row.getLong(1)).isEqualTo(10L);
        assertThat(row.isNullAt(2)).isTrue();
        assertThat(row.isNullAt(3)).isTrue();
        assertThat(row.getInt(4)).isZero();
        assertThat(row.isNullAt(5)).isTrue();
        assertThat(row.isNullAt(6)).isTrue();
        assertThat(row.isNullAt(7)).isTrue();
        assertThat(toInts(row.getArray(8))).containsExactly(1, 2, 3);
        assertThat(toInts(row.getArray(9))).isEmpty();
    }

    private static int[] toInts(ArrayData arrayData) {
        int[] values = new int[arrayData.size()];
        for (int i = 0; i < arrayData.size(); i++) {
            values[i] = arrayData.getInt(i);
        }
        return values;
    }
}
