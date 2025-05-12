/*
 *  Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.transforms;

import com.alibaba.fluss.row.TestInternalRowGenerator;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.utils.transform.Transform;
import com.alibaba.fluss.utils.transform.Transforms;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class TestDateTransforms {
    @Test
    public void testYearTransform() {
        IndexedRow data = TestInternalRowGenerator.genIndexedRowForAllType();
        TimestampNtz timeStamp = data.getTimestampNtz(15, 1);

        Transform<TimestampNtz, Integer> years = Transforms.years();
        assertThat(years.apply(timeStamp)).isEqualTo(LocalDateTime.now().getYear());
    }
}
