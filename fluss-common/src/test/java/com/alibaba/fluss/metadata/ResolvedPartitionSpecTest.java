/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.metadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.metadata.ResolvedPartitionSpec.fromPartitionSpec;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ResolvedPartitionSpec}. */
public class ResolvedPartitionSpecTest {

    @Test
    void testResolvedPartitionSpec() {
        ResolvedPartitionSpec resolvedPartitionSpec =
                fromPartitionSpec(
                        Collections.singletonList("a"),
                        new PartitionSpec(Collections.singletonMap("a", "1")));
        assertThat(resolvedPartitionSpec.getPartitionName()).isEqualTo("1");
        assertThat(resolvedPartitionSpec.getPartitionQualifiedName()).isEqualTo("a=1");
        assertThat(resolvedPartitionSpec.getPartitionKeys())
                .isEqualTo(Collections.singletonList("a"));
        assertThat(resolvedPartitionSpec.getPartitionValues())
                .isEqualTo(Collections.singletonList("1"));

        Map<String, String> partitionKeyValue = new HashMap<>();
        partitionKeyValue.put("a", "1");
        partitionKeyValue.put("b", "2");
        resolvedPartitionSpec =
                fromPartitionSpec(Arrays.asList("a", "b"), new PartitionSpec(partitionKeyValue));
        assertThat(resolvedPartitionSpec.getPartitionName()).isEqualTo("1$2");
        assertThat(resolvedPartitionSpec.getPartitionQualifiedName()).isEqualTo("a=1/b=2");
        assertThat(resolvedPartitionSpec.getPartitionKeys()).isEqualTo(Arrays.asList("a", "b"));
        assertThat(resolvedPartitionSpec.getPartitionValues()).isEqualTo(Arrays.asList("1", "2"));
    }
}
