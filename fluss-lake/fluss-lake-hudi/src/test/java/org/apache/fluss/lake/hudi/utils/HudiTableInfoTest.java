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

package org.apache.fluss.lake.hudi.utils;

import org.apache.hudi.common.model.HoodieTableType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.lake.hudi.utils.HudiConversions.FLUSS_BUCKET_AWARE_OPTION;
import static org.apache.fluss.lake.hudi.utils.HudiConversions.FLUSS_BUCKET_KEYS_OPTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HudiTableInfo}. */
class HudiTableInfoTest {

    @Test
    void testExtractPartitionValues() {
        assertThat(
                        HudiTableInfo.extractPartitionValues(
                                "dt=20260608/hr=10", Arrays.asList("dt", "hr")))
                .containsExactly("20260608", "10");
        assertThat(
                        HudiTableInfo.extractPartitionValues(
                                "hr=10/dt=20260608", Arrays.asList("dt", "hr")))
                .containsExactly("20260608", "10");
        assertThat(HudiTableInfo.extractPartitionValues("20260608/10", Arrays.asList("dt", "hr")))
                .containsExactly("20260608", "10");
        assertThat(HudiTableInfo.extractPartitionValues("", Collections.singletonList("dt")))
                .isEmpty();
    }

    @Test
    void testExtractPartitionValuesRejectsMismatchedPartitionPath() {
        assertThatThrownBy(
                        () ->
                                HudiTableInfo.extractPartitionValues(
                                        "dt=20260608", Arrays.asList("dt", "hr")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match partition fields");
        assertThatThrownBy(
                        () ->
                                HudiTableInfo.extractPartitionValues(
                                        "dt=20260608/unknown=10", Arrays.asList("dt", "hr")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown partition field");
        assertThatThrownBy(
                        () ->
                                HudiTableInfo.extractPartitionValues(
                                        "dt=20260608/dt=20260609", Arrays.asList("dt", "hr")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate partition field");
        assertThatThrownBy(
                        () ->
                                HudiTableInfo.extractPartitionValues(
                                        "dt=20260608/10", Arrays.asList("dt", "hr")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mixes hive-style and raw partition segments");
    }

    @Test
    void testResolveBucketAwareUsesExplicitBucketAwareOption() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(FLUSS_BUCKET_AWARE_OPTION, "false");
        tableOptions.put(FLUSS_BUCKET_KEYS_OPTION, "id");

        assertThat(HudiTableInfo.resolveBucketAware(tableOptions, HoodieTableType.MERGE_ON_READ))
                .isFalse();
    }

    @Test
    void testResolveBucketAwareUsesBucketKeysOption() {
        Map<String, String> bucketAwareOptions = new HashMap<>();
        bucketAwareOptions.put(FLUSS_BUCKET_KEYS_OPTION, "id");
        assertThat(
                        HudiTableInfo.resolveBucketAware(
                                bucketAwareOptions, HoodieTableType.COPY_ON_WRITE))
                .isTrue();

        Map<String, String> bucketUnawareOptions = new HashMap<>();
        bucketUnawareOptions.put(FLUSS_BUCKET_KEYS_OPTION, "");
        assertThat(
                        HudiTableInfo.resolveBucketAware(
                                bucketUnawareOptions, HoodieTableType.MERGE_ON_READ))
                .isFalse();
    }

    @Test
    void testResolveBucketAwareDefaultsByTableTypeWhenMetadataIsMissing() {
        assertThat(
                        HudiTableInfo.resolveBucketAware(
                                Collections.emptyMap(), HoodieTableType.MERGE_ON_READ))
                .isTrue();
        assertThat(
                        HudiTableInfo.resolveBucketAware(
                                Collections.emptyMap(), HoodieTableType.COPY_ON_WRITE))
                .isFalse();
    }
}
