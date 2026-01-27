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

package org.apache.fluss.fs.azure.token;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.apache.fluss.fs.azure.AzureFileSystemOptions.PROVIDER_CONFIG_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AzureDelegationTokenReceiver}. */
class AzureDelegationTokenReceiverTest {

    private static final String PROVIDER_CLASS_NAME = "TestProvider";

    @BeforeEach
    void beforeEach() {
        AzureDelegationTokenReceiver.additionInfos = new HashMap<>();
    }

    @AfterEach
    void afterEach() {
        AzureDelegationTokenReceiver.additionInfos = null;
    }

    @Test
    void updateHadoopConfigShouldFailOnEmptyAdditionalInfo() {
        AzureDelegationTokenReceiver.additionInfos = null;
        org.apache.hadoop.conf.Configuration hadoopConfiguration =
                new org.apache.hadoop.conf.Configuration();
        hadoopConfiguration.set(PROVIDER_CONFIG_NAME.key(), "");
        assertThatThrownBy(
                        () -> AzureDelegationTokenReceiver.updateHadoopConfig(hadoopConfiguration))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void updateHadoopConfigShouldSetProviderWhenEmpty() {
        org.apache.hadoop.conf.Configuration hadoopConfiguration =
                new org.apache.hadoop.conf.Configuration();
        hadoopConfiguration.set(PROVIDER_CONFIG_NAME.key(), "");
        AzureDelegationTokenReceiver.updateHadoopConfig(hadoopConfiguration);
        assertThat(hadoopConfiguration.get(PROVIDER_CONFIG_NAME.key()))
                .isEqualTo(DynamicTemporaryAzureCredentialsProvider.NAME);
    }

    @Test
    void updateHadoopConfigShouldPrependProviderWhenNotEmpty() {
        org.apache.hadoop.conf.Configuration hadoopConfiguration =
                new org.apache.hadoop.conf.Configuration();
        hadoopConfiguration.set(PROVIDER_CONFIG_NAME.key(), PROVIDER_CLASS_NAME);
        AzureDelegationTokenReceiver.updateHadoopConfig(hadoopConfiguration);
        String[] providers = hadoopConfiguration.get(PROVIDER_CONFIG_NAME.key()).split(",");
        assertThat(providers.length).isEqualTo(2);
        assertThat(providers[0]).isEqualTo(DynamicTemporaryAzureCredentialsProvider.NAME);
        assertThat(providers[1]).isEqualTo(PROVIDER_CLASS_NAME);
    }

    @Test
    void updateHadoopConfigShouldNotAddProviderWhenAlreadyExists() {
        org.apache.hadoop.conf.Configuration hadoopConfiguration =
                new org.apache.hadoop.conf.Configuration();
        hadoopConfiguration.set(
                PROVIDER_CONFIG_NAME.key(), DynamicTemporaryAzureCredentialsProvider.NAME);
        AzureDelegationTokenReceiver.updateHadoopConfig(hadoopConfiguration);
        assertThat(hadoopConfiguration.get(PROVIDER_CONFIG_NAME.key()))
                .isEqualTo(DynamicTemporaryAzureCredentialsProvider.NAME);
    }
}
