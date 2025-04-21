/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.catalog;

import com.alibaba.fluss.flink.FlinkConnectorOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.alibaba.fluss.config.FlussConfigUtils.CLIENT_SECURITY_PREFIX;
import static com.alibaba.fluss.utils.PropertiesUtils.extractPrefix;

/** Factory for {@link FlinkCatalog}. */
public class FlinkCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "fluss";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(FlinkConnectorOptions.BOOTSTRAP_SERVERS);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.singleton(FlinkCatalogOptions.DEFAULT_DATABASE);
    }

    @Override
    public FlinkCatalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(CLIENT_SECURITY_PREFIX);
        Map<String, String> options = context.getOptions();
        Map<String, String> securityConfigs = extractPrefix(options, CLIENT_SECURITY_PREFIX);

        return new FlinkCatalog(
                context.getName(),
                helper.getOptions().get(FlinkCatalogOptions.DEFAULT_DATABASE),
                helper.getOptions().get(FlinkConnectorOptions.BOOTSTRAP_SERVERS),
                context.getClassLoader(),
                securityConfigs);
    }
}
