/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.LakeStorageInfo;
import com.alibaba.fluss.utils.MapUtils;

import java.util.Map;

/** Utils for Fluss lake storage. */
public class LakeStorageUtils {

    private static final String SUPPORTED_DATALAKE_STORAGE = "paimon";

    private static final String CATALOG_PREFIX = "catalog.";

    public static LakeStorageInfo getLakeStorageInfo(Configuration configuration) {
        String datalakeStorage = configuration.get(ConfigOptions.LAKEHOUSE_STORAGE);
        if (datalakeStorage == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "The lakehouse storage is not set, please set it by %s",
                            ConfigOptions.LAKEHOUSE_STORAGE.key()));
        }

        if (!datalakeStorage.equalsIgnoreCase(SUPPORTED_DATALAKE_STORAGE)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The lakehouse storage %s "
                                    + " is not supported. Only %s is supported.",
                            datalakeStorage, SUPPORTED_DATALAKE_STORAGE));
        }

        // currently, extract catalog config
        String catalogPrefix = datalakeStorage + "." + CATALOG_PREFIX;
        Map<String, String> flussConfig = configuration.toMap();
        Map<String, String> catalogConfig =
                MapUtils.extractByKeyPrefix(flussConfig, catalogPrefix, true);
        return new LakeStorageInfo(datalakeStorage, catalogConfig);
    }
}
