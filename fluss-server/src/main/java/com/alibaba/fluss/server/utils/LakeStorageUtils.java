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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DataLakeFormat;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Utils for Fluss lake storage. */
public class LakeStorageUtils {

    @Nullable
    public static Map<String, String> generateDefaultTableLakeOptions(Configuration clusterConf) {
        Optional<DataLakeFormat> optDataLakeFormat =
                clusterConf.getOptional(ConfigOptions.DATALAKE_FORMAT);
        if (!optDataLakeFormat.isPresent()) {
            return null;
        }
        Map<String, String> datalakeProperties = new HashMap<>();
        String dataLakePrefix = "datalake." + optDataLakeFormat.get() + ".";
        for (Map.Entry<String, String> configurationEntry : clusterConf.toMap().entrySet()) {
            if (configurationEntry.getKey().startsWith(dataLakePrefix)) {
                datalakeProperties.put(
                        "table." + configurationEntry.getKey(), configurationEntry.getValue());
            }
        }
        return datalakeProperties;
    }
}
