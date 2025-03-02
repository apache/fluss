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

package com.alibaba.fluss.connector.flink.utils;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.lakehouse.LakeStorageInfo;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.TableInfo;

import java.util.HashMap;
import java.util.Map;

/** Utility class for {@link LakeStorageInfo}. */
public class LakeStorageInfoUtils {

    private static final String TABLE_DATALAKE_PAIMON_PREFIX = "table.datalake.paimon.";

    public static LakeStorageInfo getLakeStorageInfo(TableInfo tableInfo) {
        DataLakeFormat datalakeFormat =
                tableInfo.getProperties().get(ConfigOptions.TABLE_DATALAKE_FORMAT);
        if (datalakeFormat == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "The lakehouse storage is not set, please set it by %s",
                            ConfigOptions.TABLE_DATALAKE_FORMAT.key()));
        }

        if (datalakeFormat != DataLakeFormat.PAIMON) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The lakehouse storage %s "
                                    + " is not supported. Only %s is supported.",
                            datalakeFormat, DataLakeFormat.PAIMON));
        }

        // currently, extract catalog config
        Map<String, String> datalakeConfig = new HashMap<>();
        Map<String, String> flussConfig = tableInfo.getCustomProperties().toMap();
        for (Map.Entry<String, String> configEntry : flussConfig.entrySet()) {
            String configKey = configEntry.getKey();
            String configValue = configEntry.getValue();
            if (configKey.startsWith(TABLE_DATALAKE_PAIMON_PREFIX)) {
                datalakeConfig.put(
                        configKey.substring(TABLE_DATALAKE_PAIMON_PREFIX.length()), configValue);
            }
        }
        return new LakeStorageInfo(datalakeFormat.toString(), datalakeConfig);
    }
}
