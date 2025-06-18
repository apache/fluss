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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DataLakeFormat;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import static com.alibaba.fluss.utils.PropertiesUtils.asPrefixedMap;
import static com.alibaba.fluss.utils.PropertiesUtils.extractAndRemovePrefix;
import static com.alibaba.fluss.utils.PropertiesUtils.extractPrefix;

/** Utils for Fluss lake storage. */
public class LakeStorageUtils {

    @Nullable
    public static Map<String, String> generateDefaultTableLakeOptions(Configuration clusterConf) {
        Optional<DataLakeFormat> optDataLakeFormat =
                clusterConf.getOptional(ConfigOptions.DATALAKE_FORMAT);
        if (!optDataLakeFormat.isPresent()) {
            return null;
        }

        String dataLakePrefix = "datalake." + optDataLakeFormat.get() + ".";
        return asPrefixedMap(extractPrefix(clusterConf.toMap(), dataLakePrefix), "table.");
    }

    /**
     * Extract the datalake properties configured from the configuration. Return null if datalake is
     * not configured.
     */
    @Nullable
    public static Map<String, String> extractLakeProperties(Configuration configuration) {
        DataLakeFormat datalakeFormat = configuration.get(ConfigOptions.DATALAKE_FORMAT);
        if (datalakeFormat == null) {
            return null;
        }

        Map<String, String> configMap = configuration.toMap();
        String datalakePrefix = "datalake." + datalakeFormat + ".";
        return extractAndRemovePrefix(configMap, datalakePrefix);
    }
}
