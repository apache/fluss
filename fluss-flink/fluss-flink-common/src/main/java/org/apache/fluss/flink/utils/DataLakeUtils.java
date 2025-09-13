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

package org.apache.fluss.flink.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;

import org.apache.flink.configuration.ReadableConfig;

import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.config.ConfigOptions.TABLE_DATALAKE_FORMAT;
import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkOption;
import static org.apache.fluss.utils.PropertiesUtils.extractAndRemovePrefix;

/** Utility class for accessing data lake related configurations. */
public class DataLakeUtils {

    public static Map<String, String> extractLakeCatalogProperties(Configuration tableOptions) {
        DataLakeFormat datalakeFormat = tableOptions.get(ConfigOptions.TABLE_DATALAKE_FORMAT);
        if (datalakeFormat == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "The datalake format is not set, please set it by %s",
                            ConfigOptions.TABLE_DATALAKE_FORMAT.key()));
        }

        // currently, extract datalake catalog config
        String dataLakePrefix = "table.datalake." + datalakeFormat + ".";
        return extractAndRemovePrefix(tableOptions.toMap(), dataLakePrefix);
    }

    public static Optional<DataLakeFormat> getDatalakeFormat(ReadableConfig tableOptions) {
        Optional<DataLakeFormat> tableOptional =
                tableOptions.getOptional(toFlinkOption(TABLE_DATALAKE_FORMAT));
        if (tableOptional.isPresent()) {
            return tableOptional;
        }
        return Optional.empty();
    }
}
