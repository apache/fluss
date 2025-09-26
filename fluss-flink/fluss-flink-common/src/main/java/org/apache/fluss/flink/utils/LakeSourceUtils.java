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
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TablePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Utils for create lake source. */
public class LakeSourceUtils {

    public static final Logger LOG = LoggerFactory.getLogger(LakeSourceUtils.class);

    /**
     * Return the lake source of the given table. Return null when the lake storage doesn't support
     * create lake source.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static LakeSource<LakeSplit> createLakeSource(
            TablePath tablePath, Map<String, String> properties) {
        Map<String, String> catalogProperties =
                DataLakeUtils.extractLakeCatalogProperties(Configuration.fromMap(properties));
        Configuration lakeConfig = Configuration.fromMap(catalogProperties);

        String dataLake =
                Configuration.fromMap(properties)
                        .get(ConfigOptions.TABLE_DATALAKE_FORMAT)
                        .toString();
        LakeStoragePlugin lakeStoragePlugin;
        try {
            lakeStoragePlugin = LakeStoragePluginSetUp.fromDataLakeFormat(dataLake, null);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException(
                    String.format(
                            "No LakeStoragePlugin available for data lake format: %s. "
                                    + "To resolve this, ensure fluss-lake-%s.jar is in the classpath.",
                            dataLake, dataLake.toLowerCase()));
        }
        LakeStorage lakeStorage = checkNotNull(lakeStoragePlugin).createLakeStorage(lakeConfig);
        try {
            return (LakeSource<LakeSplit>) lakeStorage.createLakeSource(tablePath);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Table using '%s' data lake format cannot be used as historical data in Fluss.",
                            dataLake));
        }
    }
}
