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

import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Utils for create lake source. */
public class LakeSourceUtils {

    @SuppressWarnings("unchecked")
    public static LakeSource<LakeSplit> createLakeSource(
            TablePath tablePath, Map<String, String> properties) {
        Map<String, String> catalogProperties =
                DataLakeUtils.extractLakeCatalogProperties(Configuration.fromMap(properties));
        Configuration lakeConfig = Configuration.fromMap(catalogProperties);

        String dataLake =
                Configuration.fromMap(properties)
                        .get(ConfigOptions.TABLE_DATALAKE_FORMAT)
                        .toString();
        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromDataLakeFormat(dataLake, null);
        LakeStorage lakeStorage = checkNotNull(lakeStoragePlugin).createLakeStorage(lakeConfig);
        return (LakeSource<LakeSplit>) lakeStorage.createLakeSource(tablePath);
    }
}
