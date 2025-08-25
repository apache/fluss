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

package com.alibaba.fluss.flink.utils;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.lakestorage.LakeStorage;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePlugin;
import com.alibaba.fluss.lake.lakestorage.LakeStoragePluginSetUp;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.source.LakeSplit;
import com.alibaba.fluss.metadata.TablePath;

import java.util.Map;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

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
