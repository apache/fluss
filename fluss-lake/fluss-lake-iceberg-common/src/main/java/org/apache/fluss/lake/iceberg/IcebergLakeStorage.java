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

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.source.IcebergLakeSource;
import org.apache.fluss.lake.iceberg.source.IcebergSplit;
import org.apache.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TablePath;

/** Iceberg implementation of {@link LakeStorage}. */
public class IcebergLakeStorage implements LakeStorage {

    private final Configuration icebergConfig;

    public IcebergLakeStorage(Configuration configuration) {
        this.icebergConfig = configuration;
    }

    @Override
    public LakeTieringFactory<?, ?> createLakeTieringFactory() {
        return new IcebergLakeTieringFactory(icebergConfig);
    }

    @Override
    public IcebergLakeCatalog createLakeCatalog() {
        return new IcebergLakeCatalog(icebergConfig);
    }

    @Override
    public LakeSource<IcebergSplit> createLakeSource(TablePath tablePath) {
        return new IcebergLakeSource(icebergConfig, tablePath);
    }
}
