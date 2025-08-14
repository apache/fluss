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

package com.alibaba.fluss.lake.iceberg;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.iceberg.tiering.IcebergLakeTieringFactory;
import com.alibaba.fluss.lake.lakestorage.LakeStorage;
import com.alibaba.fluss.lake.source.LakeSource;
import com.alibaba.fluss.lake.writer.LakeTieringFactory;
import com.alibaba.fluss.metadata.TablePath;

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
    public LakeSource<?> createLakeSource(TablePath tablePath) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
