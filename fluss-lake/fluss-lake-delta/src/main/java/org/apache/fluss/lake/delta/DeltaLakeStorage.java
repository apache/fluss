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

package org.apache.fluss.lake.delta;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.delta.tiering.DeltaCommittable;
import org.apache.fluss.lake.delta.tiering.DeltaLakeTieringFactory;
import org.apache.fluss.lake.delta.tiering.DeltaWriteResult;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TablePath;

/** Delta Lake implementation of {@link LakeStorage}. */
public class DeltaLakeStorage implements LakeStorage {

    private final Configuration config;

    public DeltaLakeStorage(Configuration configuration) {
        this.config = configuration;
    }

    @Override
    public LakeTieringFactory<DeltaWriteResult, DeltaCommittable> createLakeTieringFactory() {
        return new DeltaLakeTieringFactory(config);
    }

    @Override
    public DeltaLakeCatalog createLakeCatalog() {
        return new DeltaLakeCatalog(config);
    }

    @Override
    public LakeSource<?> createLakeSource(TablePath tablePath) {
        // Union read is not supported yet, read the Delta table with a Delta reader directly.
        throw new UnsupportedOperationException("Not implemented");
    }
}
