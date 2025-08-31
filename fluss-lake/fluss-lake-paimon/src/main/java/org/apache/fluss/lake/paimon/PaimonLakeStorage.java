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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.paimon.source.PaimonLakeSource;
import org.apache.fluss.lake.paimon.source.PaimonSplit;
import org.apache.fluss.lake.paimon.tiering.PaimonCommittable;
import org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory;
import org.apache.fluss.lake.paimon.tiering.PaimonWriteResult;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TablePath;

/** Paimon implementation of {@link LakeStorage}. */
public class PaimonLakeStorage implements LakeStorage {

    private final Configuration paimonConfig;

    public PaimonLakeStorage(Configuration configuration) {
        this.paimonConfig = configuration;
    }

    @Override
    public LakeTieringFactory<PaimonWriteResult, PaimonCommittable> createLakeTieringFactory() {
        return new PaimonLakeTieringFactory(paimonConfig);
    }

    @Override
    public PaimonLakeCatalog createLakeCatalog() {
        return new PaimonLakeCatalog(paimonConfig);
    }

    @Override
    public LakeSource<PaimonSplit> createLakeSource(TablePath tablePath) {
        return new PaimonLakeSource(paimonConfig, tablePath);
    }
}
