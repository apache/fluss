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

package org.apache.fluss.lake.hudi;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TablePath;

/** Hudi Implementation of {@link LakeStorage}. */
public class HudiLakeStorage implements LakeStorage {

    private final Configuration hudiConfig;

    public HudiLakeStorage(Configuration configuration) {
        this.hudiConfig = configuration;
    }

    @Override
    public LakeTieringFactory<?, ?> createLakeTieringFactory() {
        throw new UnsupportedOperationException(
                "HudiLakeStorage is currently a scaffold and does not support creating a "
                        + "LakeTieringFactory yet. Verify that Hudi lake storage was selected "
                        + "intentionally and that the required Hudi support/module is available.");
    }

    @Override
    public LakeCatalog createLakeCatalog() {
        throw new UnsupportedOperationException(
                "HudiLakeStorage is currently a scaffold and does not support creating a "
                        + "LakeCatalog yet. Verify that Hudi lake storage was selected "
                        + "intentionally and that the required Hudi support/module is available.");
    }

    @Override
    public LakeSource<?> createLakeSource(TablePath tablePath) {
        throw new UnsupportedOperationException(
                "HudiLakeStorage is currently a scaffold and does not support creating a "
                        + "LakeSource for table '"
                        + tablePath
                        + "' yet. Verify that Hudi lake storage was selected intentionally "
                        + "and that the required Hudi support/module is available.");
    }
}
