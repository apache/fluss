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

package com.alibaba.fluss.lake.lance;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.lakestorage.LakeStorage;
import com.alibaba.fluss.lake.lance.tiering.LanceCommittable;
import com.alibaba.fluss.lake.lance.tiering.LanceWriteResult;
import com.alibaba.fluss.lake.writer.LakeTieringFactory;

/** Lance implementation of {@link LakeStorage}. */
public class LanceLakeStorage implements LakeStorage {
    private final Configuration config;

    public LanceLakeStorage(Configuration configuration) {
        this.config = configuration;
    }

    @Override
    public LakeTieringFactory<LanceWriteResult, LanceCommittable> createLakeTieringFactory() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public LanceLakeCatalog createLakeCatalog() {
        return new LanceLakeCatalog(config);
    }
}
