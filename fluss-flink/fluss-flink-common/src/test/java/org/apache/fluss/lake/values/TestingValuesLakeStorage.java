/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.values;

import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.values.tiering.TestingValuesLakeTieringFactory;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TablePath;

/** Implementation of {@link LakeStorage} for values lake. */
public class TestingValuesLakeStorage implements LakeStorage {
    @Override
    public LakeTieringFactory<?, ?> createLakeTieringFactory() {
        return new TestingValuesLakeTieringFactory();
    }

    @Override
    public LakeCatalog createLakeCatalog() {
        return new TestingValuesLakeCatalog();
    }

    @Override
    public LakeSource<?> createLakeSource(TablePath tablePath) {
        throw new UnsupportedOperationException("Not impl.");
    }
}
