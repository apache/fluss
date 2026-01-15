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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.metadata.DataLakeFormat;

/** Implementation of {@link LakeStoragePlugin} for values lake. */
public class TestingValuesLakeStoragePlugin implements LakeStoragePlugin {

    // Testing/mock implementation for values lake storage that reuses the Lance data lake format
    // identifier for compatibility with existing Fluss lake storage infrastructure.
    private static final String IDENTIFIER = DataLakeFormat.LANCE.toString();

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public LakeStorage createLakeStorage(Configuration configuration) {
        return new TestingValuesLakeStorage();
    }
}
