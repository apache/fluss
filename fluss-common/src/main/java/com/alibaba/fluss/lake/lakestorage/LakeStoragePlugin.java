/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.lake.lakestorage;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.plugin.Plugin;

/**
 * A Plugin to create instances of {@link LakeStorage}.
 *
 * @since 0.7
 */
@PublicEvolving
public interface LakeStoragePlugin extends Plugin {

    /**
     * Returns a unique identifier among {@link LakeStoragePlugin} implementations.
     *
     * @return the identifier
     */
    String identifier();

    /**
     * Creates a new instance of {@link LakeStorage}.
     *
     * @param configuration the configuration for LakeStorage
     * @return the lake storage instance
     */
    LakeStorage createLakeStorage(final Configuration configuration);
}
