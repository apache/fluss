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

package com.alibaba.fluss.lakehouse.lakestorage;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;

/**
 * The LakeStorage interface defines how to implement lakehouse storage system such as Paimon and
 * Iceberg. It provides a method to create a lake tiering factory.
 *
 * @since 0.7
 */
@PublicEvolving
public interface LakeStorage {

    /**
     * Creates a lake tiering factory to create lake writers and committers.
     *
     * @return the lake tiering factory
     */
    LakeTieringFactory<?, ?> createLakeTieringFactory();

    /** Create lake catalog. */
    LakeCatalog createLakeCatalog();
}
