/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * Information of a database metadata, includes {@link DatabaseDescriptor}.
 *
 * @since 0.6
 */
@PublicEvolving
public class DatabaseInfo {
    private final String databaseName;
    private final DatabaseDescriptor databaseDescriptor;
    private final long createdTime;
    private final long modifiedTime;

    public DatabaseInfo(
            String databaseName,
            DatabaseDescriptor databaseDescriptor,
            long createdTime,
            long modifiedTime) {
        this.databaseName = databaseName;
        this.databaseDescriptor = databaseDescriptor;
        this.createdTime = createdTime;
        this.modifiedTime = modifiedTime;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DatabaseDescriptor getDatabaseDescriptor() {
        return databaseDescriptor;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }
}
