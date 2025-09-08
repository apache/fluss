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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.metadata.DatabaseDescriptor;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * The registration information of database in {@link ZkData.DatabaseZNode}. It is used to store the
 * database information in zookeeper. Basically, it contains the same information with {@link
 * org.apache.fluss.metadata.DatabaseInfo}.
 *
 * @see DatabaseRegistrationJsonSerde for json serialization and deserialization.
 */
public class DatabaseRegistration {
    public final @Nullable String comment;
    public final Map<String, String> customProperties;
    public final long createdTime;
    public final long modifiedTime;

    public DatabaseRegistration(
            @Nullable String comment,
            Map<String, String> customProperties,
            long createdTime,
            long modifiedTime) {
        this.comment = comment;
        this.customProperties = customProperties;
        this.createdTime = createdTime;
        this.modifiedTime = modifiedTime;
    }

    public DatabaseDescriptor toDatabaseDescriptor() {
        DatabaseDescriptor.Builder builder = DatabaseDescriptor.builder().comment(comment);
        customProperties.forEach(builder::customProperty);
        return builder.build();
    }

    public static DatabaseRegistration of(DatabaseDescriptor databaseDescriptor) {
        final long currentMillis = System.currentTimeMillis();
        return new DatabaseRegistration(
                databaseDescriptor.getComment().orElse(null),
                databaseDescriptor.getCustomProperties(),
                currentMillis,
                currentMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseRegistration that = (DatabaseRegistration) o;
        return createdTime == that.createdTime
                && Objects.equals(comment, that.comment)
                && Objects.equals(customProperties, that.customProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(comment, customProperties, createdTime);
    }

    @Override
    public String toString() {
        return "DatabaseRegistration{"
                + "comment='"
                + comment
                + '\''
                + ", customProperties="
                + customProperties
                + ", createdTime="
                + createdTime
                + ", modifiedTime="
                + modifiedTime
                + '}';
    }
}
