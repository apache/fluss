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
import com.alibaba.fluss.utils.Preconditions;
import com.alibaba.fluss.utils.json.DatabaseDescriptorJsonSerde;
import com.alibaba.fluss.utils.json.JsonSerdeUtils;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represents the metadata of a database in Fluss.
 *
 * <p>It contains all characteristics that can be expressed in a SQL {@code CREATE Database}
 * statement.
 *
 * @since 0.6
 */
public class DatabaseDescriptor {
    private final Map<String, String> customProperties;
    private final String comment;

    private DatabaseDescriptor(Map<String, String> customProperties, @Nullable String comment) {
        this.customProperties = customProperties;
        this.comment = comment;
    }

    public Map<String, String> getCustomProperties() {
        return customProperties;
    }

    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    /**
     * Serialize the table descriptor to a JSON byte array.
     *
     * @see DatabaseDescriptorJsonSerde
     */
    public byte[] toJsonBytes() {
        return JsonSerdeUtils.writeValueAsBytes(this, DatabaseDescriptorJsonSerde.INSTANCE);
    }

    /**
     * Deserialize from JSON byte array to an instance of {@link DatabaseDescriptor}.
     *
     * @see DatabaseDescriptor
     */
    public static DatabaseDescriptor fromJsonBytes(byte[] json) {
        return JsonSerdeUtils.readValue(json, DatabaseDescriptorJsonSerde.INSTANCE);
    }

    /** Creates a builder for building database descriptor. */
    public static Builder builder() {
        return new Builder();
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link DatabaseDescriptor}. */
    @PublicEvolving
    public static class Builder {

        private final Map<String, String> customProperties;
        private @Nullable String comment;

        protected Builder() {
            this.customProperties = new HashMap<>();
        }

        protected Builder(DatabaseDescriptor descriptor) {
            this.customProperties = new HashMap<>(descriptor.getCustomProperties());
            this.comment = descriptor.getComment().orElse(null);
        }

        /**
         * Sets custom properties on the database.
         *
         * <p>Custom properties are not understood by Fluss, but are stored as part of the database
         * 's metadata. This provides a mechanism to persist user-defined properties with this
         * database for users.
         */
        public Builder customProperty(String key, String value) {
            Preconditions.checkNotNull(key, "Key must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            customProperties.put(key, value);
            return this;
        }

        /**
         * Sets custom properties on the database.
         *
         * <p>Custom properties are not understood by Fluss, but are stored as part of the database
         * 's metadata. This provides a mechanism to persist user-defined properties with this
         * database for users.
         */
        public Builder customProperties(Map<String, String> properties) {
            Preconditions.checkNotNull(properties, "customProperties must not be null.");
            this.customProperties.putAll(properties);
            return this;
        }

        /** Define the comment for this table. */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /** Returns an immutable instance of {@link DatabaseDescriptor}. */
        public DatabaseDescriptor build() {
            return new DatabaseDescriptor(customProperties, comment);
        }
    }
}
