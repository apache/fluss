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

package com.alibaba.fluss.types;

import com.alibaba.fluss.annotation.PublicStable;
import com.alibaba.fluss.utils.EncodingUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Defines the field of a row type.
 *
 * @since 0.1
 */
@PublicStable
public class DataField implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_FORMAT_WITH_DESCRIPTION = "%s %s '%s'";

    public static final String FIELD_FORMAT_NO_DESCRIPTION = "%s %s";

    private final String name;

    private final DataType type;

    private final @Nullable String description;

    public DataField(String name, DataType type, @Nullable String description) {
        this.name = checkNotNull(name, "Field name must not be null.");
        this.type = checkNotNull(type, "Field type must not be null.");
        this.description = description;
    }

    public DataField(String name, DataType type) {
        this(name, type, null);
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public Optional<String> getDescription() {
        return Optional.ofNullable(description);
    }

    public DataField copy() {
        return new DataField(name, type.copy(), description);
    }

    public String asSummaryString() {
        return formatString(type.asSummaryString(), true);
    }

    public String asSerializableString() {
        return formatString(type.asSerializableString(), false);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataField rowField = (DataField) o;
        return name.equals(rowField.name)
                && type.equals(rowField.type)
                && Objects.equals(description, rowField.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, description);
    }

    private String formatString(String typeString, boolean excludeDescription) {
        if (description == null) {
            return String.format(
                    FIELD_FORMAT_NO_DESCRIPTION, EncodingUtils.escapeIdentifier(name), typeString);
        } else if (excludeDescription) {
            return String.format(
                    FIELD_FORMAT_WITH_DESCRIPTION,
                    EncodingUtils.escapeIdentifier(name),
                    typeString,
                    "...");
        } else {
            return String.format(
                    FIELD_FORMAT_WITH_DESCRIPTION,
                    EncodingUtils.escapeIdentifier(name),
                    typeString,
                    EncodingUtils.escapeSingleQuotes(description));
        }
    }
}
