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

package com.alibaba.fluss.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Updating properties for updating properties. */
public class UpdateProperties {

    private final List<String> resetProperties;
    private final Map<String, String> setProperties;

    private UpdateProperties(List<String> resetProperties, Map<String, String> setProperties) {
        this.resetProperties = resetProperties;
        this.setProperties = setProperties;
    }

    /** Creates a builder for building update property data. */
    public static Builder builder() {
        return new Builder();
    }

    public List<String> getResetProperties() {
        return resetProperties;
    }

    public Map<String, String> getSetProperties() {
        return setProperties;
    }

    /** Builder for {@link TableDescriptor}. */
    public static class Builder {
        private final List<String> resetProperties;
        private final Map<String, String> setProperties;

        protected Builder() {
            this.resetProperties = new ArrayList<>();
            this.setProperties = new HashMap<>();
        }

        public Builder resetProperty(String property) {
            this.resetProperties.add(property);
            return this;
        }

        public Builder setProperty(String property, String value) {
            this.setProperties.put(property, value);
            return this;
        }

        public UpdateProperties build() {
            return new UpdateProperties(resetProperties, setProperties);
        }
    }
}
