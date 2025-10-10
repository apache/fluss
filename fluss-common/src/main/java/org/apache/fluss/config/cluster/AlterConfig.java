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

package org.apache.fluss.config.cluster;

import javax.annotation.Nullable;

import java.util.Objects;

/** Configuration change operation for cluster. */
public class AlterConfig {

    private final String key;
    @Nullable private final String value;
    private final AlterConfigOpType opType;

    public AlterConfig(String key, @Nullable String value, AlterConfigOpType opType) {
        this.key = key;
        this.value = value;
        this.opType = opType;
    }

    public String key() {
        return key;
    }

    @Nullable
    public String value() {
        return value;
    }

    public AlterConfigOpType opType() {
        return opType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AlterConfig that = (AlterConfig) o;
        return opType == that.opType
                && Objects.equals(key, that.key)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, key, value);
    }

    @Override
    public String toString() {
        return "AlterConfigOp{"
                + "name='"
                + key
                + '\''
                + ", value='"
                + value
                + '\''
                + ", opType="
                + opType
                + '}';
    }
}
