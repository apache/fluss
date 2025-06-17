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

package com.alibaba.fluss.server.kv.snapshot;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * This class represents a key that uniquely identifies (on a logical level) kv file handles for
 * registration in the {@link SharedKvFileRegistry}. Two files which should logically be the same
 * should have the same {@link SharedKvFileRegistryKey}. The meaning of logical equivalence is up to
 * the application.
 */
public class SharedKvFileRegistryKey implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Uses a String as internal representation. */
    private final String keyString;

    /** Protected constructor to enforce that subclassing. */
    protected SharedKvFileRegistryKey(String keyString) {
        this.keyString = checkNotNull(keyString);
    }

    /** Create a unique key based on physical id. */
    public static SharedKvFileRegistryKey fromKvFileHandle(KvFileHandle handle) {
        String keyString = handle.getFilePath();
        // key strings tend to be longer, so we use the MD5 of the key string to save memory
        return new SharedKvFileRegistryKey(
                UUID.nameUUIDFromBytes(keyString.getBytes(StandardCharsets.UTF_8)).toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SharedKvFileRegistryKey that = (SharedKvFileRegistryKey) o;
        return keyString.equals(that.keyString);
    }

    @Override
    public int hashCode() {
        return keyString.hashCode();
    }

    @Override
    public String toString() {
        return keyString;
    }
}
