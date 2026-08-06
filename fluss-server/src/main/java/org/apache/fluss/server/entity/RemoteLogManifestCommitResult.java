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

package org.apache.fluss.server.entity;

/** Result of publishing a remote log manifest handle. */
public enum RemoteLogManifestCommitResult {
    COMMITTED(0),
    CONFLICT(1),
    FENCED(2),
    INVALID_MANIFEST(3);

    private final int code;

    RemoteLogManifestCommitResult(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static RemoteLogManifestCommitResult fromCode(int code) {
        for (RemoteLogManifestCommitResult result : values()) {
            if (result.code == code) {
                return result;
            }
        }
        throw new IllegalArgumentException("Unknown remote log manifest commit result: " + code);
    }
}
