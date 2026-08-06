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

/** Expected existence state of the authoritative remote log manifest handle. */
public enum RemoteLogManifestExpectedHandleState {
    ABSENT(0),
    PRESENT(1);

    private final int code;

    RemoteLogManifestExpectedHandleState(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static RemoteLogManifestExpectedHandleState fromCode(int code) {
        for (RemoteLogManifestExpectedHandleState state : values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown expected handle state: " + code);
    }
}
