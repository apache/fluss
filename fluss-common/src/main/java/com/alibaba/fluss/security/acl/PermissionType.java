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

package com.alibaba.fluss.security.acl;

/**
 * Enumeration representing permission types used in ACL.
 *
 * @since 0.7
 */
public enum PermissionType {
    /** In a filter, matches any PermissionType. */
    ANY((byte) 1),

    /**
     * Permission type indicating allowed access. Grants explicit permission for specified
     * operations on resources.
     */
    ALLOW((byte) 2);

    // todo: Will introduce DENY type in the future.

    private final byte code;

    PermissionType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static PermissionType fromCode(byte code) {
        for (PermissionType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown permission type: " + code);
    }
}
