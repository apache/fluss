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

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * Enumeration representing resource types used in ACL management. Permission hierarchies are
 * defined as follows:
 *
 * <p>1. CLUSTER permissions implicitly include all own database permissions under the same
 * operation. 2. DATABASE permissions implicitly include all own tables permissions under the same
 * operation 3. ANY represents global permissions covering all resource types, this only used for
 * lookup.
 *
 * <p>This hierarchy ensures that higher-level permissions automatically grant access to lower-level
 * resources within the same operation.
 *
 * @since 0.7
 */
@PublicEvolving
public enum ResourceType {
    /** In a filter, matches any ResourceType. */
    ANY((byte) 1),
    CLUSTER((byte) 2),
    DATABASE((byte) 3),
    TABLE((byte) 4);

    private final byte code;

    ResourceType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static ResourceType fromCode(byte code) {
        for (ResourceType resourceType : values()) {
            if (resourceType.code == code) {
                return resourceType;
            }
        }
        throw new IllegalArgumentException("Unknown resource type code " + code);
    }

    public static ResourceType fromName(String name) {
        for (ResourceType resourceType : values()) {
            if (resourceType.name().equalsIgnoreCase(name)) {
                return resourceType;
            }
        }
        throw new IllegalArgumentException("Unknown resource type name " + name);
    }
}
