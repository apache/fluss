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

package org.apache.fluss.metadata;

/**
 * The delete behavior for the primary key table.
 *
 * <p>This enum defines how delete operations should be handled for primary key tables. It provides
 * different strategies to control whether deletions are allowed, ignored, or explicitly disabled.
 *
 * @since 0.8
 */
public enum DeleteBehavior {

    /**
     * Allow normal delete operations. This is the default behavior for primary key tables without
     * merge engines.
     */
    ALLOW,

    /**
     * Silently ignore delete requests without error. Delete operations will be dropped at the
     * server side, and no deletion will be performed. This is the default behavior for tables with
     * FIRST_ROW or VERSIONED merge engines.
     */
    IGNORE,

    /**
     * Reject delete requests with a clear error message. Any attempt to perform delete operations
     * will result in an exception being thrown.
     */
    DISABLE;

    /** Creates a {@link DeleteBehavior} from the given string. */
    public static DeleteBehavior fromString(String behavior) {
        switch (behavior.toUpperCase()) {
            case "ALLOW":
                return ALLOW;
            case "IGNORE":
                return IGNORE;
            case "DISABLE":
                return DISABLE;
            default:
                throw new IllegalArgumentException("Unsupported delete behavior: " + behavior);
        }
    }
}
