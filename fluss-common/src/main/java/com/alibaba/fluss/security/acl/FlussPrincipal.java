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

package com.alibaba.fluss.security.acl;

import com.alibaba.fluss.annotation.PublicEvolving;

import java.security.Principal;
import java.util.Objects;

/**
 * Represents a security principal in Fluss, defined by a {@code name} and {@code type}.
 *
 * <p>The principal type indicates the category of the principal (e.g., "User", "Group", "Role"),
 * while the name identifies the specific entity within that category. By default, the simple
 * authorizer uses "User" as the principal type, but custom authorizers can extend this to support
 * role-based or group-based access control lists (ACLs).
 *
 * <p>Example usage:
 *
 * <ul>
 *   <li>{@code new FlussPrincipal("admin", "User")} – A standard user principal.
 *   <li>{@code new FlussPrincipal("admins", "Group")} – A group-based principal for authorization.
 * </ul>
 *
 * @since 0.7
 */
@PublicEvolving
public class FlussPrincipal implements Principal {
    public static final FlussPrincipal ANONYMOUS = new FlussPrincipal("ANONYMOUS", "User");
    /** The wildcard principal, which represents all principals. */
    public static final FlussPrincipal WILD_CARD_PRINCIPAL = new FlussPrincipal("*", "*");

    /** The wildcard principal, which is only used in filter to matches all principals. */
    public static final FlussPrincipal ANY = new FlussPrincipal(null, null);

    private final String name;
    private final String type;

    public FlussPrincipal(String name, String type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussPrincipal that = (FlussPrincipal) o;
        return Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public String toString() {
        return "FlussPrincipal{" + "name='" + name + '\'' + ", type='" + type + '\'' + '}';
    }
}
