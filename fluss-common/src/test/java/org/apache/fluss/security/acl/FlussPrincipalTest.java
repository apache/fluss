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

package org.apache.fluss.security.acl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlussPrincipal}. */
class FlussPrincipalTest {

    @Test
    void testParsePrincipals() {
        assertThat(FlussPrincipal.parsePrincipals("User:root; Group:admins"))
                .containsExactlyInAnyOrder(
                        new FlussPrincipal("root", "User"), new FlussPrincipal("admins", "Group"));
    }

    @Test
    void testParsePrincipalNameWithColons() {
        // only the first colon separates the type from the name.
        assertThat(
                        FlussPrincipal.parsePrincipals(
                                "User:spiffe://example.org/fluss;User:CN=admin,O=Example"))
                .containsExactlyInAnyOrder(
                        new FlussPrincipal("spiffe://example.org/fluss", "User"),
                        new FlussPrincipal("CN=admin,O=Example", "User"));
    }
}
