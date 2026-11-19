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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.security.acl.FlussPrincipal;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the authenticated principals in a {@link Session}. */
class SessionTest {

    @Test
    void testSnapshotsPrincipalsFromIterable() {
        FlussPrincipal user = new FlussPrincipal("alice", "User");
        FlussPrincipal group = new FlussPrincipal("readers", "Group");
        List<FlussPrincipal> principals = new ArrayList<>(Arrays.asList(user, group));
        Iterable<FlussPrincipal> iterable = principals::iterator;
        Session session = new Session((short) 0, "CLIENT", false, null, iterable);

        principals.clear();

        assertThat(session.getPrincipal()).isEqualTo(user);
        assertThat(session.getPrincipals()).containsExactly(user, group);
        assertThatThrownBy(() -> session.getPrincipals().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRejectsNullPrincipals() {
        assertThatThrownBy(
                        () ->
                                new Session(
                                        (short) 0,
                                        "CLIENT",
                                        false,
                                        null,
                                        Arrays.asList(FlussPrincipal.ANONYMOUS, null)))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testUnauthenticatedSessionHasNoPrincipals() {
        Session session = new Session((short) 0, "CLIENT", false, null, Collections.emptyList());

        assertThat(session.getPrincipals()).isEmpty();
        assertThat(session.getPrincipal()).isNull();
    }
}
