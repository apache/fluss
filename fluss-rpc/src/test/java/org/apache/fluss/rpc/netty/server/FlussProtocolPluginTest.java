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

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.metrics.groups.MetricGroup;
import org.apache.fluss.metrics.util.NOPMetricsGroup;
import org.apache.fluss.security.acl.FlussPrincipal;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussProtocolPlugin}. */
class FlussProtocolPluginTest {

    private static final FlussPrincipal ROOT = new FlussPrincipal("root", "User");
    private static final FlussPrincipal OPERATOR = new FlussPrincipal("operator", "User");

    @Test
    void testAuthorizeSuperUserCredentialChanges() {
        FlussProtocolPlugin plugin = createPlugin(false);

        // a non-super user may change ordinary credentials
        Configuration addedAlice = credentials("root:root-pass,operator:new-pass,alice:alice-pass");
        assertThatCode(() -> plugin.validate(addedAlice, OPERATOR)).doesNotThrowAnyException();

        // but not the credentials of a configured super user
        Configuration changedRoot = credentials("root:new-root-pass,operator:operator-pass");
        assertThatThrownBy(() -> plugin.validate(changedRoot, OPERATOR))
                .isInstanceOf(AuthorizationException.class)
                .hasMessageContaining("Only configured super users may alter credentials");

        // a super user may, and so may the server itself
        assertThatCode(() -> plugin.validate(changedRoot, ROOT)).doesNotThrowAnyException();
        assertThatCode(() -> plugin.validate(changedRoot, null)).doesNotThrowAnyException();
    }

    @Test
    void testAuthorizeSuperUserCredentialChangesIgnoringCase() {
        FlussProtocolPlugin plugin = createPlugin(true);

        // the super user lookup ignores the case of the principal name and type
        Configuration changedRoot = credentials("root:new-root-pass,operator:operator-pass");
        assertThatCode(() -> plugin.validate(changedRoot, new FlussPrincipal("ROOT", "USER")))
                .doesNotThrowAnyException();

        // renaming a super user by case only still changes super user credentials
        Configuration renamedRoot = credentials("ROOT:root-pass,operator:operator-pass");
        assertThatThrownBy(() -> plugin.validate(renamedRoot, OPERATOR))
                .isInstanceOf(AuthorizationException.class);
    }

    private static FlussProtocolPlugin createPlugin(boolean principalIgnoreCase) {
        Configuration configuration = credentials("root:root-pass,operator:operator-pass");
        configuration.set(ConfigOptions.SUPER_USERS, "User:root");
        configuration.set(ConfigOptions.SECURITY_ACL_PRINCIPAL_IGNORE_CASE, principalIgnoreCase);

        MetricGroup metricGroup = NOPMetricsGroup.newInstance();
        FlussProtocolPlugin plugin =
                new FlussProtocolPlugin(
                        ServerType.COORDINATOR,
                        Collections.emptyList(),
                        RequestsMetrics.createCoordinatorServerRequestMetrics(metricGroup));
        plugin.setup(configuration);
        return plugin;
    }

    private static Configuration credentials(String credentials) {
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.SERVER_SASL_CREDENTIALS.key(), credentials);
        return configuration;
    }
}
