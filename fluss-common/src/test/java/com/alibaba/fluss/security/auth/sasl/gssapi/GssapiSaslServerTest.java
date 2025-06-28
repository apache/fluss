/*
 *  Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.security.auth.sasl.gssapi;

import com.alibaba.fluss.security.auth.sasl.jaas.LoginManager;
import com.alibaba.fluss.security.auth.sasl.jaas.SaslServerFactory;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SaslServerFactory} to verify it can create a SASL server for the mechanism
 * "GSSAPI".
 *
 * <p>This test verifies that when "GSSAPI" is specified as the mechanism, the factory delegates to
 * a {@link GssapiServerCallbackHandler} and correctly invokes {@link Sasl@createSaslServer} within
 * a privileged context.
 */
public class GssapiSaslServerTest {

    @Test
    public void testCreateSaslServerForGssapi() throws Exception {

        // Mock the dependencies: LoginManager, SaslServer, and Subject
        LoginManager mockLoginManager = mock(LoginManager.class);
        SaslServer mockSaslServer = mock(SaslServer.class);

        Subject subject = new Subject();
        when(mockLoginManager.subject()).thenReturn(subject);

        // Create empty JAAS configuration and SASL properties
        List<AppConfigurationEntry> jaasConfig = Collections.emptyList();
        Map<String, ?> props = Collections.emptyMap();

        try (MockedStatic<Subject> subjectMock = mockStatic(Subject.class);
                MockedStatic<Sasl> saslMock = mockStatic(Sasl.class)) {

            // Mock Subject.doAs to execute the provided PrivilegedExceptionAction directly
            subjectMock
                    .when(
                            () ->
                                    Subject.doAs(
                                            eq(subject), (PrivilegedExceptionAction<Object>) any()))
                    .thenAnswer(
                            invocation -> {
                                PrivilegedExceptionAction<?> action = invocation.getArgument(1);
                                return action.run();
                            });

            // Mock the Sasl.createSaslServer to return the mock SaslServer for "GSSAPI" mechanism
            saslMock.when(
                            () ->
                                    Sasl.createSaslServer(
                                            eq("GSSAPI"),
                                            eq("fluss"),
                                            eq("localhost"),
                                            eq(props),
                                            any()))
                    .thenReturn(mockSaslServer);

            // Call SaslServerFactory to create a SASL server for "GSSAPI" and verify the result
            SaslServer saslServer =
                    SaslServerFactory.createSaslServer(
                            "GSSAPI", "localhost", props, mockLoginManager, jaasConfig);

            assertThat(saslServer).isNotNull();
            assertThat(saslServer).isEqualTo(mockSaslServer);
        }
    }
}
