/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.security.auth;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.security.acl.FlussPrincipal;

/**
 * Authenticator for server side.
 *
 * @since 0.7
 */
@PublicEvolving
public interface ServerAuthenticator {

    String protocol();

    /** Initialize the authenticator. */
    default void initialize(AuthenticateContext context) {}

    /**
     * * Generates the challenge based on the client's token, then sends it back to the client. This
     * method sets the server authentication status as complete if the authentication succeeds.
     *
     * <p>Below are examples illustrating the design rationale:
     *
     * <p>1. **Username and Password Authentication (One-Way Authentication):** <br>
     * - Client → Server: Sends an initial token containing the username and password, marking the
     * client authentication as complete. <br>
     * - Server verifies the token and sets its status as complete. <br>
     * - Server → Client: Responds with success or failure.
     *
     * <p>2. **GSS-KRB5 Authentication (Two-Way Authentication with a Third-Party Authentication
     * Server):** <br>
     * - Client → Server: Sends an initial token calculated using the client's ticket. <br>
     * - Server verifies the client's ticket and generates a challenge based on the client's token
     * and the server's ticket. <br>
     * - Server → Client: Sends the challenge. <br>
     * - Client verifies the server's ticket, sets its status as complete, and calculates a response
     * token. <br>
     * - Client → Server: Sends the response token. <br>
     * - Server verifies the token, sets its status as complete, and responds with success or
     * failure.
     *
     * <p>3. **SCRAM-SHA-256 Authentication (Two-Way Authentication without a Third-Party
     * Authentication Server):** <br>
     * - Client → Server: Sends an initial token containing a random string. <br>
     * - Server verifies the token format and responds with a salt value. <br>
     * - Server → Client: Sends the salt value. <br>
     * - Client → Server: Encrypts the password with the salt and sends the result. <br>
     * - Server verifies the token, sets its status as complete, and sends a signature challenge.
     * <br>
     * - Server → Client: Sends the server's signature. <br>
     * - Client verifies the signature, sets its status as complete, and returns `null` to indicate
     * no further token exchange is needed.
     *
     * @param token the token sent by the client.
     * @return The challenge to send back to the server.
     */
    byte[] evaluateResponse(byte[] token) throws AuthenticationException;

    /** Checks if the authentication from server side is completed. */
    boolean isCompleted();

    /**
     * Create principal from authenticated token for later authorization.(this can only invoke if is
     * complete).
     */
    FlussPrincipal createPrincipal();

    /** The context of the authentication process. */
    interface AuthenticateContext {}
}
