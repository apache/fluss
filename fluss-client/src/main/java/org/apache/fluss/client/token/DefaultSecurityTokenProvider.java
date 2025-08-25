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

package org.apache.fluss.client.token;

import org.apache.fluss.client.utils.ClientRpcMessageUtils;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.GetFileSystemSecurityTokenRequest;

/** A default implementation of {@link SecurityTokenProvider} to get token from server. */
public class DefaultSecurityTokenProvider implements SecurityTokenProvider {

    private final AdminReadOnlyGateway adminReadOnlyGateway;

    public DefaultSecurityTokenProvider(AdminReadOnlyGateway adminReadOnlyGateway) {
        this.adminReadOnlyGateway = adminReadOnlyGateway;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws Exception {
        return adminReadOnlyGateway
                .getFileSystemSecurityToken(new GetFileSystemSecurityTokenRequest())
                .thenApply(ClientRpcMessageUtils::toSecurityToken)
                .get();
    }
}
