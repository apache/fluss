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

package org.apache.fluss.fs.bos.token;

import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.fs.token.SecurityTokenReceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Client side of the BOS delegation-token flow: keeps the most recent token pushed by Fluss's token
 * manager. {@link DynamicTemporaryBOSCredentialsProvider} reads it on every BOS request, so
 * refreshed tokens take effect without re-creating the file system or the BOS client.
 */
public class BOSSecurityTokenReceiver implements SecurityTokenReceiver {
    private static final Logger LOG = LoggerFactory.getLogger(BOSSecurityTokenReceiver.class);

    public static final String SCHEME = "bos";

    private static volatile Credentials credentials;
    private static volatile Map<String, String> additionInfos = Collections.emptyMap();

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public void onNewTokensObtained(ObtainedSecurityToken token) {
        Credentials c = CredentialsJsonSerde.fromJson(token.getToken());
        credentials = c;
        additionInfos =
                token.getAdditionInfos() == null
                        ? Collections.emptyMap()
                        : token.getAdditionInfos();
        String ak = c.getAccessKeyId();
        LOG.info(
                "Received BOS session credentials, access key {}*** valid until {}",
                ak.substring(0, Math.min(6, ak.length())),
                token.getValidUntil().orElse(-1L));
    }

    /** Returns the most recently received credentials, or null if no token was received yet. */
    public static Credentials getCredentials() {
        return credentials;
    }

    /** Returns the addition infos (e.g. endpoint and region) of the most recent token. */
    public static Map<String, String> getAdditionInfos() {
        return additionInfos;
    }
}
