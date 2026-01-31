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

package org.apache.fluss.fs.gs.token;

import org.apache.fluss.exception.FlussRuntimeException;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Delegation token provider for GCS Hadoop filesystems. */
public class GSImperonatedAccessTokenProvider implements AccessTokenProvider {

    public static final String NAME = GSImperonatedAccessTokenProvider.class.getName();

    public static final String COMPONENT = "Dynamic session credentials for Fluss";

    private Configuration configuration;

    private static final Logger LOG =
            LoggerFactory.getLogger(GSImperonatedAccessTokenProvider.class);

    @Override
    public AccessToken getAccessToken() {
        AccessTokenProvider.AccessToken accessToken = GSImpersonatedTokenReceiver.getAccessToken();

        if (accessToken == null) {
            throw new FlussRuntimeException(
                    GSImperonatedAccessTokenProvider.COMPONENT + " not set");
        }

        LOG.debug("Providing session credentials");

        return accessToken;
    }

    @Override
    public void refresh() throws IOException {
        // Intentionally blank. Credentials are updated by GSImpersonatedTokenReceiver
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}
