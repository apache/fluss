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

package org.apache.fluss.server.utils;

import org.apache.fluss.testutils.common.CustomExtension;
import org.apache.fluss.utils.NetUtils;

import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A Junit {@link Extension} which manages an available socket port during the lifecycle of tests.
 */
public class AvailablePortExtension implements CustomExtension {

    @Nullable private NetUtils.Port port;

    @Override
    public void before(ExtensionContext context) throws Exception {
        this.port = NetUtils.getAvailablePort();
    }

    @Override
    public void after(ExtensionContext context) throws Exception {
        if (port != null) {
            port.close();
        }
    }

    public int port() {
        checkNotNull(port);
        return port.getPort();
    }
}
