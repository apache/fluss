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

package com.alibaba.fluss.server.authorizer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.plugin.Plugin;
import com.alibaba.fluss.server.zk.ZooKeeperClient;

import java.util.Optional;

/** AuthorizePlugin. */
public interface AuthorizationPlugin extends Plugin {

    String identifier();

    Authorizer createAuthorizer(Context context);

    /** Provides session information describing the authorizer to be accessed. */
    @PublicEvolving
    interface Context {

        /** Get configuration of fluss server to authorize. */
        Configuration getConfiguration();

        /** Get zookeeper client to store authorization information. */
        Optional<ZooKeeperClient> getZooKeeperClient();
    }
}
